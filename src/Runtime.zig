const std = @import("std");
const Fiber = @import("Fiber.zig");
const actor = @import("runtime/actor.zig");
const inbox = @import("runtime/inbox.zig");
const concrete_io = @import("io.zig");
const io = @import("runtime/io.zig");
const reclamation = @import("runtime/reclamation.zig");
const registry = @import("runtime/registry.zig");
const stack_pool = @import("runtime/stack_pool.zig");
const trace = @import("runtime/trace.zig");
const worker = @import("runtime/worker.zig");

const Allocator = std.mem.Allocator;

pub const ActorId = trace.ActorId;
pub const MessageTrace = trace.MessageTrace;
pub const FailureTrace = trace.FailureTrace;
pub const TraceEvent = trace.TraceEvent;
pub const Tracer = trace.Tracer;
pub const MessageOf = actor.MessageOf;
pub const Io = std.Io;
pub const IoDriver = RuntimeIo.Driver;
pub const IoRequest = RuntimeIo.Request;
pub const DefaultIo = concrete_io.Default;
pub const PosixIo = concrete_io.Posix;

const ActorIoContext = io.ActorIoContext;
const RuntimeIo = io.RuntimeIo;
const RetiredActors = reclamation.RetiredActors(ActorHeader);
const Registry = registry.Registry(ActorHeader);
const StackPool = stack_pool.StackPool;
const Worker = worker.Worker(ActorHeader);

const ActorState = enum(u8) {
    runnable,
    running,
    waiting,
    completed,
    failed,
};

const WaitReason = enum(u8) {
    none,
    recv,
    io,
};

const RuntimeState = enum(u8) {
    idle,
    running,
    stopping,
    stopped,
};

const ActorHeader = struct {
    runtime: *Runtime,
    id: ActorId,
    owner_worker: usize,
    msg_type: usize,
    state: std.atomic.Value(ActorState),
    wait_reason: std.atomic.Value(WaitReason),
    queued: std.atomic.Value(bool),
    budget_remaining: usize,
    io_budget_remaining: usize,
    next_run: ?*ActorHeader,
    next_retired: ?*ActorHeader,
    fiber: Fiber,
    stack: []align(Fiber.stack_alignment) u8,
    send_fn: *const fn (*ActorHeader, *const anyopaque) anyerror!void,
    destroy_fn: *const fn (*Runtime, *ActorHeader) void,

    fn loadState(header: *const ActorHeader) ActorState {
        return header.state.load(.acquire);
    }

    fn storeState(header: *ActorHeader, state: ActorState) void {
        header.state.store(state, .release);
    }

    fn exchangeState(header: *ActorHeader, state: ActorState) ActorState {
        return header.state.swap(state, .acq_rel);
    }

    fn loadWaitReason(header: *const ActorHeader) WaitReason {
        return header.wait_reason.load(.acquire);
    }

    fn storeWaitReason(header: *ActorHeader, reason: WaitReason) void {
        header.wait_reason.store(reason, .release);
    }
};

threadlocal var current_actor_header: ?*ActorHeader = null;

/// Scheduler-local actor runtime.
///
/// Actor handles are typed, and sending structurally copies the message value
/// into the recipient inbox. Pointer fields remain references owned by user
/// code.
///
/// This scheduler is currently single-threaded and cooperative. The module
/// boundaries keep the core ready for the planned SMP scheduler and work
/// stealing queues.
pub const Runtime = struct {
    pub const State = RuntimeState;

    pub const Options = struct {
        /// Stack bytes allocated for each actor fiber.
        stack_size: usize = 64 * 1024,
        /// Bytes reserved per stack slab. Larger slabs make spawn cheaper at the cost of chunkier memory growth.
        stack_slab_size: usize = 4 * 1024 * 1024,
        /// Preallocate one stack slab during runtime initialization.
        preallocate_stack_slab: bool = true,
        /// Number of explicit yield checkpoints an actor may pass per scheduler turn.
        execution_budget: usize = 64,
        /// Number of completed I/O boundaries an actor may pass per scheduler turn.
        io_budget: usize = 64,
        /// Number of scheduler workers. Only worker 0 is executed until SMP run mode lands.
        worker_count: usize = 1,
        /// I/O implementation used only for scheduler parker futex/condition operations.
        scheduler_io: std.Io = std.Options.debug_io,
        /// Allocator used for actor cells, fiber stacks, registry slots, and inbox nodes.
        internal_allocator: ?Allocator = null,
        /// Optional runtime event sink. `null` avoids constructing trace events.
        tracer: ?Tracer = null,
        /// Optional non-blocking I/O driver used by `ctx.io()`.
        io: ?IoDriver = null,
    };

    allocator: Allocator,
    internal_allocator: Allocator,
    options: Options,
    io: RuntimeIo,
    stacks: []StackPool,
    actors: Registry,
    lifetime_mutex: std.Io.Mutex,
    trace_mutex: std.Io.Mutex,
    retired: RetiredActors,
    workers: []Worker,
    next_external_spawn_worker: std.atomic.Value(usize),
    runnable_count: std.atomic.Value(usize),
    active_worker_count: std.atomic.Value(usize),
    smp_quiescing: std.atomic.Value(bool),
    lifecycle: std.atomic.Value(RuntimeState),

    /// Creates a runtime. `allocator` is exposed to actors through `ctx.allocator()`.
    pub fn init(allocator: Allocator, options: Options) RuntimeIo.InitError!Runtime {
        const internal_allocator = options.internal_allocator orelse std.heap.smp_allocator;
        var runtime_io = try RuntimeIo.init(internal_allocator, options.io);
        errdefer runtime_io.deinit(internal_allocator);

        const worker_count = @max(options.worker_count, 1);
        const workers = try internal_allocator.alloc(Worker, worker_count);
        errdefer internal_allocator.free(workers);
        for (workers, 0..) |*worker_slot, index| {
            worker_slot.* = .init(.{ .index = index });
        }

        const stacks = try internal_allocator.alloc(StackPool, worker_count);
        errdefer internal_allocator.free(stacks);
        var initialized_stack_pools: usize = 0;
        errdefer {
            for (stacks[0..initialized_stack_pools]) |*pool| pool.deinit();
        }
        for (stacks) |*pool| {
            pool.* = StackPool.init(internal_allocator, options.stack_size, options.stack_slab_size);
            initialized_stack_pools += 1;
            if (options.preallocate_stack_slab) try pool.preallocateSlab();
        }

        return .{
            .allocator = allocator,
            .internal_allocator = internal_allocator,
            .options = options,
            .io = runtime_io,
            .stacks = stacks,
            .actors = .{},
            .lifetime_mutex = .init,
            .trace_mutex = .init,
            .retired = .{},
            .workers = workers,
            .next_external_spawn_worker = .init(0),
            .runnable_count = .init(0),
            .active_worker_count = .init(0),
            .smp_quiescing = .init(false),
            .lifecycle = .init(.idle),
        };
    }

    /// Destroys all live actors and runtime-owned internal storage.
    pub fn deinit(rt: *Runtime) void {
        rt.reclaimRetiredActors();
        rt.actors.deinit(rt.internal_allocator, rt);
        for (rt.stacks) |*pool| pool.deinit();
        rt.internal_allocator.free(rt.stacks);
        rt.io.deinit(rt.internal_allocator);
        rt.internal_allocator.free(rt.workers);
        rt.* = undefined;
    }

    /// Spawns a function actor or a struct actor and returns its typed handle.
    ///
    /// Function actors must have shape `fn (*Ctx(Msg)) !void`. Struct actors
    /// must declare `pub const Msg` and a `run` method.
    pub fn spawn(rt: *Runtime, entry: anytype) !Actor(MessageOf(@TypeOf(entry))) {
        const ActorType = @TypeOf(entry);
        const Msg = MessageOf(ActorType);

        return switch (@typeInfo(ActorType)) {
            .@"fn" => rt.spawnFunction(Msg, entry),
            .@"struct" => rt.spawnStruct(Msg, ActorType, entry),
            else => @compileError("spawn expects a function actor or a struct actor with pub const Msg and pub fn run"),
        };
    }

    /// Sends a message to a raw actor id. Prefer `Actor(Msg).send` in user code.
    pub fn send(rt: *Runtime, comptime Msg: type, actor_id: ActorId, msg: Msg) !void {
        if (rt.state() == .stopped) return error.RuntimeStopped;

        rt.lifetime_mutex.lockUncancelable(rt.options.scheduler_io);
        defer rt.lifetime_mutex.unlock(rt.options.scheduler_io);

        const target = try rt.resolve(Msg, actor_id);
        rt.traceMessageSent(target.id);
        try target.send_fn(target, &msg);
        rt.wake(target);
    }

    /// Requests runtime shutdown and wakes any parked workers.
    pub fn stop(rt: *Runtime) void {
        const previous = rt.lifecycle.swap(.stopping, .acq_rel);
        if (previous == .stopped) {
            rt.lifecycle.store(.stopped, .release);
            return;
        }
        rt.closeWorkers();
    }

    /// Returns the runtime lifecycle state.
    pub fn state(rt: *const Runtime) State {
        return rt.lifecycle.load(.acquire);
    }

    /// Runs runnable actors until no runnable actor remains or an actor fails.
    pub fn run(rt: *Runtime) !void {
        try rt.enterRun();
        defer rt.leaveRun();
        rt.io.setCompletionNotify(rt, notifyIoCompletion);

        while (true) {
            if (rt.isStopping()) return;

            try rt.pollIo(.nonblocking);
            rt.drainIoCompletions();

            var made_progress = false;
            for (rt.workers) |*current_worker| {
                made_progress = try rt.runReadyActor(current_worker) or made_progress;
            }
            if (!made_progress) {
                if (rt.io.hasPoller() and rt.io.hasPending()) {
                    try rt.pollIo(.wait);
                    rt.drainIoCompletions();
                    continue;
                }
                return;
            }
        }
    }

    /// Runs configured workers on OS threads and waits for all workers to stop.
    ///
    /// This is the first SMP run mode. Idle parking, work stealing, and
    /// per-worker I/O completion routing are layered on top in later slices.
    pub fn runSmp(rt: *Runtime) !void {
        try rt.enterRun();
        defer rt.leaveRun();
        rt.io.setCompletionNotify(rt, notifyIoCompletion);
        rt.smp_quiescing.store(false, .release);

        if (rt.workers.len == 1) return rt.runSmpWorker(0);

        const ThreadContext = struct {
            runtime: *Runtime,
            worker_index: usize,
            err: ?anyerror = null,

            fn run(context: *@This()) void {
                context.runtime.runSmpWorker(context.worker_index) catch |err| {
                    context.err = err;
                    context.runtime.stop();
                };
            }
        };

        const other_worker_count = rt.workers.len - 1;
        const contexts = try rt.internal_allocator.alloc(ThreadContext, other_worker_count);
        defer rt.internal_allocator.free(contexts);
        const threads = try rt.internal_allocator.alloc(std.Thread, other_worker_count);
        defer rt.internal_allocator.free(threads);

        for (contexts, 0..) |*context, offset| {
            context.* = .{
                .runtime = rt,
                .worker_index = offset + 1,
            };
        }

        var spawned: usize = 0;
        errdefer {
            rt.stop();
            for (threads[0..spawned]) |thread| thread.join();
        }
        for (threads, contexts) |*thread, *context| {
            thread.* = try std.Thread.spawn(.{}, ThreadContext.run, .{context});
            spawned += 1;
        }

        var main_err: ?anyerror = null;
        rt.runSmpWorker(0) catch |err| {
            rt.stop();
            main_err = err;
        };

        for (threads) |thread| thread.join();

        if (main_err) |err| return err;
        for (contexts) |context| {
            if (context.err) |err| return err;
        }
    }

    fn enterRun(rt: *Runtime) !void {
        while (true) {
            switch (rt.lifecycle.load(.acquire)) {
                .idle => {
                    if (rt.lifecycle.cmpxchgStrong(.idle, .running, .acq_rel, .acquire) == null) return;
                },
                .running => return error.RuntimeAlreadyRunning,
                .stopping => return,
                .stopped => return error.RuntimeStopped,
            }
        }
    }

    fn leaveRun(rt: *Runtime) void {
        while (true) {
            switch (rt.lifecycle.load(.acquire)) {
                .running => {
                    if (rt.lifecycle.cmpxchgStrong(.running, .idle, .acq_rel, .acquire) == null) return;
                },
                .stopping => {
                    rt.lifecycle.store(.stopped, .release);
                    return;
                },
                .idle, .stopped => return,
            }
        }
    }

    fn isStopping(rt: *const Runtime) bool {
        return switch (rt.state()) {
            .stopping, .stopped => true,
            .idle, .running => false,
        };
    }

    fn closeWorkers(rt: *Runtime) void {
        for (rt.workers) |*target_worker| target_worker.close(rt.options.scheduler_io);
    }

    fn runSmpWorker(rt: *Runtime, worker_index: usize) !void {
        const current_worker = &rt.workers[worker_index];

        while (!rt.isStopping()) {
            if (rt.smp_quiescing.load(.acquire)) return;

            rt.drainIoCompletions();
            if (worker_index == 0) {
                try rt.pollIo(.nonblocking);
                rt.drainIoCompletions();
                if (rt.io.hasPoller() and rt.io.hasPending() and rt.runnable_count.load(.acquire) == 0) {
                    try rt.pollIo(.wait);
                    rt.drainIoCompletions();
                    continue;
                }
            }

            if (try rt.runReadyActor(current_worker)) continue;
            if (rt.tryQuiesceSmp()) return;

            switch (current_worker.wait(rt.options.scheduler_io)) {
                .notified => {},
                .closed => return,
            }
        }
    }

    fn runReadyActor(rt: *Runtime, current_worker: *Worker) !bool {
        const ready = rt.dequeue(current_worker) orelse return false;

        defer rt.reclaimRetiredActors();

        if (ready.loadState() != .runnable) return true;

        _ = rt.active_worker_count.fetchAdd(1, .acq_rel);
        defer _ = rt.active_worker_count.fetchSub(1, .acq_rel);

        ready.storeState(.running);
        ready.storeWaitReason(.none);
        ready.budget_remaining = rt.executionBudget();
        ready.io_budget_remaining = rt.ioBudget();
        rt.traceActorResumed(ready.id);
        current_worker.setCurrent(ready);
        current_actor_header = ready;
        const status = ready.fiber.run() catch |err| {
            current_actor_header = null;
            current_worker.setCurrent(null);
            return err;
        };
        current_actor_header = null;
        current_worker.setCurrent(null);
        switch (status) {
            .created => unreachable,
            .running => unreachable,
            .suspended => switch (ready.loadState()) {
                .running => {
                    ready.storeState(.runnable);
                    rt.enqueue(ready);
                },
                .runnable, .waiting => {},
                .completed, .failed => unreachable,
            },
            .completed => {
                ready.storeState(.completed);
                rt.traceActorCompleted(ready.id);
                rt.destroyActor(ready);
            },
            .failed => {
                ready.storeState(.failed);
                if (ready.fiber.failure()) |err| {
                    rt.traceActorFailed(ready.id, err);
                    rt.destroyActor(ready);
                    rt.reclaimRetiredActors();
                    return err;
                }
                unreachable;
            },
        }

        return true;
    }

    fn runWorker(rt: *Runtime, worker_index: usize) !void {
        const current_worker = &rt.workers[worker_index];

        while (true) {
            try rt.pollIo(.nonblocking);
            rt.drainIoCompletions();

            if (!try rt.runReadyActor(current_worker)) {
                if (rt.io.hasPoller() and rt.io.hasPending()) {
                    try rt.pollIo(.wait);
                    rt.drainIoCompletions();
                    continue;
                }
                return;
            }
        }
    }

    fn spawnFunction(rt: *Runtime, comptime Msg: type, comptime entry: anytype) !Actor(Msg) {
        const Cell = FunctionActorCell(Msg, entry);
        return rt.spawnCell(Msg, Cell, {});
    }

    fn spawnStruct(rt: *Runtime, comptime Msg: type, comptime ActorType: type, instance: ActorType) !Actor(Msg) {
        const Cell = StructActorCell(Msg, ActorType);
        return rt.spawnCell(Msg, Cell, instance);
    }

    fn spawnCell(rt: *Runtime, comptime Msg: type, comptime Cell: type, actor_value: anytype) !Actor(Msg) {
        try rt.syncStackPoolPolicy();
        const owner_worker = rt.spawnOwnerWorker();

        const cell = try rt.internal_allocator.create(Cell);
        errdefer rt.internal_allocator.destroy(cell);

        const owner_stack_pool = rt.stackPool(owner_worker);
        const stack = try owner_stack_pool.alloc();
        errdefer owner_stack_pool.free(stack);

        const actor_id = try rt.actors.reserve(rt.internal_allocator, rt.options.scheduler_io);
        errdefer rt.actors.cancelReserve(rt.options.scheduler_io, actor_id);

        cell.* = Cell.init(rt, actor_id, owner_worker, stack, actor_value);
        cell.header.fiber = try Fiber.init(stack, Cell.fiberEntry, cell);
        errdefer cell.header.fiber.deinit();

        rt.actors.publish(&cell.header);
        rt.traceActorSpawned(actor_id);
        rt.enqueueSpawn(&cell.header);

        return .{
            .raw = actor_id,
            .runtime = rt,
        };
    }

    fn syncStackPoolPolicy(rt: *Runtime) Allocator.Error!void {
        var matches = true;
        for (rt.stacks) |*pool| {
            if (!pool.matchesPolicy(rt.options.stack_size, rt.options.stack_slab_size)) {
                matches = false;
                break;
            }
        }
        if (matches) return;

        for (rt.stacks) |*pool| {
            std.debug.assert(pool.live_count == 0);
            pool.deinit();
            pool.* = .init(rt.internal_allocator, rt.options.stack_size, rt.options.stack_slab_size);
            if (rt.options.preallocate_stack_slab) try pool.preallocateSlab();
        }
    }

    fn ownerWorker(rt: *Runtime, target: *const ActorHeader) *Worker {
        return &rt.workers[target.owner_worker];
    }

    fn stackPool(rt: *Runtime, owner_worker: usize) *StackPool {
        return &rt.stacks[owner_worker];
    }

    fn actorStackPool(rt: *Runtime, target: *const ActorHeader) *StackPool {
        return rt.stackPool(target.owner_worker);
    }

    fn spawnOwnerWorker(rt: *Runtime) usize {
        if (rt.currentActor()) |current| return current.owner_worker;
        return rt.next_external_spawn_worker.fetchAdd(1, .monotonic) % rt.workers.len;
    }

    fn currentActor(rt: *Runtime) ?*ActorHeader {
        const current = current_actor_header orelse return null;
        if (current.runtime != rt) return null;
        return current;
    }

    fn resolve(rt: *Runtime, comptime Msg: type, actor_id: ActorId) !*ActorHeader {
        const target = rt.actors.get(actor_id) orelse return error.InvalidActor;
        if (target.msg_type != typeId(Msg)) return error.WrongMessageType;

        return switch (target.loadState()) {
            .completed, .failed => error.ActorDead,
            .runnable, .running, .waiting => target,
        };
    }

    fn wake(rt: *Runtime, target: *ActorHeader) void {
        switch (target.loadState()) {
            .waiting => {
                switch (target.loadWaitReason()) {
                    .recv => {
                        target.storeWaitReason(.none);
                        target.storeState(.runnable);
                        rt.enqueue(target);
                    },
                    .io, .none => {},
                }
            },
            .runnable, .running => {},
            .completed, .failed => {},
        }
    }

    fn enqueue(rt: *Runtime, target: *ActorHeader) void {
        const owner = rt.ownerWorker(target);
        if (rt.currentActor()) |current| {
            if (current.owner_worker == target.owner_worker) {
                if (owner.enqueue(rt.options.scheduler_io, target)) rt.noteRunnableQueued();
                return;
            }
        }

        if (owner.inject(target)) {
            rt.noteRunnableQueued();
            owner.notify(rt.options.scheduler_io);
        }
    }

    fn enqueueSpawn(rt: *Runtime, target: *ActorHeader) void {
        const owner = rt.ownerWorker(target);
        if (rt.currentActor() != null or rt.state() != .running) {
            if (owner.enqueue(rt.options.scheduler_io, target)) rt.noteRunnableQueued();
            return;
        }

        rt.enqueue(target);
    }

    fn dequeue(rt: *Runtime, target_worker: *Worker) ?*ActorHeader {
        const target = target_worker.dequeue(rt.options.scheduler_io) orelse rt.steal(target_worker) orelse return null;
        _ = rt.runnable_count.fetchSub(1, .acq_rel);
        return target;
    }

    fn steal(rt: *Runtime, target_worker: *Worker) ?*ActorHeader {
        for (rt.workers) |*victim| {
            if (victim == target_worker) continue;
            if (victim.steal(rt.options.scheduler_io)) |stolen| return stolen;
        }
        return null;
    }

    fn noteRunnableQueued(rt: *Runtime) void {
        _ = rt.runnable_count.fetchAdd(1, .acq_rel);
    }

    fn tryQuiesceSmp(rt: *Runtime) bool {
        if (rt.runnable_count.load(.acquire) != 0) return false;
        if (rt.active_worker_count.load(.acquire) != 0) return false;
        if (rt.io.hasPending()) return false;

        if (rt.smp_quiescing.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
            for (rt.workers) |*target_worker| target_worker.notify(rt.options.scheduler_io);
        }
        return true;
    }

    fn destroyActor(rt: *Runtime, target: *ActorHeader) void {
        rt.lifetime_mutex.lockUncancelable(rt.options.scheduler_io);
        defer rt.lifetime_mutex.unlock(rt.options.scheduler_io);

        rt.actors.remove(rt.options.scheduler_io, target);
        rt.retired.retire(target);
    }

    fn reclaimRetiredActors(rt: *Runtime) void {
        rt.lifetime_mutex.lockUncancelable(rt.options.scheduler_io);
        defer rt.lifetime_mutex.unlock(rt.options.scheduler_io);

        rt.retired.drain(rt);
    }

    fn drainIoCompletions(rt: *Runtime) void {
        while (rt.io.popCompletion(rt.options.scheduler_io)) |request| {
            const target: *ActorHeader = @ptrCast(@alignCast(request.actor));
            switch (target.loadState()) {
                .waiting => switch (target.loadWaitReason()) {
                    .io => {
                        rt.traceActorIoCompleted(target.id);
                        target.storeWaitReason(.none);
                        target.storeState(.runnable);
                        rt.enqueue(target);
                    },
                    .recv, .none => {},
                },
                .runnable, .running, .completed, .failed => {},
            }
        }
    }

    fn pollIo(rt: *Runtime, mode: RuntimeIo.PollMode) !void {
        if (!rt.io.hasPoller()) return;
        try rt.io.poll(mode);
    }

    fn executionBudget(rt: *const Runtime) usize {
        return @max(rt.options.execution_budget, 1);
    }

    fn ioBudget(rt: *const Runtime) usize {
        return @max(rt.options.io_budget, 1);
    }

    fn chargeIoBoundary(rt: *Runtime, target: *ActorHeader) void {
        if (target.io_budget_remaining > 1) {
            target.io_budget_remaining -= 1;
            return;
        }

        target.io_budget_remaining = 0;
        target.storeWaitReason(.none);
        target.storeState(.runnable);
        rt.traceActorYielded(target.id);
        rt.enqueue(target);
        Fiber.yield();
    }

    fn traceActorSpawned(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .actor_spawned = actor_id });
    }

    fn traceActorResumed(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .actor_resumed = actor_id });
    }

    fn traceActorWaiting(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .actor_waiting = actor_id });
    }

    fn traceActorYielded(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .actor_yielded = actor_id });
    }

    fn traceActorCompleted(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .actor_completed = actor_id });
    }

    fn traceActorFailed(rt: *Runtime, actor_id: ActorId, err: anyerror) void {
        rt.recordTrace(.{ .actor_failed = .{ .actor = actor_id, .err = err } });
    }

    fn traceActorIoSubmitted(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .io_submitted = actor_id });
    }

    fn traceActorIoCompleted(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .io_completed = actor_id });
    }

    fn traceMessageSent(rt: *Runtime, to: ActorId) void {
        rt.recordTrace(.{
            .message_sent = .{
                .from = if (rt.currentActor()) |current| current.id else null,
                .to = to,
            },
        });
    }

    fn traceMessageReceived(rt: *Runtime, actor_id: ActorId) void {
        rt.recordTrace(.{ .message_received = actor_id });
    }

    fn recordTrace(rt: *Runtime, event: TraceEvent) void {
        const tracer = rt.options.tracer orelse return;

        rt.trace_mutex.lockUncancelable(rt.options.scheduler_io);
        defer rt.trace_mutex.unlock(rt.options.scheduler_io);

        tracer.record(event);
    }
};

fn chargeActorIo(runtime: *anyopaque, actor_header: *anyopaque) void {
    const rt: *Runtime = @ptrCast(@alignCast(runtime));
    const target: *ActorHeader = @ptrCast(@alignCast(actor_header));
    rt.chargeIoBoundary(target);
}

fn notifyIoCompletion(context: ?*anyopaque, request: *RuntimeIo.Request) void {
    const rt: *Runtime = @ptrCast(@alignCast(context.?));
    const target: *ActorHeader = @ptrCast(@alignCast(request.actor));
    rt.ownerWorker(target).notify(rt.options.scheduler_io);
}

fn parkActorIo(runtime: *anyopaque, actor_header: *anyopaque) void {
    const rt: *Runtime = @ptrCast(@alignCast(runtime));
    const target: *ActorHeader = @ptrCast(@alignCast(actor_header));
    if (target.loadWaitReason() != .io) rt.traceActorIoSubmitted(target.id);
    rt.traceActorWaiting(target.id);
    target.storeWaitReason(.io);
    target.storeState(.waiting);
    Fiber.yield();
}

/// Typed actor handle. This is the user-facing capability used for sends.
pub fn Actor(comptime Msg: type) type {
    return actor.Actor(Runtime, ActorId, Msg);
}

/// Actor execution context passed to actor bodies.
pub fn Ctx(comptime Msg: type) type {
    return struct {
        pub const Message = Msg;

        runtime: *Runtime,
        actor: *ActorHeader,
        inbox: *Inbox(Msg),
        self_actor: Actor(Msg),
        io_context: ActorIoContext,

        /// Receives the next message, yielding until one is available.
        pub fn recv(ctx: *@This()) !Msg {
            while (true) {
                if (ctx.inbox.pop()) |msg| {
                    ctx.runtime.traceMessageReceived(ctx.actor.id);
                    return msg;
                }

                ctx.runtime.traceActorWaiting(ctx.actor.id);
                ctx.actor.storeWaitReason(.recv);
                ctx.actor.storeState(.waiting);
                if (ctx.inbox.pop()) |msg| {
                    ctx.actor.storeWaitReason(.none);
                    ctx.actor.storeState(.running);
                    ctx.runtime.traceMessageReceived(ctx.actor.id);
                    return msg;
                }
                Fiber.yield();
            }
        }

        /// Cooperative checkpoint for long-running CPU loops.
        ///
        /// The scheduler switches only once this actor's execution budget for
        /// the current turn is exhausted.
        pub fn yield(ctx: *@This()) void {
            if (ctx.actor.budget_remaining > 1) {
                ctx.actor.budget_remaining -= 1;
                return;
            }

            ctx.actor.budget_remaining = 0;
            ctx.runtime.traceActorYielded(ctx.actor.id);
            ctx.actor.storeWaitReason(.none);
            ctx.actor.storeState(.runnable);
            ctx.runtime.enqueue(ctx.actor);
            Fiber.yield();
        }

        /// Returns the cooperative I/O facade for this actor.
        pub fn io(ctx: *@This()) std.Io {
            return ctx.io_context.interface();
        }

        /// Spawns a child actor on the same runtime.
        pub fn spawn(ctx: *@This(), entry: anytype) !Actor(MessageOf(@TypeOf(entry))) {
            return ctx.runtime.spawn(entry);
        }

        /// Returns this actor's typed handle.
        pub fn self(ctx: *const @This()) Actor(Msg) {
            return ctx.self_actor;
        }

        /// Requests runtime shutdown after the current actor turn.
        pub fn stop(ctx: *const @This()) void {
            ctx.runtime.stop();
        }

        /// Returns the user allocator provided to `Runtime.init`.
        pub fn allocator(ctx: *const @This()) Allocator {
            return ctx.runtime.allocator;
        }
    };
}

fn FunctionActorCell(comptime Msg: type, comptime entry: anytype) type {
    return struct {
        header: ActorHeader,
        inbox: Inbox(Msg),

        const Self = @This();

        fn init(rt: *Runtime, actor_id: ActorId, owner_worker: usize, stack: []align(Fiber.stack_alignment) u8, _: void) Self {
            return .{
                .header = .{
                    .runtime = rt,
                    .id = actor_id,
                    .owner_worker = owner_worker,
                    .msg_type = typeId(Msg),
                    .state = .init(.runnable),
                    .wait_reason = .init(.none),
                    .queued = .init(false),
                    .budget_remaining = 0,
                    .io_budget_remaining = 0,
                    .next_run = null,
                    .next_retired = null,
                    .fiber = undefined,
                    .stack = stack,
                    .send_fn = send,
                    .destroy_fn = destroy,
                },
                .inbox = .init(rt.internal_allocator),
            };
        }

        fn fiberEntry(arg: ?*anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(arg.?));
            var ctx: Ctx(Msg) = .{
                .runtime = cell.header.runtime,
                .actor = &cell.header,
                .inbox = &cell.inbox,
                .self_actor = .{
                    .raw = cell.header.id,
                    .runtime = cell.header.runtime,
                },
                .io_context = .init(&cell.header.runtime.io, cell.header.runtime, &cell.header, chargeActorIo, parkActorIo),
            };
            try entry(&ctx);
        }

        fn send(header: *ActorHeader, raw_msg: *const anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(header));
            const msg: *const Msg = @ptrCast(@alignCast(raw_msg));
            try cell.inbox.push(msg.*);
        }

        fn destroy(rt: *Runtime, header: *ActorHeader) void {
            const cell: *Self = @ptrCast(@alignCast(header));
            cell.inbox.deinit();
            cell.header.fiber.deinit();
            rt.actorStackPool(&cell.header).free(cell.header.stack);
            rt.internal_allocator.destroy(cell);
        }
    };
}

fn StructActorCell(comptime Msg: type, comptime ActorType: type) type {
    return struct {
        header: ActorHeader,
        inbox: Inbox(Msg),
        actor: ActorType,

        const Self = @This();

        fn init(rt: *Runtime, actor_id: ActorId, owner_worker: usize, stack: []align(Fiber.stack_alignment) u8, instance: ActorType) Self {
            return .{
                .header = .{
                    .runtime = rt,
                    .id = actor_id,
                    .owner_worker = owner_worker,
                    .msg_type = typeId(Msg),
                    .state = .init(.runnable),
                    .wait_reason = .init(.none),
                    .queued = .init(false),
                    .budget_remaining = 0,
                    .io_budget_remaining = 0,
                    .next_run = null,
                    .next_retired = null,
                    .fiber = undefined,
                    .stack = stack,
                    .send_fn = send,
                    .destroy_fn = destroy,
                },
                .inbox = .init(rt.internal_allocator),
                .actor = instance,
            };
        }

        fn fiberEntry(arg: ?*anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(arg.?));
            var ctx: Ctx(Msg) = .{
                .runtime = cell.header.runtime,
                .actor = &cell.header,
                .inbox = &cell.inbox,
                .self_actor = .{
                    .raw = cell.header.id,
                    .runtime = cell.header.runtime,
                },
                .io_context = .init(&cell.header.runtime.io, cell.header.runtime, &cell.header, chargeActorIo, parkActorIo),
            };
            try cell.actor.run(&ctx);
        }

        fn send(header: *ActorHeader, raw_msg: *const anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(header));
            const msg: *const Msg = @ptrCast(@alignCast(raw_msg));
            try cell.inbox.push(msg.*);
        }

        fn destroy(rt: *Runtime, header: *ActorHeader) void {
            const cell: *Self = @ptrCast(@alignCast(header));
            cell.inbox.deinit();
            cell.header.fiber.deinit();
            rt.actorStackPool(&cell.header).free(cell.header.stack);
            rt.internal_allocator.destroy(cell);
        }
    };
}

fn Inbox(comptime Msg: type) type {
    return inbox.Inbox(Msg);
}

fn typeId(comptime T: type) usize {
    return @intFromPtr(&TypeId(T).id);
}

fn TypeId(comptime T: type) type {
    return struct {
        const Type = T;
        var id: u8 = 0;
    };
}

test "runtime creates one stack pool per configured worker" {
    const testing = std.testing;

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 4,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    try testing.expectEqual(@as(usize, 4), rt.workers.len);
    try testing.expectEqual(@as(usize, 4), rt.stacks.len);
}

test "runtime distributes external spawns across configured workers" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Stopper = struct {
        pub const Msg = StopMsg;

        stopped: *usize,

        pub fn run(self: *@This(), ctx: *Ctx(StopMsg)) !void {
            switch (try ctx.recv()) {
                .stop => self.stopped.* += 1,
            }
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    var stopped: usize = 0;
    const first = try rt.spawn(Stopper{ .stopped = &stopped });
    const second = try rt.spawn(Stopper{ .stopped = &stopped });

    try testing.expectEqual(@as(usize, 0), rt.actors.get(first.any()).?.owner_worker);
    try testing.expectEqual(@as(usize, 1), rt.actors.get(second.any()).?.owner_worker);

    try first.send(.stop);
    try second.send(.stop);
    try rt.run();

    try testing.expectEqual(@as(usize, 2), stopped);
}

test "runtime returns to idle after a normal run" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Stopper = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(StopMsg)) !void {
            _ = try ctx.recv();
        }
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const stopper = try rt.spawn(Stopper{});
    try stopper.send(.stop);
    try rt.run();

    try testing.expectEqual(Runtime.State.idle, rt.state());
}

test "runtime stop exits after current actor turn" {
    const testing = std.testing;

    const RunMsg = union(enum) {
        run,
    };

    const Stopper = struct {
        pub const Msg = RunMsg;

        ran: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(RunMsg)) !void {
            _ = try ctx.recv();
            self.ran.* = true;
            ctx.stop();
        }
    };

    const Observer = struct {
        pub const Msg = RunMsg;

        ran: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(RunMsg)) !void {
            _ = try ctx.recv();
            self.ran.* = true;
        }
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var stopper_ran = false;
    var observer_ran = false;
    const stopper = try rt.spawn(Stopper{ .ran = &stopper_ran });
    const observer = try rt.spawn(Observer{ .ran = &observer_ran });

    try stopper.send(.run);
    try observer.send(.run);
    try rt.run();

    try testing.expect(stopper_ran);
    try testing.expect(!observer_ran);
    try testing.expectEqual(Runtime.State.stopped, rt.state());
    try testing.expectError(error.RuntimeStopped, observer.send(.run));
}

test "runtime smp run executes actors on configured workers" {
    const testing = std.testing;

    const RunMsg = union(enum) {
        run,
    };

    const WorkerActor = struct {
        pub const Msg = RunMsg;

        completed: *std.atomic.Value(usize),

        pub fn run(self: *@This(), ctx: *Ctx(RunMsg)) !void {
            _ = try ctx.recv();
            _ = self.completed.fetchAdd(1, .acq_rel);
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    var completed = std.atomic.Value(usize).init(0);
    const first = try rt.spawn(WorkerActor{ .completed = &completed });
    const second = try rt.spawn(WorkerActor{ .completed = &completed });

    try first.send(.run);
    try second.send(.run);
    try rt.runSmp();

    try testing.expectEqual(@as(usize, 2), completed.load(.acquire));
    try testing.expectEqual(Runtime.State.idle, rt.state());
}

test "runtime smp run wakes parked worker for remote send" {
    const testing = std.testing;

    const ReceiverMsg = union(enum) {
        hit,
    };

    const SenderMsg = union(enum) {
        go,
    };

    const Receiver = struct {
        pub const Msg = ReceiverMsg;

        ready: *std.atomic.Value(bool),
        received: *std.atomic.Value(bool),

        pub fn run(self: *@This(), ctx: *Ctx(ReceiverMsg)) !void {
            self.ready.store(true, .release);
            _ = try ctx.recv();
            self.received.store(true, .release);
        }
    };

    const Sender = struct {
        pub const Msg = SenderMsg;

        target: Actor(ReceiverMsg),
        receiver_ready: *std.atomic.Value(bool),

        pub fn run(self: *@This(), ctx: *Ctx(SenderMsg)) !void {
            _ = try ctx.recv();
            while (!self.receiver_ready.load(.acquire)) ctx.yield();
            try self.target.send(.hit);
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .execution_budget = 1,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    var receiver_ready = std.atomic.Value(bool).init(false);
    var received = std.atomic.Value(bool).init(false);
    const receiver = try rt.spawn(Receiver{
        .ready = &receiver_ready,
        .received = &received,
    });
    const sender = try rt.spawn(Sender{
        .target = receiver,
        .receiver_ready = &receiver_ready,
    });

    try sender.send(.go);
    try rt.runSmp();

    try testing.expect(received.load(.acquire));
}

test "runtime smp run wakes owner worker for async io completion" {
    const testing = std.testing;

    const AsyncSleepDriver = struct {
        mutex: std.Io.Mutex = .init,
        condition: std.Io.Condition = .init,
        request: ?*IoRequest = null,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.mutex.lockUncancelable(testing.io);
            defer self.mutex.unlock(testing.io);

            self.request = request;
            self.condition.signal(testing.io);
        }

        fn complete(self: *Self) void {
            self.mutex.lockUncancelable(testing.io);
            defer self.mutex.unlock(testing.io);

            while (self.request == null) self.condition.waitUncancelable(testing.io, &self.mutex);
            const request = self.request.?;
            self.request = null;
            request.completeSleep({});
        }
    };

    const AsyncMsg = union(enum) {
        run,
    };

    const Quick = struct {
        pub const Msg = AsyncMsg;

        pub fn run(_: *@This(), ctx: *Ctx(AsyncMsg)) !void {
            _ = try ctx.recv();
        }
    };

    const Sleeper = struct {
        pub const Msg = AsyncMsg;

        completed: *std.atomic.Value(bool),

        pub fn run(self: *@This(), ctx: *Ctx(AsyncMsg)) !void {
            _ = try ctx.recv();
            try std.Io.Timeout.sleep(.none, ctx.io());
            self.completed.store(true, .release);
        }
    };

    const Completer = struct {
        driver: *AsyncSleepDriver,

        fn run(self: @This()) void {
            self.driver.complete();
        }
    };

    var driver: AsyncSleepDriver = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .preallocate_stack_slab = false,
        .io = driver.driver(),
    });
    defer rt.deinit();

    var completed = std.atomic.Value(bool).init(false);
    const quick = try rt.spawn(Quick{});
    const sleeper = try rt.spawn(Sleeper{ .completed = &completed });
    try quick.send(.run);
    try sleeper.send(.run);

    const completer = try std.Thread.spawn(.{}, Completer.run, .{Completer{ .driver = &driver }});
    try rt.runSmp();
    completer.join();

    try testing.expect(completed.load(.acquire));
}

test "runtime workers can steal runnable actors" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Stopper = struct {
        pub const Msg = StopMsg;

        stopped: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(StopMsg)) !void {
            _ = try ctx.recv();
            self.stopped.* = true;
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    rt.next_external_spawn_worker.store(0, .release);
    var stopped = false;
    const stopper = try rt.spawn(Stopper{ .stopped = &stopped });

    const stolen = rt.dequeue(&rt.workers[1]) orelse return error.TestExpectedEqual;
    try testing.expectEqual(stopper.any(), stolen.id);
    try testing.expectEqual(@as(usize, 1), rt.workers[0].steal_count.load(.acquire));

    rt.enqueue(stolen);
    try stopper.send(.stop);
    try rt.runSmp();

    try testing.expect(stopped);
}

test "runtime serializes tracer calls during smp run" {
    const testing = std.testing;

    const Recorder = struct {
        events: [64]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(self: *@This()) Tracer {
            return .{
                .context = self,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.events[self.len] = event;
            self.len += 1;
        }
    };

    const RunMsg = union(enum) {
        run,
    };

    const WorkerActor = struct {
        pub const Msg = RunMsg;

        pub fn run(_: *@This(), ctx: *Ctx(RunMsg)) !void {
            _ = try ctx.recv();
        }
    };

    var recorder: Recorder = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .preallocate_stack_slab = false,
        .tracer = recorder.tracer(),
    });
    defer rt.deinit();

    const first = try rt.spawn(WorkerActor{});
    const second = try rt.spawn(WorkerActor{});
    try first.send(.run);
    try second.send(.run);
    try rt.runSmp();

    var completions: usize = 0;
    for (recorder.events[0..recorder.len]) |event| {
        if (event == .actor_completed) completions += 1;
    }
    try testing.expectEqual(@as(usize, 2), completions);
}
