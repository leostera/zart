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

const WaitReason = union(enum) {
    none,
    recv,
    io,
};

const ActorHeader = struct {
    runtime: *Runtime,
    id: ActorId,
    owner_worker: usize,
    msg_type: usize,
    state: std.atomic.Value(ActorState),
    wait_reason: WaitReason,
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
    retired: RetiredActors,
    workers: []Worker,
    next_external_spawn_worker: std.atomic.Value(usize),

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
            .retired = .{},
            .workers = workers,
            .next_external_spawn_worker = .init(0),
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
        const target = try rt.resolve(Msg, actor_id);
        rt.traceMessageSent(target.id);
        try target.send_fn(target, &msg);
        rt.wake(target);
    }

    /// Runs runnable actors until no runnable actor remains or an actor fails.
    pub fn run(rt: *Runtime) !void {
        while (true) {
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

    fn runReadyActor(rt: *Runtime, current_worker: *Worker) !bool {
        const ready = rt.dequeue(current_worker) orelse return false;

        defer rt.reclaimRetiredActors();

        if (ready.loadState() != .runnable) return true;

        ready.storeState(.running);
        ready.wait_reason = .none;
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

        const actor_id = try rt.actors.reserve(rt.internal_allocator);
        errdefer rt.actors.cancelReserve(actor_id);

        cell.* = Cell.init(rt, actor_id, owner_worker, stack, actor_value);
        cell.header.fiber = try Fiber.init(stack, Cell.fiberEntry, cell);
        errdefer cell.header.fiber.deinit();

        rt.actors.publish(&cell.header);
        rt.traceActorSpawned(actor_id);
        rt.ownerWorker(&cell.header).enqueue(&cell.header);

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
                switch (target.wait_reason) {
                    .recv => {
                        target.wait_reason = .none;
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
                owner.enqueue(target);
                return;
            }
        }

        owner.injectAndNotify(rt.options.scheduler_io, target);
    }

    fn dequeue(_: *Runtime, target_worker: *Worker) ?*ActorHeader {
        return target_worker.dequeue();
    }

    fn destroyActor(rt: *Runtime, target: *ActorHeader) void {
        rt.actors.remove(target);
        rt.retired.retire(target);
    }

    fn reclaimRetiredActors(rt: *Runtime) void {
        rt.retired.drain(rt);
    }

    fn drainIoCompletions(rt: *Runtime) void {
        while (rt.io.popCompletion()) |request| {
            const target: *ActorHeader = @ptrCast(@alignCast(request.actor));
            switch (target.loadState()) {
                .waiting => switch (target.wait_reason) {
                    .io => {
                        rt.traceActorIoCompleted(target.id);
                        target.wait_reason = .none;
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
        target.wait_reason = .none;
        target.storeState(.runnable);
        rt.traceActorYielded(target.id);
        rt.enqueue(target);
        Fiber.yield();
    }

    fn traceActorSpawned(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .actor_spawned = actor_id });
    }

    fn traceActorResumed(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .actor_resumed = actor_id });
    }

    fn traceActorWaiting(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .actor_waiting = actor_id });
    }

    fn traceActorYielded(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .actor_yielded = actor_id });
    }

    fn traceActorCompleted(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .actor_completed = actor_id });
    }

    fn traceActorFailed(rt: *Runtime, actor_id: ActorId, err: anyerror) void {
        if (rt.options.tracer) |tracer| {
            tracer.record(.{ .actor_failed = .{ .actor = actor_id, .err = err } });
        }
    }

    fn traceActorIoSubmitted(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .io_submitted = actor_id });
    }

    fn traceActorIoCompleted(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .io_completed = actor_id });
    }

    fn traceMessageSent(rt: *Runtime, to: ActorId) void {
        if (rt.options.tracer) |tracer| {
            tracer.record(.{
                .message_sent = .{
                    .from = if (rt.currentActor()) |current| current.id else null,
                    .to = to,
                },
            });
        }
    }

    fn traceMessageReceived(rt: *Runtime, actor_id: ActorId) void {
        if (rt.options.tracer) |tracer| tracer.record(.{ .message_received = actor_id });
    }
};

fn chargeActorIo(runtime: *anyopaque, actor_header: *anyopaque) void {
    const rt: *Runtime = @ptrCast(@alignCast(runtime));
    const target: *ActorHeader = @ptrCast(@alignCast(actor_header));
    rt.chargeIoBoundary(target);
}

fn parkActorIo(runtime: *anyopaque, actor_header: *anyopaque) void {
    const rt: *Runtime = @ptrCast(@alignCast(runtime));
    const target: *ActorHeader = @ptrCast(@alignCast(actor_header));
    if (target.wait_reason != .io) rt.traceActorIoSubmitted(target.id);
    rt.traceActorWaiting(target.id);
    target.wait_reason = .io;
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
                ctx.actor.wait_reason = .recv;
                ctx.actor.storeState(.waiting);
                if (ctx.inbox.pop()) |msg| {
                    ctx.actor.wait_reason = .none;
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
            ctx.actor.wait_reason = .none;
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
                    .wait_reason = .none,
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
                    .wait_reason = .none,
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
