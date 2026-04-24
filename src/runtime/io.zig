//! Runtime-owned wrappers for Zig's standard I/O interface.

const std = @import("std");

pub const RuntimeIo = struct {
    shared: *Shared,

    pub const InitError = std.mem.Allocator.Error || std.Thread.SpawnError;

    pub const Request = struct {
        next: ?*Request = null,
        actor: *anyopaque,
        completed: bool = false,
        payload: Payload,

        const Payload = union(enum) {
            operate: Operate,
            sleep: Sleep,
        };

        const Operate = struct {
            operation: std.Io.Operation,
            result: std.Io.Cancelable!std.Io.Operation.Result = undefined,
        };

        const Sleep = struct {
            timeout: std.Io.Timeout,
            result: std.Io.Cancelable!void = undefined,
        };

        pub fn initOperate(actor: *anyopaque, operation: std.Io.Operation) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .operate = .{ .operation = operation },
                },
            };
        }

        pub fn initSleep(actor: *anyopaque, timeout: std.Io.Timeout) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .sleep = .{ .timeout = timeout },
                },
            };
        }
    };

    const Shared = struct {
        base: std.Io,
        lock: SpinLock = .{},
        submissions_head: ?*Request = null,
        submissions_tail: ?*Request = null,
        completions_head: ?*Request = null,
        completions_tail: ?*Request = null,
        active_count: usize = 0,
        stopping: bool = false,
        poll_interval_ns: u64,
        worker: ?std.Thread = null,
    };

    const SpinLock = struct {
        locked: std.atomic.Value(bool) = .init(false),

        fn lock(spin: *SpinLock) void {
            while (spin.locked.cmpxchgWeak(false, true, .acquire, .monotonic)) |_| {
                while (spin.locked.load(.monotonic)) std.atomic.spinLoopHint();
            }
        }

        fn unlock(spin: *SpinLock) void {
            spin.locked.store(false, .release);
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        base_io: ?std.Io,
        use_thread: bool,
        poll_interval_ns: u64,
    ) InitError!RuntimeIo {
        const shared = try allocator.create(Shared);
        errdefer allocator.destroy(shared);

        shared.* = .{
            .base = base_io orelse std.Io.failing,
            .poll_interval_ns = poll_interval_ns,
        };

        if (base_io != null and use_thread) {
            shared.worker = try std.Thread.spawn(.{}, workerMain, .{shared});
        }

        return .{ .shared = shared };
    }

    pub fn deinit(runtime_io: *RuntimeIo, allocator: std.mem.Allocator) void {
        const shared = runtime_io.shared;
        {
            shared.lock.lock();
            defer shared.lock.unlock();
            shared.stopping = true;
        }

        if (shared.worker) |worker| worker.join();

        std.debug.assert(shared.active_count == 0);
        allocator.destroy(shared);
        runtime_io.* = undefined;
    }

    pub fn hasWorker(runtime_io: *const RuntimeIo) bool {
        return runtime_io.shared.worker != null;
    }

    pub fn baseIo(runtime_io: *const RuntimeIo) std.Io {
        return runtime_io.shared.base;
    }

    pub fn submit(runtime_io: *RuntimeIo, request: *Request) void {
        const shared = runtime_io.shared;

        shared.lock.lock();
        defer shared.lock.unlock();

        std.debug.assert(shared.worker != null);
        std.debug.assert(!shared.stopping);

        request.next = null;
        request.completed = false;
        if (shared.submissions_tail) |tail| {
            tail.next = request;
        } else {
            shared.submissions_head = request;
        }
        shared.submissions_tail = request;
        shared.active_count += 1;
    }

    pub fn popCompletion(runtime_io: *RuntimeIo) ?*Request {
        const shared = runtime_io.shared;

        shared.lock.lock();
        defer shared.lock.unlock();

        const request = shared.completions_head orelse return null;
        shared.completions_head = request.next;
        if (shared.completions_head == null) shared.completions_tail = null;
        request.next = null;
        shared.active_count -= 1;
        return request;
    }

    pub fn hasPending(runtime_io: *RuntimeIo) bool {
        const shared = runtime_io.shared;

        shared.lock.lock();
        defer shared.lock.unlock();

        return shared.active_count != 0;
    }

    pub fn waitForCompletion(runtime_io: *RuntimeIo) void {
        const shared = runtime_io.shared;

        while (true) {
            shared.lock.lock();
            const done = shared.completions_head != null or shared.active_count == 0;
            shared.lock.unlock();
            if (done) return;

            sleepNs(shared.poll_interval_ns);
        }
    }

    fn popSubmission(shared: *Shared) ?*Request {
        shared.lock.lock();
        defer shared.lock.unlock();

        const request = shared.submissions_head orelse return null;
        shared.submissions_head = request.next;
        if (shared.submissions_head == null) shared.submissions_tail = null;
        request.next = null;
        return request;
    }

    fn pushCompletion(shared: *Shared, request: *Request) void {
        shared.lock.lock();
        defer shared.lock.unlock();

        request.completed = true;
        request.next = null;
        if (shared.completions_tail) |tail| {
            tail.next = request;
        } else {
            shared.completions_head = request;
        }
        shared.completions_tail = request;
    }

    fn shouldStop(shared: *Shared) bool {
        shared.lock.lock();
        defer shared.lock.unlock();

        return shared.stopping and shared.submissions_head == null;
    }

    fn workerMain(shared: *Shared) void {
        while (true) {
            if (popSubmission(shared)) |request| {
                process(shared.base, request);
                pushCompletion(shared, request);
                continue;
            }

            if (shouldStop(shared)) return;
            sleepNs(shared.poll_interval_ns);
        }
    }

    fn process(base_io: std.Io, request: *Request) void {
        switch (request.payload) {
            .operate => |*operate| {
                operate.result = base_io.vtable.operate(base_io.userdata, operate.operation);
            },
            .sleep => |*sleep| {
                sleep.result = base_io.vtable.sleep(base_io.userdata, sleep.timeout);
            },
        }
    }

    fn sleepNs(ns: u64) void {
        if (ns == 0) {
            std.atomic.spinLoopHint();
            return;
        }

        var request: std.c.timespec = .{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };
        while (std.c.nanosleep(&request, &request) != 0) {}
    }
};

pub const ActorIoContext = struct {
    runtime_io: *RuntimeIo,
    charge_runtime: *anyopaque,
    charge_actor: *anyopaque,
    charge_fn: *const fn (*anyopaque, *anyopaque) void,
    park_fn: *const fn (*anyopaque, *anyopaque) void,

    pub fn init(
        runtime_io: *RuntimeIo,
        charge_runtime: *anyopaque,
        charge_actor: *anyopaque,
        charge_fn: *const fn (*anyopaque, *anyopaque) void,
        park_fn: *const fn (*anyopaque, *anyopaque) void,
    ) ActorIoContext {
        return .{
            .runtime_io = runtime_io,
            .charge_runtime = charge_runtime,
            .charge_actor = charge_actor,
            .charge_fn = charge_fn,
            .park_fn = park_fn,
        };
    }

    pub fn interface(context: *ActorIoContext) std.Io {
        return .{
            .userdata = context,
            .vtable = &actor_io_vtable,
        };
    }

    fn charge(context: *ActorIoContext) void {
        context.charge_fn(context.charge_runtime, context.charge_actor);
    }

    fn park(context: *ActorIoContext) void {
        context.park_fn(context.charge_runtime, context.charge_actor);
    }
};

const actor_io_vtable: std.Io.VTable = blk: {
    var vtable = std.Io.failing.vtable.*;
    vtable.async = actorIoAsync;
    vtable.concurrent = actorIoConcurrent;
    vtable.await = actorIoAwait;
    vtable.cancel = actorIoCancel;
    vtable.groupAsync = actorIoGroupAsync;
    vtable.groupConcurrent = actorIoGroupConcurrent;
    vtable.groupAwait = actorIoGroupAwait;
    vtable.groupCancel = actorIoGroupCancel;
    vtable.operate = actorIoOperate;
    vtable.batchAwaitAsync = actorIoBatchAwaitAsync;
    vtable.batchAwaitConcurrent = actorIoBatchAwaitConcurrent;
    vtable.batchCancel = actorIoBatchCancel;
    vtable.sleep = actorIoSleep;
    vtable.random = actorIoRandom;
    vtable.randomSecure = actorIoRandomSecure;
    vtable.now = actorIoNow;
    vtable.clockResolution = actorIoClockResolution;
    break :blk vtable;
};

fn actorIoContext(userdata: ?*anyopaque) *ActorIoContext {
    return @ptrCast(@alignCast(userdata.?));
}

fn actorIoAsync(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: std.mem.Alignment,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*std.Io.AnyFuture {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.async(
        base.userdata,
        result,
        result_alignment,
        context_bytes,
        context_alignment,
        start,
    );
}

fn actorIoConcurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: std.mem.Alignment,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) std.Io.ConcurrentError!*std.Io.AnyFuture {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.concurrent(
        base.userdata,
        result_len,
        result_alignment,
        context_bytes,
        context_alignment,
        start,
    );
}

fn actorIoAwait(
    userdata: ?*anyopaque,
    any_future: *std.Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.await(base.userdata, any_future, result, result_alignment);
}

fn actorIoCancel(
    userdata: ?*anyopaque,
    any_future: *std.Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.cancel(base.userdata, any_future, result, result_alignment);
}

fn actorIoGroupAsync(
    userdata: ?*anyopaque,
    group: *std.Io.Group,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.groupAsync(base.userdata, group, context_bytes, context_alignment, start);
}

fn actorIoGroupConcurrent(
    userdata: ?*anyopaque,
    group: *std.Io.Group,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) std.Io.ConcurrentError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.groupConcurrent(base.userdata, group, context_bytes, context_alignment, start);
}

fn actorIoGroupAwait(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.groupAwait(base.userdata, group, token);
}

fn actorIoGroupCancel(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.groupCancel(base.userdata, group, token);
}

fn actorIoOperate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasWorker()) {
        var request: RuntimeIo.Request = .initOperate(context.charge_actor, operation);
        context.runtime_io.submit(&request);
        while (!request.completed) context.park();
        return request.payload.operate.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.operate(base.userdata, operation);
}

fn actorIoBatchAwaitAsync(userdata: ?*anyopaque, batch: *std.Io.Batch) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.batchAwaitAsync(base.userdata, batch);
}

fn actorIoBatchAwaitConcurrent(
    userdata: ?*anyopaque,
    batch: *std.Io.Batch,
    timeout: std.Io.Timeout,
) std.Io.Batch.AwaitConcurrentError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.batchAwaitConcurrent(base.userdata, batch, timeout);
}

fn actorIoBatchCancel(userdata: ?*anyopaque, batch: *std.Io.Batch) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.batchCancel(base.userdata, batch);
}

fn actorIoSleep(userdata: ?*anyopaque, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasWorker()) {
        var request: RuntimeIo.Request = .initSleep(context.charge_actor, timeout);
        context.runtime_io.submit(&request);
        while (!request.completed) context.park();
        return request.payload.sleep.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.sleep(base.userdata, timeout);
}

fn actorIoRandom(userdata: ?*anyopaque, buffer: []u8) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.random(base.userdata, buffer);
}

fn actorIoRandomSecure(userdata: ?*anyopaque, buffer: []u8) std.Io.RandomSecureError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.randomSecure(base.userdata, buffer);
}

fn actorIoNow(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
    const context = actorIoContext(userdata);
    const base = context.runtime_io.baseIo();
    return base.vtable.now(base.userdata, clock);
}

fn actorIoClockResolution(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Clock.ResolutionError!std.Io.Duration {
    const context = actorIoContext(userdata);
    const base = context.runtime_io.baseIo();
    return base.vtable.clockResolution(base.userdata, clock);
}
