//! Runtime-owned wrappers for Zig's standard I/O interface.

const std = @import("std");

pub const RuntimeIo = struct {
    shared: *Shared,

    pub const InitError = std.mem.Allocator.Error;

    /// Non-blocking actor I/O backend.
    ///
    /// `submit_fn` must return without blocking the scheduler. It may complete
    /// the request immediately or retain it and call the matching `complete*`
    /// method later from a poller/event callback.
    pub const Driver = struct {
        context: ?*anyopaque = null,
        submit_fn: *const fn (?*anyopaque, *Request) void,
        poll_fn: ?*const fn (?*anyopaque, PollMode) anyerror!void = null,

        pub fn submit(driver: Driver, request: *Request) void {
            driver.submit_fn(driver.context, request);
        }

        pub fn poll(driver: Driver, mode: PollMode) !void {
            const poll_fn = driver.poll_fn orelse return;
            try poll_fn(driver.context, mode);
        }
    };

    pub const PollMode = enum {
        nonblocking,
        wait,
    };

    pub const Request = struct {
        next: ?*Request = null,
        completion_next: ?*Request = null,
        actor: *anyopaque,
        complete_context: ?*anyopaque = null,
        complete_fn: ?*const fn (?*anyopaque, *Request) void = null,
        completed: std.atomic.Value(bool) = .init(false),
        payload: Payload,

        pub const Payload = union(enum) {
            operate: Operate,
            file_read_positional: FileReadPositional,
            file_write_positional: FileWritePositional,
            net_read: NetRead,
            net_write: NetWrite,
            batch_await_async: BatchAwaitAsync,
            batch_await_concurrent: BatchAwaitConcurrent,
            sleep: Sleep,
        };

        pub const Operate = struct {
            operation: std.Io.Operation,
            result: std.Io.Cancelable!std.Io.Operation.Result = undefined,
        };

        pub const FileReadPositional = struct {
            file: std.Io.File,
            data: []const []u8,
            offset: u64,
            result: std.Io.File.ReadPositionalError!usize = undefined,
        };

        pub const FileWritePositional = struct {
            file: std.Io.File,
            header: []const u8,
            data: []const []const u8,
            splat: usize,
            offset: u64,
            result: std.Io.File.WritePositionalError!usize = undefined,
        };

        pub const NetRead = struct {
            handle: std.Io.net.Socket.Handle,
            data: [][]u8,
            result: std.Io.net.Stream.Reader.Error!usize = undefined,
        };

        pub const NetWrite = struct {
            handle: std.Io.net.Socket.Handle,
            header: []const u8,
            data: []const []const u8,
            splat: usize,
            result: std.Io.net.Stream.Writer.Error!usize = undefined,
        };

        pub const Sleep = struct {
            timeout: std.Io.Timeout,
            result: std.Io.Cancelable!void = undefined,
        };

        pub const BatchAwaitAsync = struct {
            batch: *std.Io.Batch,
            result: std.Io.Cancelable!void = undefined,
        };

        pub const BatchAwaitConcurrent = struct {
            batch: *std.Io.Batch,
            timeout: std.Io.Timeout,
            result: std.Io.Batch.AwaitConcurrentError!void = undefined,
        };

        pub fn initOperate(actor: *anyopaque, operation: std.Io.Operation) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .operate = .{ .operation = operation },
                },
            };
        }

        pub fn initFileReadPositional(actor: *anyopaque, file: std.Io.File, data: []const []u8, offset: u64) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .file_read_positional = .{
                        .file = file,
                        .data = data,
                        .offset = offset,
                    },
                },
            };
        }

        pub fn initFileWritePositional(
            actor: *anyopaque,
            file: std.Io.File,
            header: []const u8,
            data: []const []const u8,
            splat: usize,
            offset: u64,
        ) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .file_write_positional = .{
                        .file = file,
                        .header = header,
                        .data = data,
                        .splat = splat,
                        .offset = offset,
                    },
                },
            };
        }

        pub fn initNetRead(actor: *anyopaque, handle: std.Io.net.Socket.Handle, data: [][]u8) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .net_read = .{
                        .handle = handle,
                        .data = data,
                    },
                },
            };
        }

        pub fn initNetWrite(
            actor: *anyopaque,
            handle: std.Io.net.Socket.Handle,
            header: []const u8,
            data: []const []const u8,
            splat: usize,
        ) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .net_write = .{
                        .handle = handle,
                        .header = header,
                        .data = data,
                        .splat = splat,
                    },
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

        pub fn initBatchAwaitAsync(actor: *anyopaque, batch: *std.Io.Batch) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .batch_await_async = .{ .batch = batch },
                },
            };
        }

        pub fn initBatchAwaitConcurrent(actor: *anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) Request {
            return .{
                .actor = actor,
                .payload = .{
                    .batch_await_concurrent = .{
                        .batch = batch,
                        .timeout = timeout,
                    },
                },
            };
        }

        pub fn completeOperate(request: *Request, result: std.Io.Cancelable!std.Io.Operation.Result) void {
            request.payload.operate.result = result;
            request.complete();
        }

        pub fn completeFileReadPositional(request: *Request, result: std.Io.File.ReadPositionalError!usize) void {
            request.payload.file_read_positional.result = result;
            request.complete();
        }

        pub fn completeFileWritePositional(request: *Request, result: std.Io.File.WritePositionalError!usize) void {
            request.payload.file_write_positional.result = result;
            request.complete();
        }

        pub fn completeNetRead(request: *Request, result: std.Io.net.Stream.Reader.Error!usize) void {
            request.payload.net_read.result = result;
            request.complete();
        }

        pub fn completeNetWrite(request: *Request, result: std.Io.net.Stream.Writer.Error!usize) void {
            request.payload.net_write.result = result;
            request.complete();
        }

        pub fn completeSleep(request: *Request, result: std.Io.Cancelable!void) void {
            request.payload.sleep.result = result;
            request.complete();
        }

        pub fn completeBatchAwaitAsync(request: *Request, result: std.Io.Cancelable!void) void {
            request.payload.batch_await_async.result = result;
            request.complete();
        }

        pub fn completeBatchAwaitConcurrent(request: *Request, result: std.Io.Batch.AwaitConcurrentError!void) void {
            request.payload.batch_await_concurrent.result = result;
            request.complete();
        }

        fn complete(request: *Request) void {
            const complete_fn = request.complete_fn orelse @panic("I/O request has no completion owner");
            complete_fn(request.complete_context, request);
        }
    };

    const Shared = struct {
        driver: ?Driver,
        completions_incoming: std.atomic.Value(?*Request) = .init(null),
        completions_mutex: std.Io.Mutex = .init,
        completions_local: ?*Request = null,
        pending_count: std.atomic.Value(usize) = .init(0),
        notify_context: ?*anyopaque = null,
        notify_fn: ?*const fn (?*anyopaque, *Request) void = null,
    };

    pub fn init(allocator: std.mem.Allocator, driver: ?Driver) InitError!RuntimeIo {
        const shared = try allocator.create(Shared);
        errdefer allocator.destroy(shared);

        shared.* = .{
            .driver = driver,
        };

        return .{ .shared = shared };
    }

    pub fn deinit(runtime_io: *RuntimeIo, allocator: std.mem.Allocator) void {
        const shared = runtime_io.shared;
        discardCompletions(shared);
        std.debug.assert(shared.pending_count.load(.acquire) == 0);
        allocator.destroy(shared);
        runtime_io.* = undefined;
    }

    pub fn hasDriver(runtime_io: *const RuntimeIo) bool {
        return runtime_io.shared.driver != null;
    }

    pub fn baseIo(runtime_io: *const RuntimeIo) std.Io {
        _ = runtime_io;
        return std.Io.failing;
    }

    pub fn submit(runtime_io: *RuntimeIo, request: *Request) void {
        const shared = runtime_io.shared;
        const driver = shared.driver orelse @panic("actor I/O submitted without an I/O driver");

        request.next = null;
        request.completion_next = null;
        request.complete_context = shared;
        request.complete_fn = completeRequest;
        request.completed.store(false, .monotonic);
        _ = shared.pending_count.fetchAdd(1, .release);

        driver.submit(request);
    }

    pub fn setCompletionNotify(
        runtime_io: *RuntimeIo,
        context: ?*anyopaque,
        notify_fn: ?*const fn (?*anyopaque, *Request) void,
    ) void {
        runtime_io.shared.notify_context = context;
        runtime_io.shared.notify_fn = notify_fn;
    }

    pub fn hasPoller(runtime_io: *const RuntimeIo) bool {
        const driver = runtime_io.shared.driver orelse return false;
        return driver.poll_fn != null;
    }

    pub fn poll(runtime_io: *RuntimeIo, mode: PollMode) !void {
        const driver = runtime_io.shared.driver orelse return;
        try driver.poll(mode);
    }

    pub fn popCompletion(runtime_io: *RuntimeIo, sync_io: std.Io) ?*Request {
        const shared = runtime_io.shared;
        shared.completions_mutex.lockUncancelable(sync_io);
        defer shared.completions_mutex.unlock(sync_io);

        return popSharedCompletion(shared);
    }

    pub fn hasPending(runtime_io: *RuntimeIo) bool {
        return runtime_io.shared.pending_count.load(.acquire) != 0;
    }

    fn completeRequest(context: ?*anyopaque, request: *Request) void {
        const shared: *Shared = @ptrCast(@alignCast(context.?));

        request.completed.store(true, .release);
        var head = shared.completions_incoming.load(.monotonic);
        while (true) {
            request.completion_next = head;
            head = shared.completions_incoming.cmpxchgWeak(head, request, .release, .monotonic) orelse break;
        }
        if (shared.notify_fn) |notify_fn| notify_fn(shared.notify_context, request);
    }

    fn popSharedCompletion(shared: *Shared) ?*Request {
        if (shared.completions_local == null) refillLocalCompletions(shared);

        const request = shared.completions_local orelse return null;
        shared.completions_local = request.completion_next;
        request.completion_next = null;
        _ = shared.pending_count.fetchSub(1, .acq_rel);
        return request;
    }

    fn refillLocalCompletions(shared: *Shared) void {
        var stack = shared.completions_incoming.swap(null, .acquire);
        var reversed: ?*Request = null;
        while (stack) |request| {
            stack = request.completion_next;
            request.completion_next = reversed;
            reversed = request;
        }
        shared.completions_local = reversed;
    }

    fn discardCompletions(shared: *Shared) void {
        while (popSharedCompletion(shared)) |_| {}
    }
};

test "runtime io completion queue accepts concurrent producers" {
    const testing = std.testing;

    const ProducerCount = 4;
    const PerProducer = 256;
    const Total = ProducerCount * PerProducer;

    const ImmediateDriver = struct {
        fn driver() RuntimeIo.Driver {
            return .{ .submit_fn = submit };
        }

        fn submit(_: ?*anyopaque, request: *RuntimeIo.Request) void {
            request.completeSleep({});
        }
    };

    const Producer = struct {
        runtime_io: *RuntimeIo,
        requests: []RuntimeIo.Request,
        actors: []u8,

        fn run(self: @This()) void {
            for (self.requests, 0..) |*request, index| {
                request.* = .initSleep(&self.actors[index], .none);
                self.runtime_io.submit(request);
            }
        }
    };

    var runtime_io = try RuntimeIo.init(testing.allocator, ImmediateDriver.driver());
    defer runtime_io.deinit(testing.allocator);

    var requests: [Total]RuntimeIo.Request = undefined;
    var actors: [Total]u8 = undefined;
    var threads: [ProducerCount]std.Thread = undefined;

    for (&threads, 0..) |*thread, producer| {
        const start = producer * PerProducer;
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{Producer{
            .runtime_io = &runtime_io,
            .requests = requests[start..][0..PerProducer],
            .actors = actors[start..][0..PerProducer],
        }});
    }
    for (threads) |thread| thread.join();

    var seen = [_]bool{false} ** Total;
    var count: usize = 0;
    while (runtime_io.popCompletion(testing.io)) |request| {
        const index = @intFromPtr(request.actor) - @intFromPtr(&actors[0]);
        try testing.expect(index < Total);
        try testing.expect(!seen[index]);
        seen[index] = true;
        count += 1;
    }

    try testing.expectEqual(@as(usize, Total), count);
    try testing.expect(!runtime_io.hasPending());
    for (seen) |item_seen| try testing.expect(item_seen);
}

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

    fn wait(context: *ActorIoContext, request: *RuntimeIo.Request) void {
        context.runtime_io.submit(request);

        // Always park once so the scheduler drains the completion before the
        // actor can return and unwind this stack-backed request.
        context.park();
        while (!request.completed.load(.acquire)) context.park();
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
    vtable.fileReadPositional = actorIoFileReadPositional;
    vtable.fileWritePositional = actorIoFileWritePositional;
    vtable.netRead = actorIoNetRead;
    vtable.netWrite = actorIoNetWrite;
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
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initOperate(context.charge_actor, operation);
        context.wait(&request);
        return request.payload.operate.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.operate(base.userdata, operation);
}

fn actorIoBatchAwaitAsync(userdata: ?*anyopaque, batch: *std.Io.Batch) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initBatchAwaitAsync(context.charge_actor, batch);
        context.wait(&request);
        return request.payload.batch_await_async.result;
    }

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
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initBatchAwaitConcurrent(context.charge_actor, batch, timeout);
        context.wait(&request);
        return request.payload.batch_await_concurrent.result;
    }

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

fn actorIoFileReadPositional(
    userdata: ?*anyopaque,
    file: std.Io.File,
    data: []const []u8,
    offset: u64,
) std.Io.File.ReadPositionalError!usize {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initFileReadPositional(context.charge_actor, file, data, offset);
        context.wait(&request);
        return request.payload.file_read_positional.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.fileReadPositional(base.userdata, file, data, offset);
}

fn actorIoFileWritePositional(
    userdata: ?*anyopaque,
    file: std.Io.File,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
    offset: u64,
) std.Io.File.WritePositionalError!usize {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initFileWritePositional(context.charge_actor, file, header, data, splat, offset);
        context.wait(&request);
        return request.payload.file_write_positional.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.fileWritePositional(base.userdata, file, header, data, splat, offset);
}

fn actorIoNetRead(
    userdata: ?*anyopaque,
    handle: std.Io.net.Socket.Handle,
    data: [][]u8,
) std.Io.net.Stream.Reader.Error!usize {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initNetRead(context.charge_actor, handle, data);
        context.wait(&request);
        return request.payload.net_read.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.netRead(base.userdata, handle, data);
}

fn actorIoNetWrite(
    userdata: ?*anyopaque,
    handle: std.Io.net.Socket.Handle,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) std.Io.net.Stream.Writer.Error!usize {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initNetWrite(context.charge_actor, handle, header, data, splat);
        context.wait(&request);
        return request.payload.net_write.result;
    }

    defer context.charge();
    const base = context.runtime_io.baseIo();
    return base.vtable.netWrite(base.userdata, handle, header, data, splat);
}

fn actorIoSleep(userdata: ?*anyopaque, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    if (context.runtime_io.hasDriver()) {
        var request: RuntimeIo.Request = .initSleep(context.charge_actor, timeout);
        context.wait(&request);
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
