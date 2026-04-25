const common = @import("common.zig");
const std = common.std;
const Actor = common.Actor;
const ActorId = common.ActorId;
const Ctx = common.Ctx;
const IoDriver = common.IoDriver;
const IoPollMode = common.IoPollMode;
const IoRequest = common.IoRequest;
const Runtime = common.Runtime;
const TraceEvent = common.TraceEvent;
const Tracer = common.Tracer;

test "tracer records actor-originated sends" {
    const testing = std.testing;

    const Reply = union(enum) {
        ok,
    };

    const SenderMsg = union(enum) {
        start: Actor(Reply),
    };

    const Recorder = struct {
        events: [32]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(recorder: *@This()) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *@This() = @ptrCast(@alignCast(context.?));
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };

    const Actors = struct {
        const Collector = struct {
            pub const Msg = Reply;

            seen: *bool,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .ok => self.seen.* = true,
                }
            }
        };

        const Sender = struct {
            pub const Msg = SenderMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |reply_to| try reply_to.send(.ok),
                }
            }
        };
    };

    var recorder: Recorder = .{};
    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1, .tracer = recorder.tracer() });
    defer rt.deinit();

    var seen = false;
    const collector = try rt.spawn(Actors.Collector{ .seen = &seen });
    const sender = try rt.spawn(Actors.Sender{});

    try sender.send(.{ .start = collector });
    try rt.run();

    try testing.expect(seen);

    var found_actor_send = false;
    for (recorder.events[0..recorder.len]) |event| {
        switch (event) {
            .message_sent => |message| {
                if (message.from != null and
                    std.meta.eql(message.from.?, sender.any()) and
                    std.meta.eql(message.to, collector.any()))
                {
                    found_actor_send = true;
                }
            },
            else => {},
        }
    }

    try testing.expect(found_actor_send);
}

test "tracer records actor failures" {
    const testing = std.testing;

    const FailureMsg = union(enum) {
        go,
    };

    const Recorder = struct {
        events: [16]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(recorder: *@This()) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *@This() = @ptrCast(@alignCast(context.?));
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };

    const FailingActor = struct {
        pub const Msg = FailureMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .go => return error.Boom,
            }
        }
    };

    var recorder: Recorder = .{};
    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1, .tracer = recorder.tracer() });
    defer rt.deinit();

    const failing = try rt.spawn(FailingActor{});
    try failing.send(.go);

    try testing.expectError(error.Boom, rt.run());

    var found_failure = false;
    for (recorder.events[0..recorder.len]) |event| {
        switch (event) {
            .actor_failed => |failure| {
                if (std.meta.eql(failure.actor, failing.any()) and failure.err == error.Boom) {
                    found_failure = true;
                }
            },
            else => {},
        }
    }

    try testing.expect(found_failure);
}

test "tracer records runtime events" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Recorder = struct {
        events: [16]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(recorder: *@This()) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *@This() = @ptrCast(@alignCast(context.?));
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };

    const StopActor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(StopMsg)) !void {
            switch (try ctx.recv()) {
                .stop => return,
            }
        }
    };

    var recorder: Recorder = .{};
    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1, .tracer = recorder.tracer() });
    defer rt.deinit();

    const stopped = try rt.spawn(StopActor{});
    try stopped.send(.stop);
    try rt.run();

    try testing.expectEqual(@as(usize, 5), recorder.len);

    switch (recorder.events[0]) {
        .actor_spawned => |actor_id| try testing.expectEqual(stopped.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[1]) {
        .message_sent => |message| {
            try testing.expectEqual(@as(?ActorId, null), message.from);
            try testing.expectEqual(stopped.any(), message.to);
        },
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[2]) {
        .actor_resumed => |actor_id| try testing.expectEqual(stopped.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[3]) {
        .message_received => |actor_id| try testing.expectEqual(stopped.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[4]) {
        .actor_completed => |actor_id| try testing.expectEqual(stopped.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
}

test "tracer records actor io events" {
    const testing = std.testing;

    const FakeDriver = struct {
        pending: ?*IoRequest = null,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
                .poll_fn = poll,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.pending = request;
        }

        fn poll(context: ?*anyopaque, _: IoPollMode) !void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            const request = self.pending orelse return;
            self.pending = null;
            request.completeOperate(.{ .file_read_streaming = 1 });
        }
    };

    const WorkerMsg = union(enum) {
        start,
    };

    const Recorder = struct {
        events: [16]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(recorder: *@This()) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *@This() = @ptrCast(@alignCast(context.?));
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };

    const Worker = struct {
        pub const Msg = WorkerMsg;

        pub fn run(_: *@This(), ctx: *Ctx(WorkerMsg)) !void {
            switch (try ctx.recv()) {
                .start => {
                    var buffer: [1]u8 = undefined;
                    var buffers = [_][]u8{buffer[0..]};
                    const result = try std.Io.operate(ctx.io(), .{
                        .file_read_streaming = .{
                            .file = .stdin(),
                            .data = &buffers,
                        },
                    });
                    switch (result) {
                        .file_read_streaming => |read_result| {
                            try testing.expectEqual(@as(usize, 1), try read_result);
                        },
                        else => unreachable,
                    }
                },
            }
        }
    };

    var fake_driver: FakeDriver = .{};
    var recorder: Recorder = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 1,
        .io = fake_driver.driver(),
        .tracer = recorder.tracer(),
    });
    defer rt.deinit();

    const worker = try rt.spawn(Worker{});
    try worker.send(.start);
    try rt.run();

    try testing.expectEqual(@as(usize, 9), recorder.len);

    switch (recorder.events[0]) {
        .actor_spawned => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[1]) {
        .message_sent => |message| {
            try testing.expectEqual(@as(?ActorId, null), message.from);
            try testing.expectEqual(worker.any(), message.to);
        },
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[2]) {
        .actor_resumed => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[3]) {
        .message_received => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[4]) {
        .io_submitted => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[5]) {
        .actor_waiting => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[6]) {
        .io_completed => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[7]) {
        .actor_resumed => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
    switch (recorder.events[8]) {
        .actor_completed => |actor_id| try testing.expectEqual(worker.any(), actor_id),
        else => return error.UnexpectedTraceEvent,
    }
}
