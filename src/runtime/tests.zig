//! Runtime behavior tests.

const std = @import("std");
const runtime = @import("../Runtime.zig");

const Actor = runtime.Actor;
const ActorId = runtime.ActorId;
const Ctx = runtime.Ctx;
const Runtime = runtime.Runtime;
const TraceEvent = runtime.TraceEvent;
const Tracer = runtime.Tracer;

test "function actor counter" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const CounterMsg = union(enum) {
        inc: u64,
        get: Actor(Reply),
        stop,
    };

    const Actors = struct {
        fn counter(ctx: *Ctx(CounterMsg)) !void {
            var value: u64 = 0;

            while (true) {
                switch (try ctx.recv()) {
                    .inc => |n| value += n,
                    .get => |reply_to| try reply_to.send(.{ .value = value }),
                    .stop => return,
                }
            }
        }

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const counter = try rt.spawn(Actors.counter);

    try counter.send(.{ .inc = 40 });
    try counter.send(.{ .inc = 2 });
    try counter.send(.{ .get = collector });
    try counter.send(.stop);

    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "struct actor counter" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const CounterMsg = union(enum) {
        inc: u64,
        get: Actor(Reply),
        stop,
    };

    const Actors = struct {
        const Counter = struct {
            pub const Msg = CounterMsg;

            initial: u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                var value = self.initial;

                while (true) {
                    switch (try ctx.recv()) {
                        .inc => |n| value += n,
                        .get => |reply_to| try reply_to.send(.{ .value = value }),
                        .stop => return,
                    }
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const counter = try rt.spawn(Actors.Counter{ .initial = 10 });

    try counter.send(.{ .inc = 32 });
    try counter.send(.{ .get = collector });
    try counter.send(.stop);

    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "yield lets another runnable actor run" {
    const testing = std.testing;

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const WorkerMsg = union(enum) {
        start: Actor(OtherMsg),
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |other| {
                        self.trace.push('a');
                        try other.send(.hit);
                        ctx.yield();
                        self.trace.push('b');
                        ctx.yield();
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('x'),
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{ .execution_budget = 1 });
    defer rt.deinit();

    var timeline: Trace = .{};
    const other = try rt.spawn(Actors.Other{ .trace = &timeline });
    const worker = try rt.spawn(Actors.Worker{ .trace = &timeline });

    try worker.send(.{ .start = other });
    try rt.run();

    try testing.expectEqualStrings("axbc", timeline.slice());
}

test "execution budget controls yield switching" {
    const testing = std.testing;

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const WorkerMsg = union(enum) {
        start: Actor(OtherMsg),
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |other| {
                        self.trace.push('a');
                        try other.send(.hit);
                        ctx.yield();
                        self.trace.push('b');
                        ctx.yield();
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('x'),
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{ .execution_budget = 2 });
    defer rt.deinit();

    var timeline: Trace = .{};
    const other = try rt.spawn(Actors.Other{ .trace = &timeline });
    const worker = try rt.spawn(Actors.Worker{ .trace = &timeline });

    try worker.send(.{ .start = other });
    try rt.run();

    try testing.expectEqualStrings("abxc", timeline.slice());
}

test "actor can spawn child actor" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const ChildMsg = union(enum) {
        report: Actor(Reply),
    };

    const ParentMsg = union(enum) {
        start: Actor(Reply),
    };

    const Actors = struct {
        const Child = struct {
            pub const Msg = ChildMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .report => |reply_to| try reply_to.send(.{ .value = 42 }),
                }
            }
        };

        const Parent = struct {
            pub const Msg = ParentMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |reply_to| {
                        const child = try ctx.spawn(Child{});
                        try child.send(.{ .report = reply_to });
                    },
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const parent = try rt.spawn(Actors.Parent{});

    try parent.send(.{ .start = collector });
    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "run parks waiting actors until later sends wake them" {
    const testing = std.testing;

    const RecorderMsg = union(enum) {
        record: u8,
        stop,
    };

    const Recorder = struct {
        pub const Msg = RecorderMsg;

        items: *[4]u8,
        len: *usize,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            while (true) {
                switch (try ctx.recv()) {
                    .record => |item| {
                        self.items[self.len.*] = item;
                        self.len.* += 1;
                    },
                    .stop => return,
                }
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var items: [4]u8 = undefined;
    var len: usize = 0;
    const recorder = try rt.spawn(Recorder{
        .items = &items,
        .len = &len,
    });

    try rt.run();
    try testing.expectEqual(@as(usize, 0), len);

    try recorder.send(.{ .record = 'a' });
    try rt.run();
    try testing.expectEqualStrings("a", items[0..len]);

    try recorder.send(.{ .record = 'b' });
    try recorder.send(.stop);
    try rt.run();

    try testing.expectEqualStrings("ab", items[0..len]);
    try testing.expectError(error.InvalidActor, recorder.send(.stop));
}

test "messages to one actor are received in FIFO order" {
    const testing = std.testing;

    const SinkMsg = union(enum) {
        value: u16,
        stop,
    };

    const Sink = struct {
        pub const Msg = SinkMsg;

        values: *[64]u16,
        len: *usize,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            while (true) {
                switch (try ctx.recv()) {
                    .value => |value| {
                        self.values[self.len.*] = value;
                        self.len.* += 1;
                    },
                    .stop => return,
                }
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: [64]u16 = undefined;
    var observed_len: usize = 0;
    const sink = try rt.spawn(Sink{
        .values = &observed,
        .len = &observed_len,
    });

    for (0..observed.len) |i| {
        try sink.send(.{ .value = @intCast(i * 3 + 1) });
    }
    try sink.send(.stop);
    try rt.run();

    try testing.expectEqual(observed.len, observed_len);
    for (observed[0..observed_len], 0..) |value, i| {
        try testing.expectEqual(@as(u16, @intCast(i * 3 + 1)), value);
    }
}

test "actor self handle can enqueue messages to itself" {
    const testing = std.testing;

    const SelfMsg = union(enum) {
        start: u8,
        tick: u8,
    };

    const SelfSender = struct {
        pub const Msg = SelfMsg;

        items: *[8]u8,
        len: *usize,
        limit: u8 = 0,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            while (true) {
                switch (try ctx.recv()) {
                    .start => |limit| {
                        self.limit = limit;
                        try ctx.self().send(.{ .tick = 0 });
                    },
                    .tick => |value| {
                        self.items[self.len.*] = value;
                        self.len.* += 1;
                        if (value + 1 == self.limit) return;

                        try ctx.self().send(.{ .tick = value + 1 });
                    },
                }
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{ .execution_budget = 1 });
    defer rt.deinit();

    var observed: [8]u8 = undefined;
    var observed_len: usize = 0;
    const self_sender = try rt.spawn(SelfSender{
        .items = &observed,
        .len = &observed_len,
    });

    try self_sender.send(.{ .start = 5 });
    try rt.run();

    try testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3, 4 }, observed[0..observed_len]);
}

test "raw send rejects wrong message type" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const StopActor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .stop => return,
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const stopped = try rt.spawn(StopActor{});
    try testing.expectError(error.WrongMessageType, rt.send(OtherMsg, stopped.any(), .hit));

    try stopped.send(.stop);
    try rt.run();
}

test "actor failure destroys actor and invalidates handle" {
    const testing = std.testing;

    const FailingMsg = union(enum) {
        go,
    };

    const FailingActor = struct {
        pub const Msg = FailingMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .go => return error.Boom,
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const failing = try rt.spawn(FailingActor{});
    try failing.send(.go);

    try testing.expectError(error.Boom, rt.run());
    try testing.expectError(error.InvalidActor, failing.send(.go));
}

test "message sends structurally copy values but preserve references" {
    const testing = std.testing;

    const Payload = struct {
        value: u64,
        reference: *u64,
    };

    const ObserverMsg = union(enum) {
        sample: Payload,
    };

    const Observer = struct {
        pub const Msg = ObserverMsg;

        copied_value: *u64,
        referenced_value: *u64,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .sample => |payload| {
                    self.copied_value.* = payload.value;
                    self.referenced_value.* = payload.reference.*;
                },
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var referenced: u64 = 1;
    var copied_value: u64 = 0;
    var referenced_value: u64 = 0;
    const observer = try rt.spawn(Observer{
        .copied_value = &copied_value,
        .referenced_value = &referenced_value,
    });

    var payload: Payload = .{
        .value = 7,
        .reference = &referenced,
    };
    try observer.send(.{ .sample = payload });

    payload.value = 99;
    referenced = 42;

    try rt.run();

    try testing.expectEqual(@as(u64, 7), copied_value);
    try testing.expectEqual(@as(u64, 42), referenced_value);
}

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
    var rt = Runtime.init(testing.allocator, .{ .tracer = recorder.tracer() });
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
    var rt = Runtime.init(testing.allocator, .{ .tracer = recorder.tracer() });
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

test "randomized counter command streams match a reference model" {
    const testing = std.testing;

    const MaxOps = 128;
    const Command = union(enum) {
        add: i64,
        get,
    };

    const Reply = union(enum) {
        value: i64,
    };

    const CounterMsg = union(enum) {
        add: i64,
        get: Actor(Reply),
        stop,
    };

    const Actors = struct {
        const Counter = struct {
            pub const Msg = CounterMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                var value: i64 = 0;

                while (true) {
                    switch (try ctx.recv()) {
                        .add => |delta| value += delta,
                        .get => |reply_to| try reply_to.send(.{ .value = value }),
                        .stop => return,
                    }

                    ctx.yield();
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            values: *[MaxOps]i64,
            len: *usize,
            expected_len: usize,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                while (self.len.* < self.expected_len) {
                    switch (try ctx.recv()) {
                        .value => |value| {
                            self.values[self.len.*] = value;
                            self.len.* += 1;
                        },
                    }
                }
            }
        };
    };

    const seeds = [_]u64{
        0x1,
        0x1234_5678,
        0xfeed_face_cafe_beef,
        0x9e37_79b9_7f4a_7c15,
    };

    for (seeds) |seed| {
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();

        var commands: [MaxOps]Command = undefined;
        var expected: [MaxOps]i64 = undefined;
        var expected_len: usize = 0;
        var model: i64 = 0;

        for (commands[0..]) |*command| {
            const action = random.uintLessThan(u8, 5);
            if (action <= 2) {
                const delta = random.intRangeAtMost(i64, -50, 50);
                command.* = .{ .add = delta };
                model += delta;
            } else {
                command.* = .get;
                expected[expected_len] = model;
                expected_len += 1;
            }
        }

        var rt = Runtime.init(testing.allocator, .{
            .execution_budget = @as(usize, @intCast(seed % 4 + 1)),
        });
        defer rt.deinit();

        var observed: [MaxOps]i64 = undefined;
        var observed_len: usize = 0;
        const collector = try rt.spawn(Actors.Collector{
            .values = &observed,
            .len = &observed_len,
            .expected_len = expected_len,
        });
        const counter = try rt.spawn(Actors.Counter{});

        for (commands) |command| {
            switch (command) {
                .add => |delta| try counter.send(.{ .add = delta }),
                .get => try counter.send(.{ .get = collector }),
            }
        }
        try counter.send(.stop);

        try rt.run();
        try testing.expectEqualSlices(i64, expected[0..expected_len], observed[0..observed_len]);
    }
}

test "completed actors release their registry slot" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const StopActor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .stop => return,
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const first = try rt.spawn(StopActor{});
    try first.send(.stop);
    try rt.run();

    try testing.expectError(error.InvalidActor, first.send(.stop));

    const second = try rt.spawn(StopActor{});
    try testing.expectEqual(first.any().index, second.any().index);
    try testing.expect(first.any().generation != second.any().generation);

    try second.send(.stop);
    try rt.run();
}

test "failed spawn releases reserved actor id" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const StopActor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .stop => return,
            }
        }
    };

    var rt = Runtime.init(testing.allocator, .{ .stack_size = 1 });
    defer rt.deinit();

    try testing.expectError(error.StackTooSmall, rt.spawn(StopActor{}));

    rt.options.stack_size = 64 * 1024;
    const stopped = try rt.spawn(StopActor{});
    try testing.expectEqual(@as(usize, 0), stopped.any().index);

    try stopped.send(.stop);
    try rt.run();
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
    var rt = Runtime.init(testing.allocator, .{ .tracer = recorder.tracer() });
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
