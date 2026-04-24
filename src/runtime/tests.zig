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
