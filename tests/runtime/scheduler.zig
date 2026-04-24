const common = @import("common.zig");
const std = common.std;
const Actor = common.Actor;
const Ctx = common.Ctx;
const Runtime = common.Runtime;

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

    var rt = try Runtime.init(testing.allocator, .{ .execution_budget = 1 });
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

    var rt = try Runtime.init(testing.allocator, .{ .execution_budget = 2 });
    defer rt.deinit();

    var timeline: Trace = .{};
    const other = try rt.spawn(Actors.Other{ .trace = &timeline });
    const worker = try rt.spawn(Actors.Worker{ .trace = &timeline });

    try worker.send(.{ .start = other });
    try rt.run();

    try testing.expectEqualStrings("abxc", timeline.slice());
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

    var rt = try Runtime.init(testing.allocator, .{});
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

    var rt = try Runtime.init(testing.allocator, .{});
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

    var rt = try Runtime.init(testing.allocator, .{ .execution_budget = 1 });
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

test "runtime accepts configured worker count before SMP run mode" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Stopper = struct {
        pub const Msg = StopMsg;

        stopped: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .stop => self.stopped.* = true,
            }
        }
    };

    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 4 });
    defer rt.deinit();

    var stopped = false;
    const stopper = try rt.spawn(Stopper{ .stopped = &stopped });

    try stopper.send(.stop);
    try rt.run();

    try testing.expect(stopped);
}
