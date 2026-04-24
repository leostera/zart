const common = @import("common.zig");
const std = common.std;
const Actor = common.Actor;
const Ctx = common.Ctx;
const Runtime = common.Runtime;
const zart = common.zart;

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

    const interleaver = zart.testing.Interleaver.init(.{
        .min_execution_budget = 1,
        .max_execution_budget = 4,
        .min_io_budget = 1,
        .max_io_budget = 1,
    });

    for (zart.testing.seeds[0..4]) |seed| {
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();
        const schedule = interleaver.case(seed);

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

        var rt = try Runtime.init(testing.allocator, .{
            .worker_count = 1,
            .execution_budget = schedule.execution_budget,
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

test "randomized multi-counter routing matches a reference model" {
    const testing = std.testing;

    const CounterCount = 4;
    const MaxOps = 192;

    const Command = union(enum) {
        add: struct {
            counter: usize,
            delta: i64,
        },
        get: struct {
            counter: usize,
            sequence: usize,
        },
    };

    const Reply = union(enum) {
        value: struct {
            sequence: usize,
            counter: usize,
            value: i64,
        },
    };

    const CounterMsg = union(enum) {
        add: i64,
        get: struct {
            sequence: usize,
            reply_to: Actor(Reply),
        },
        stop,
    };

    const Actors = struct {
        const Counter = struct {
            pub const Msg = CounterMsg;

            index: usize,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                var value: i64 = 0;

                while (true) {
                    switch (try ctx.recv()) {
                        .add => |delta| value += delta,
                        .get => |request| {
                            try request.reply_to.send(.{
                                .value = .{
                                    .sequence = request.sequence,
                                    .counter = self.index,
                                    .value = value,
                                },
                            });
                        },
                        .stop => return,
                    }

                    ctx.yield();
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            values: *[MaxOps]i64,
            counters: *[MaxOps]usize,
            seen: *[MaxOps]bool,
            len: *usize,
            expected_len: usize,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                while (self.len.* < self.expected_len) {
                    switch (try ctx.recv()) {
                        .value => |reply| {
                            try testing.expect(reply.sequence < self.expected_len);
                            try testing.expect(!self.seen[reply.sequence]);
                            self.values[reply.sequence] = reply.value;
                            self.counters[reply.sequence] = reply.counter;
                            self.seen[reply.sequence] = true;
                            self.len.* += 1;
                        },
                    }
                }
            }
        };
    };

    const interleaver = zart.testing.Interleaver.init(.{
        .min_execution_budget = 1,
        .max_execution_budget = 5,
        .min_io_budget = 1,
        .max_io_budget = 1,
    });

    for (zart.testing.seeds[2..]) |seed| {
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();
        const schedule = interleaver.case(seed);

        var commands: [MaxOps]Command = undefined;
        var expected_values: [MaxOps]i64 = undefined;
        var expected_counters: [MaxOps]usize = undefined;
        var expected_len: usize = 0;
        var model = [_]i64{0} ** CounterCount;

        for (&commands) |*command| {
            const counter = random.uintLessThan(usize, CounterCount);
            const action = random.uintLessThan(u8, 7);
            if (action < 4) {
                const delta = random.intRangeAtMost(i64, -100, 100);
                command.* = .{
                    .add = .{
                        .counter = counter,
                        .delta = delta,
                    },
                };
                model[counter] += delta;
            } else {
                command.* = .{
                    .get = .{
                        .counter = counter,
                        .sequence = expected_len,
                    },
                };
                expected_values[expected_len] = model[counter];
                expected_counters[expected_len] = counter;
                expected_len += 1;
            }
        }

        var rt = try Runtime.init(testing.allocator, .{
            .worker_count = 1,
            .execution_budget = schedule.execution_budget,
        });
        defer rt.deinit();

        var observed_values: [MaxOps]i64 = undefined;
        var observed_counters: [MaxOps]usize = undefined;
        var seen = [_]bool{false} ** MaxOps;
        var observed_len: usize = 0;
        const collector = try rt.spawn(Actors.Collector{
            .values = &observed_values,
            .counters = &observed_counters,
            .seen = &seen,
            .len = &observed_len,
            .expected_len = expected_len,
        });

        var counters: [CounterCount]Actor(CounterMsg) = undefined;
        for (&counters, 0..) |*counter, index| {
            counter.* = try rt.spawn(Actors.Counter{ .index = index });
        }

        for (commands) |command| {
            switch (command) {
                .add => |add| try counters[add.counter].send(.{ .add = add.delta }),
                .get => |get| {
                    try counters[get.counter].send(.{
                        .get = .{
                            .sequence = get.sequence,
                            .reply_to = collector,
                        },
                    });
                },
            }
        }
        for (counters) |counter| {
            try counter.send(.stop);
        }

        try rt.run();

        try testing.expectEqual(expected_len, observed_len);
        for (0..expected_len) |sequence| {
            try testing.expect(seen[sequence]);
            try testing.expectEqual(expected_values[sequence], observed_values[sequence]);
            try testing.expectEqual(expected_counters[sequence], observed_counters[sequence]);
        }
    }
}
