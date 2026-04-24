const common = @import("common.zig");
const std = common.std;
const Actor = common.Actor;
const Ctx = common.Ctx;
const Runtime = common.Runtime;
const zart = common.zart;

test "smp run completes many actors across workers" {
    const testing = std.testing;

    const ActorCount = 128;

    const RunMsg = union(enum) {
        run,
    };

    const Worker = struct {
        pub const Msg = RunMsg;

        completed: *std.atomic.Value(usize),

        pub fn run(self: *@This(), ctx: *Ctx(RunMsg)) !void {
            _ = try ctx.recv();
            _ = self.completed.fetchAdd(1, .acq_rel);
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 4,
        .execution_budget = 1,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    var completed = std.atomic.Value(usize).init(0);
    for (0..ActorCount) |_| {
        const worker = try rt.spawn(Worker{ .completed = &completed });
        try worker.send(.run);
    }

    try rt.run();

    try testing.expectEqual(@as(usize, ActorCount), completed.load(.acquire));
}

test "smp randomized counter routing matches reference counts" {
    const testing = std.testing;

    const CounterCount = 4;
    const CommandCount = 128;

    const CounterMsg = union(enum) {
        inc,
        stop,
    };

    const Counter = struct {
        pub const Msg = CounterMsg;

        observed: *usize,

        pub fn run(self: *@This(), ctx: *Ctx(CounterMsg)) !void {
            while (true) {
                switch (try ctx.recv()) {
                    .inc => self.observed.* += 1,
                    .stop => return,
                }
            }
        }
    };

    const interleaver = zart.testing.Interleaver.init(.{
        .min_execution_budget = 1,
        .max_execution_budget = 3,
        .min_io_budget = 1,
        .max_io_budget = 2,
    });

    for (zart.testing.seeds[0..4]) |seed| {
        const case = interleaver.case(seed);

        var rt = try Runtime.init(testing.allocator, .{
            .worker_count = 4,
            .execution_budget = case.execution_budget,
            .io_budget = case.io_budget,
            .preallocate_stack_slab = false,
        });
        defer rt.deinit();

        var observed = [_]usize{0} ** CounterCount;
        var expected = [_]usize{0} ** CounterCount;
        var counters: [CounterCount]Actor(CounterMsg) = undefined;
        for (&counters, 0..) |*counter, index| {
            counter.* = try rt.spawn(Counter{ .observed = &observed[index] });
        }

        var value = seed;
        for (0..CommandCount) |_| {
            value = value *% 6364136223846793005 +% 1442695040888963407;
            const target: usize = @intCast(value % CounterCount);
            expected[target] += 1;
            try counters[target].send(.inc);
        }
        for (counters) |counter| try counter.send(.stop);

        try rt.run();

        try testing.expectEqualSlices(usize, &expected, &observed);
    }
}

test "smp fuzzed actor graph message cascades match reference counts" {
    const testing = std.testing;

    const ActorCount = 8;
    const CommandCount = 96;
    const MaxDepth = 4;
    const MaxExpected = CommandCount * (MaxDepth + 1);

    const GraphMsg = union(enum) {
        hit: struct {
            origin: usize,
            remaining: usize,
        },
        stop,
    };

    const Pending = struct {
        target: usize,
        origin: usize,
        remaining: usize,
    };

    const GraphActor = struct {
        pub const Msg = GraphMsg;

        index: usize,
        peers: []const Actor(GraphMsg),
        observed: []std.atomic.Value(usize),
        seed: u64,

        pub fn run(self: *@This(), ctx: *Ctx(GraphMsg)) !void {
            while (true) {
                switch (try ctx.recv()) {
                    .hit => |hit| {
                        _ = self.observed[self.index].fetchAdd(1, .acq_rel);
                        if (hit.remaining != 0) {
                            const next = nextTarget(self.seed, self.index, hit.origin, hit.remaining);
                            try self.peers[next].send(.{ .hit = .{
                                .origin = hit.origin,
                                .remaining = hit.remaining - 1,
                            } });
                        }
                    },
                    .stop => return,
                }
                ctx.yield();
            }
        }
    };

    const interleaver = zart.testing.Interleaver.init(.{
        .min_execution_budget = 1,
        .max_execution_budget = 3,
        .min_io_budget = 1,
        .max_io_budget = 1,
    });

    for (zart.testing.seeds[0..4]) |seed| {
        var recorder = zart.testing.TraceRecorder(4096).init(seed);
        const case = interleaver.case(seed);
        var rt = try Runtime.init(testing.allocator, .{
            .worker_count = 4,
            .execution_budget = case.execution_budget,
            .io_budget = case.io_budget,
            .preallocate_stack_slab = false,
            .tracer = recorder.tracer(),
        });
        defer rt.deinit();

        var actors: [ActorCount]Actor(GraphMsg) = undefined;
        var observed: [ActorCount]std.atomic.Value(usize) = undefined;
        var expected = [_]usize{0} ** ActorCount;
        for (&observed) |*count| count.* = .init(0);

        for (&actors, 0..) |*handle, index| {
            handle.* = try rt.spawn(GraphActor{
                .index = index,
                .peers = &actors,
                .observed = &observed,
                .seed = seed,
            });
        }

        var pending: [MaxExpected]Pending = undefined;
        var pending_head: usize = 0;
        var pending_len: usize = 0;
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();

        for (0..CommandCount) |command_index| {
            const target = random.uintLessThan(usize, ActorCount);
            const remaining = random.uintLessThan(usize, MaxDepth + 1);
            pending[pending_len] = .{
                .target = target,
                .origin = command_index,
                .remaining = remaining,
            };
            pending_len += 1;
            try actors[target].send(.{ .hit = .{
                .origin = command_index,
                .remaining = remaining,
            } });
        }

        while (pending_head < pending_len) : (pending_head += 1) {
            const item = pending[pending_head];
            expected[item.target] += 1;
            if (item.remaining != 0) {
                const next = nextTarget(seed, item.target, item.origin, item.remaining);
                pending[pending_len] = .{
                    .target = next,
                    .origin = item.origin,
                    .remaining = item.remaining - 1,
                };
                pending_len += 1;
            }
        }

        try rt.run();

        for (expected, 0..) |expected_count, index| {
            const actual = observed[index].load(.acquire);
            if (actual != expected_count) {
                std.debug.print("actor graph mismatch seed=0x{x} actor={d} expected={d} actual={d}\n", .{
                    seed,
                    index,
                    expected_count,
                    actual,
                });
                return error.ActorGraphMismatch;
            }
        }

        for (actors) |actor_handle| try actor_handle.send(.stop);
        try rt.run();

        try testing.expect(recorder.len != 0);
    }
}

fn nextTarget(seed: u64, current: usize, origin: usize, remaining: usize) usize {
    const mixed = seed ^ (@as(u64, @intCast(current)) *% 0x9e37_79b9) ^ (@as(u64, @intCast(origin)) *% 0x85eb_ca6b) ^ @as(u64, @intCast(remaining));
    return @intCast(mixed % 8);
}
