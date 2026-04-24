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

    try rt.runSmp();

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

        try rt.runSmp();

        try testing.expectEqualSlices(usize, &expected, &observed);
    }
}
