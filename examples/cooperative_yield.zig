const std = @import("std");
const zart = @import("zart");

const ResultMsg = union(enum) {
    sum: u64,
};

const SumMsg = union(enum) {
    start: struct {
        limit: u64,
        reply_to: zart.Actor(ResultMsg),
    },
};

const Summation = struct {
    pub const Msg = SumMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .start => |job| {
                var sum: u64 = 0;
                var n: u64 = 0;
                while (n <= job.limit) : (n += 1) {
                    sum += n;
                    if (n % 1024 == 0) ctx.yield();
                }
                try job.reply_to.send(.{ .sum = sum });
            },
        }
    }
};

const Collector = struct {
    pub const Msg = ResultMsg;

    slot: *u64,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .sum => |sum| self.slot.* = sum,
        }
    }
};

pub fn main(_: std.process.Init) !void {
    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .execution_budget = 1,
    });
    defer rt.deinit();

    var sum: u64 = 0;
    const collector = try rt.spawn(Collector{ .slot = &sum });
    const worker = try rt.spawn(Summation{});

    try worker.send(.{ .start = .{
        .limit = 100_000,
        .reply_to = collector,
    } });
    try rt.run();

    std.debug.print("sum 0..100000 = {d}\n", .{sum});
}
