const std = @import("std");
const zart = @import("zart");

const ResultMsg = union(enum) {
    value: u64,
};

const WorkerMsg = union(enum) {
    work: struct {
        input: u64,
        reply_to: zart.Actor(ResultMsg),
    },
    stop,
};

const Worker = struct {
    pub const Msg = WorkerMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .work => |job| try job.reply_to.send(.{ .value = job.input * job.input }),
                .stop => return,
            }
        }
    }
};

const Collector = struct {
    pub const Msg = ResultMsg;

    expected: usize,
    seen: *usize,
    total: *u64,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (self.seen.* < self.expected) {
            switch (try ctx.recv()) {
                .value => |n| {
                    self.total.* += n;
                    self.seen.* += 1;
                },
            }
        }
    }
};

pub fn main(_: std.process.Init) !void {
    const WorkerCount = 8;

    var rt = try zart.Runtime.init(std.heap.page_allocator, .{});
    defer rt.deinit();

    var seen: usize = 0;
    var total: u64 = 0;
    const collector = try rt.spawn(Collector{
        .expected = WorkerCount,
        .seen = &seen,
        .total = &total,
    });

    var workers: [WorkerCount]zart.Actor(WorkerMsg) = undefined;
    for (&workers) |*worker| {
        worker.* = try rt.spawn(Worker{});
    }

    for (workers, 1..) |worker, input| {
        try worker.send(.{ .work = .{
            .input = @intCast(input),
            .reply_to = collector,
        } });
    }
    try rt.run();

    for (workers) |worker| try worker.send(.stop);
    try rt.run();

    std.debug.print("sum of squares = {d}\n", .{total});
}
