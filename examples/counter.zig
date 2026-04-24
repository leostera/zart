const std = @import("std");
const zart = @import("zart");

const Reply = union(enum) {
    value: u64,
};

const CounterMsg = union(enum) {
    inc: u64,
    get: zart.Actor(Reply),
    stop,
};

const Counter = struct {
    pub const Msg = CounterMsg;

    initial: u64 = 0,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
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

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .value => |n| self.slot.* = n,
        }
    }
};

pub fn main(_: std.process.Init) !void {
    var rt = try zart.Runtime.init(std.heap.page_allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const replies = try rt.spawn(Collector{ .slot = &observed });
    const counter = try rt.spawn(Counter{ .initial = 10 });

    try counter.send(.{ .inc = 32 });
    try counter.send(.{ .get = replies });
    try counter.send(.stop);

    try rt.run();

    std.debug.print("counter = {d}\n", .{observed});
}
