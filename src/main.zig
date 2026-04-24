const std = @import("std");
const zart = @import("zart");

const Reply = union(enum) {
    value: u64,
};

const CounterMsg = union(enum) {
    inc: u64,
    get: zart.Mailbox(Reply),
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

fn printReply(ctx: *zart.Ctx(Reply)) !void {
    switch (try ctx.recv()) {
        .value => |n| std.debug.print("counter = {}\n", .{n}),
    }
}

pub fn main(init: std.process.Init) !void {
    _ = init;

    var rt = zart.Runtime.init(std.heap.page_allocator, .{});
    defer rt.deinit();

    const replies = try rt.spawn(printReply);
    const counter = try rt.spawn(Counter{ .initial = 10 });

    try counter.send(.{ .inc = 32 });
    try counter.send(.{ .get = replies });
    try counter.send(.stop);

    try rt.run();
}
