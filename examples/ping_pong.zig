const std = @import("std");
const zart = @import("zart");

const DoneMsg = union(enum) {
    done,
};

const PingMsg = union(enum) {
    pong: struct {
        remaining: u32,
        pong: zart.Actor(PongMsg),
        done: zart.Actor(DoneMsg),
    },
    stop,
};

const PongMsg = union(enum) {
    ping: struct {
        remaining: u32,
        ping: zart.Actor(PingMsg),
        done: zart.Actor(DoneMsg),
    },
    stop,
};

const Ping = struct {
    pub const Msg = PingMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .pong => |msg| {
                    if (msg.remaining == 0) {
                        try msg.done.send(.done);
                    } else {
                        try msg.pong.send(.{ .ping = .{
                            .remaining = msg.remaining - 1,
                            .ping = ctx.self(),
                            .done = msg.done,
                        } });
                    }
                },
                .stop => return,
            }
        }
    }
};

const Pong = struct {
    pub const Msg = PongMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .ping => |msg| {
                    if (msg.remaining == 0) {
                        try msg.done.send(.done);
                    } else {
                        try msg.ping.send(.{ .pong = .{
                            .remaining = msg.remaining - 1,
                            .pong = ctx.self(),
                            .done = msg.done,
                        } });
                    }
                },
                .stop => return,
            }
        }
    }
};

const Done = struct {
    pub const Msg = DoneMsg;

    observed: *bool,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        _ = try ctx.recv();
        self.observed.* = true;
    }
};

pub fn main(_: std.process.Init) !void {
    var rt = try zart.Runtime.init(std.heap.page_allocator, .{});
    defer rt.deinit();

    var observed = false;
    const done = try rt.spawn(Done{ .observed = &observed });
    const ping = try rt.spawn(Ping{});
    const pong = try rt.spawn(Pong{});

    try pong.send(.{ .ping = .{
        .remaining = 10,
        .ping = ping,
        .done = done,
    } });
    try rt.run();

    try ping.send(.stop);
    try pong.send(.stop);
    try rt.run();

    std.debug.print("ping-pong completed = {}\n", .{observed});
}
