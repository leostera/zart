const common = @import("common.zig");
const std = common.std;
const Ctx = common.Ctx;
const Runtime = common.Runtime;

test "stress many short-lived actors release registry slots" {
    const testing = std.testing;

    const Count = 512;

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

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var previous = try rt.spawn(StopActor{});
    try previous.send(.stop);

    for (0..Count) |_| {
        try rt.run();
        try testing.expectError(error.InvalidActor, previous.send(.stop));

        const next = try rt.spawn(StopActor{});
        try testing.expectEqual(previous.any().index, next.any().index);
        try testing.expect(previous.any().generation != next.any().generation);
        try next.send(.stop);
        previous = next;
    }

    try rt.run();
}
