const common = @import("common.zig");
const std = common.std;
const Ctx = common.Ctx;
const Runtime = common.Runtime;

test "raw send rejects wrong message type" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const OtherMsg = union(enum) {
        hit,
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

    const stopped = try rt.spawn(StopActor{});
    try testing.expectError(error.WrongMessageType, rt.send(OtherMsg, stopped.any(), .hit));

    try stopped.send(.stop);
    try rt.run();
}

test "actor failure destroys actor and invalidates handle" {
    const testing = std.testing;

    const FailingMsg = union(enum) {
        go,
    };

    const FailingActor = struct {
        pub const Msg = FailingMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .go => return error.Boom,
            }
        }
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    const failing = try rt.spawn(FailingActor{});
    try failing.send(.go);

    try testing.expectError(error.Boom, rt.run());
    try testing.expectError(error.InvalidActor, failing.send(.go));
}

test "completed actors release their registry slot" {
    const testing = std.testing;

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

    const first = try rt.spawn(StopActor{});
    try first.send(.stop);
    try rt.run();

    try testing.expectError(error.InvalidActor, first.send(.stop));

    const second = try rt.spawn(StopActor{});
    try testing.expectEqual(first.any().index, second.any().index);
    try testing.expect(first.any().generation != second.any().generation);

    try second.send(.stop);
    try rt.run();
}

test "failed spawn releases reserved actor id" {
    const testing = std.testing;

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

    var rt = try Runtime.init(testing.allocator, .{ .stack_size = 1 });
    defer rt.deinit();

    try testing.expectError(error.StackTooSmall, rt.spawn(StopActor{}));

    rt.options.stack_size = 64 * 1024;
    const stopped = try rt.spawn(StopActor{});
    try testing.expectEqual(@as(usize, 0), stopped.any().index);

    try stopped.send(.stop);
    try rt.run();
}
