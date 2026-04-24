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

    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1 });
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

    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1 });
    defer rt.deinit();

    const failing = try rt.spawn(FailingActor{});
    try failing.send(.go);

    try testing.expectError(error.Boom, rt.run());
    try testing.expectError(error.InvalidActor, failing.send(.go));
}

test "actor failure returns from current run without stopping runtime" {
    const testing = std.testing;

    const FailingMsg = union(enum) {
        go,
    };

    const SurvivorMsg = union(enum) {
        ping,
    };

    const FailingActor = struct {
        pub const Msg = FailingMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            _ = try ctx.recv();
            return error.Boom;
        }
    };

    const Survivor = struct {
        pub const Msg = SurvivorMsg;

        observed: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            _ = try ctx.recv();
            self.observed.* = true;
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 2,
        .execution_budget = 1,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    var observed = false;
    const failing = try rt.spawn(FailingActor{});
    const survivor = try rt.spawn(Survivor{ .observed = &observed });

    try failing.send(.go);
    try testing.expectError(error.Boom, rt.run());
    try testing.expectEqual(Runtime.State.idle, rt.state());
    try testing.expectError(error.InvalidActor, failing.send(.go));

    try survivor.send(.ping);
    try rt.run();

    try testing.expect(observed);
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

    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1 });
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

    var rt = try Runtime.init(testing.allocator, .{ .worker_count = 1, .stack_size = 1 });
    defer rt.deinit();

    try testing.expectError(error.StackTooSmall, rt.spawn(StopActor{}));

    rt.options.stack_size = 64 * 1024;
    const stopped = try rt.spawn(StopActor{});
    try testing.expectEqual(@as(usize, 0), stopped.any().index);

    try stopped.send(.stop);
    try rt.run();
}
