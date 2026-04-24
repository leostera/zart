const common = @import("common.zig");
const std = common.std;
const Ctx = common.Ctx;
const IoDriver = common.IoDriver;
const IoRequest = common.IoRequest;
const Runtime = common.Runtime;

test "runtime deinit releases spawned actors that never ran" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Actor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            _ = try ctx.recv();
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 1,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    _ = try rt.spawn(Actor{});
}

test "runtime deinit releases actors parked on recv" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Actor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            _ = try ctx.recv();
        }
    };

    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 1,
        .preallocate_stack_slab = false,
    });
    defer rt.deinit();

    _ = try rt.spawn(Actor{});
    try rt.run();
}

test "runtime exposes pending no-poller io before teardown" {
    const testing = std.testing;

    const Driver = struct {
        pending: ?*IoRequest = null,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.pending = request;
        }

        fn complete(self: *Self) void {
            const request = self.pending orelse unreachable;
            self.pending = null;
            request.completeOperate(.{ .file_read_streaming = 0 });
        }
    };

    const IoMsg = union(enum) {
        start,
    };

    const Actor = struct {
        pub const Msg = IoMsg;

        completed: *bool,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            _ = try ctx.recv();
            var buffer: [1]u8 = undefined;
            var buffers = [_][]u8{buffer[0..]};
            _ = try std.Io.operate(ctx.io(), .{
                .file_read_streaming = .{
                    .file = .stdin(),
                    .data = &buffers,
                },
            });
            self.completed.* = true;
        }
    };

    var driver: Driver = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .worker_count = 1,
        .preallocate_stack_slab = false,
        .io = driver.driver(),
    });
    defer rt.deinit();

    var completed = false;
    const actor = try rt.spawn(Actor{ .completed = &completed });
    try actor.send(.start);

    try rt.run();
    try testing.expect(rt.hasPendingIo());
    try testing.expect(!completed);

    driver.complete();
    try rt.run();

    try testing.expect(!rt.hasPendingIo());
    try testing.expect(completed);
}
