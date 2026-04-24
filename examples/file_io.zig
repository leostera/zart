const std = @import("std");
const zart = @import("zart");

const ResultMsg = union(enum) {
    bytes: usize,
};

const ReadMsg = union(enum) {
    read: struct {
        file: std.Io.File,
        reply_to: zart.Actor(ResultMsg),
    },
};

const Reader = struct {
    pub const Msg = ReadMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .read => |job| {
                var buffer: [128]u8 = undefined;
                var buffers = [_][]u8{buffer[0..]};
                const n = try job.file.readPositional(ctx.io(), &buffers, 0);
                try job.reply_to.send(.{ .bytes = n });
            },
        }
    }
};

const Collector = struct {
    pub const Msg = ResultMsg;

    slot: *usize,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .bytes => |n| self.slot.* = n,
        }
    }
};

pub fn main(init: std.process.Init) !void {
    var cache_dir = try std.Io.Dir.cwd().createDirPathOpen(init.io, ".zig-cache/tmp", .{});
    defer cache_dir.close(init.io);

    var file = try cache_dir.createFile(init.io, "zart-example-file-io.txt", .{ .read = true });
    defer file.close(init.io);
    try file.writeStreamingAll(init.io, "hello from zart actors\n");

    var actor_io = zart.io.Default.init();
    defer actor_io.deinit();

    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .io = actor_io.driver(),
    });
    defer rt.deinit();

    var bytes: usize = 0;
    const collector = try rt.spawn(Collector{ .slot = &bytes });
    const reader = try rt.spawn(Reader{});

    try reader.send(.{ .read = .{
        .file = file,
        .reply_to = collector,
    } });
    try rt.run();

    std.debug.print("read bytes = {d}\n", .{bytes});
}
