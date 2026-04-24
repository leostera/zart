const common = @import("common.zig");
const std = common.std;
const Ctx = common.Ctx;
const DefaultIo = common.DefaultIo;
const IoDriver = common.IoDriver;
const IoRequest = common.IoRequest;
const Runtime = common.Runtime;
const zart = common.zart;

test "ctx io preserves std Io signature and delegates to runtime io" {
    const testing = std.testing;

    const FakeDriver = struct {
        operate_calls: usize = 0,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.operate_calls += 1;
            request.completeOperate(.{ .file_read_streaming = 1 });
        }
    };

    const WorkerMsg = union(enum) {
        run,
    };

    const Actors = struct {
        fn readOne(io: std.Io, out: *u8) void {
            var buffer: [1]u8 = undefined;
            var buffers = [_][]u8{buffer[0..]};
            const result = std.Io.operate(io, .{
                .file_read_streaming = .{
                    .file = .stdin(),
                    .data = &buffers,
                },
            }) catch unreachable;
            switch (result) {
                .file_read_streaming => |read_result| out.* = @intCast(read_result catch unreachable),
                else => unreachable,
            }
        }

        const Worker = struct {
            pub const Msg = WorkerMsg;

            out: *u8,

            pub fn run(self: *@This(), ctx: *Ctx(WorkerMsg)) !void {
                switch (try ctx.recv()) {
                    .run => readOne(ctx.io(), self.out),
                }
            }
        };
    };

    var fake_driver: FakeDriver = .{};
    var rt = try Runtime.init(testing.allocator, .{ .io = fake_driver.driver() });
    defer rt.deinit();

    var observed: u8 = 0;
    const worker = try rt.spawn(Actors.Worker{ .out = &observed });

    try worker.send(.run);
    try rt.run();

    try testing.expectEqual(@as(u8, 1), observed);
    try testing.expectEqual(@as(usize, 1), fake_driver.operate_calls);
}

test "std Io calls consume io budget and yield cooperatively" {
    const testing = std.testing;

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const WorkerMsg = union(enum) {
        start,
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var buffer: [1]u8 = undefined;

                        self.trace.push('a');
                        std.Io.random(ctx.io(), &buffer);
                        self.trace.push('b');
                        std.Io.random(ctx.io(), &buffer);
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('x'),
                }
            }
        };
    };

    var rt = try Runtime.init(testing.allocator, .{
        .io_budget = 1,
    });
    defer rt.deinit();

    var trace: Trace = .{};
    const worker = try rt.spawn(Actors.Worker{ .trace = &trace });
    const other = try rt.spawn(Actors.Other{ .trace = &trace });

    try worker.send(.start);
    try other.send(.hit);
    try rt.run();

    try testing.expectEqualStrings("axbc", trace.slice());
}

test "std Io operate parks actor until non-blocking driver completion" {
    const testing = std.testing;

    const FakeDriver = struct {
        operate_calls: usize = 0,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.operate_calls += 1;
            request.completeOperate(.{ .file_read_streaming = 7 });
        }
    };

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const WorkerMsg = union(enum) {
        start,
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var buffer: [1]u8 = undefined;
                        var buffers = [_][]u8{buffer[0..]};

                        self.trace.push('a');
                        const result = try std.Io.operate(ctx.io(), .{
                            .file_read_streaming = .{
                                .file = .stdin(),
                                .data = &buffers,
                            },
                        });
                        switch (result) {
                            .file_read_streaming => |read_result| {
                                try testing.expectEqual(@as(usize, 7), try read_result);
                            },
                            else => unreachable,
                        }
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('b'),
                }
            }
        };
    };

    var fake_driver: FakeDriver = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .io = fake_driver.driver(),
    });
    defer rt.deinit();

    var trace: Trace = .{};
    const worker = try rt.spawn(Actors.Worker{ .trace = &trace });
    const other = try rt.spawn(Actors.Other{ .trace = &trace });

    try worker.send(.start);
    try other.send(.hit);
    try rt.run();

    try testing.expectEqualStrings("abc", trace.slice());
    try testing.expectEqual(@as(usize, 1), fake_driver.operate_calls);
}

test "pending non-blocking io can complete after run returns" {
    const testing = std.testing;

    const FakeDriver = struct {
        pending: ?*IoRequest = null,
        submit_calls: usize = 0,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            self.submit_calls += 1;
            self.pending = request;
        }

        fn complete(self: *Self, value: usize) void {
            const request = self.pending orelse unreachable;
            self.pending = null;
            request.completeOperate(.{ .file_read_streaming = value });
        }
    };

    const Trace = struct {
        items: [4]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const WorkerMsg = union(enum) {
        start,
    };

    const Worker = struct {
        pub const Msg = WorkerMsg;

        trace: *Trace,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .start => {
                    var buffer: [1]u8 = undefined;
                    var buffers = [_][]u8{buffer[0..]};

                    self.trace.push('a');
                    const result = try std.Io.operate(ctx.io(), .{
                        .file_read_streaming = .{
                            .file = .stdin(),
                            .data = &buffers,
                        },
                    });
                    switch (result) {
                        .file_read_streaming => |read_result| {
                            try testing.expectEqual(@as(usize, 3), try read_result);
                        },
                        else => unreachable,
                    }
                    self.trace.push('b');
                },
            }
        }
    };

    var fake_driver: FakeDriver = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .io = fake_driver.driver(),
    });
    defer rt.deinit();

    var trace: Trace = .{};
    const worker = try rt.spawn(Worker{ .trace = &trace });

    try worker.send(.start);
    try rt.run();

    try testing.expectEqualStrings("a", trace.slice());
    try testing.expectEqual(@as(usize, 1), fake_driver.submit_calls);
    try testing.expect(fake_driver.pending != null);

    fake_driver.complete(3);
    try rt.run();

    try testing.expectEqualStrings("ab", trace.slice());
    try testing.expectEqual(@as(?*IoRequest, null), fake_driver.pending);
}

test "std Io batch waits park actor until non-blocking driver completion" {
    const testing = std.testing;

    const FakeDriver = struct {
        async_calls: usize = 0,
        concurrent_calls: usize = 0,

        const Self = @This();

        fn driver(self: *Self) IoDriver {
            return .{
                .context = self,
                .submit_fn = submit,
            };
        }

        fn submit(context: ?*anyopaque, request: *IoRequest) void {
            const self: *Self = @ptrCast(@alignCast(context.?));
            switch (request.payload) {
                .batch_await_async => {
                    self.async_calls += 1;
                    request.completeBatchAwaitAsync({});
                },
                .batch_await_concurrent => {
                    self.concurrent_calls += 1;
                    request.completeBatchAwaitConcurrent({});
                },
                else => unreachable,
            }
        }
    };

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(self: *@This(), item: u8) void {
            self.items[self.len] = item;
            self.len += 1;
        }

        fn slice(self: *const @This()) []const u8 {
            return self.items[0..self.len];
        }
    };

    const WorkerMsg = union(enum) {
        start,
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var async_storage: [1]std.Io.Operation.Storage = undefined;
                        var async_batch: std.Io.Batch = .init(&async_storage);
                        var concurrent_storage: [1]std.Io.Operation.Storage = undefined;
                        var concurrent_batch: std.Io.Batch = .init(&concurrent_storage);

                        self.trace.push('a');
                        try async_batch.awaitAsync(ctx.io());
                        self.trace.push('c');
                        try concurrent_batch.awaitConcurrent(ctx.io(), .none);
                        self.trace.push('d');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('b'),
                }
            }
        };
    };

    var fake_driver: FakeDriver = .{};
    var rt = try Runtime.init(testing.allocator, .{
        .io = fake_driver.driver(),
    });
    defer rt.deinit();

    var trace: Trace = .{};
    const worker = try rt.spawn(Actors.Worker{ .trace = &trace });
    const other = try rt.spawn(Actors.Other{ .trace = &trace });

    try worker.send(.start);
    try other.send(.hit);
    try rt.run();

    try testing.expectEqualStrings("abcd", trace.slice());
    try testing.expectEqual(@as(usize, 1), fake_driver.async_calls);
    try testing.expectEqual(@as(usize, 1), fake_driver.concurrent_calls);
}

test "posix io driver reads files through ctx io" {
    const testing = std.testing;

    const FileReadMsg = union(enum) {
        read: std.Io.File,
    };

    const Reader = struct {
        pub const Msg = FileReadMsg;

        out: *[5]u8,
        len: *usize,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .read => |file| {
                    var buffers = [_][]u8{self.out[0..]};
                    self.len.* = try file.readPositional(ctx.io(), &buffers, 0);
                },
            }
        }
    };

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile(testing.io, "input.txt", .{ .read = true });
    defer file.close(testing.io);
    try file.writeStreamingAll(testing.io, "hello");

    var posix_io = DefaultIo.init();
    defer posix_io.deinit();

    var rt = try Runtime.init(testing.allocator, .{ .io = posix_io.driver() });
    defer rt.deinit();

    var out: [5]u8 = undefined;
    var len: usize = 0;
    const reader = try rt.spawn(Reader{ .out = &out, .len = &len });

    try reader.send(.{ .read = file });
    try rt.run();

    try testing.expectEqualStrings("hello", out[0..len]);
}

test "posix io driver writes files through ctx io" {
    const testing = std.testing;

    const FileWriteMsg = union(enum) {
        write: std.Io.File,
    };

    const Writer = struct {
        pub const Msg = FileWriteMsg;

        pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .write => |file| try file.writePositionalAll(ctx.io(), "hello", 0),
            }
        }
    };

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile(testing.io, "output.txt", .{ .read = true });
    defer file.close(testing.io);

    var posix_io = DefaultIo.init();
    defer posix_io.deinit();

    var rt = try Runtime.init(testing.allocator, .{ .io = posix_io.driver() });
    defer rt.deinit();

    const writer = try rt.spawn(Writer{});
    try writer.send(.{ .write = file });
    try rt.run();

    var out: [5]u8 = undefined;
    var buffers = [_][]u8{out[0..]};
    const len = try file.readPositional(testing.io, &buffers, 0);
    try testing.expectEqualStrings("hello", out[0..len]);
}

test "posix io driver parks socket reads until another actor writes" {
    const testing = std.testing;

    if (!@hasDecl(std.posix.system, "socketpair")) return error.SkipZigTest;

    var sockets: [2]std.posix.fd_t = undefined;
    switch (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.SkipZigTest,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);
    try setNonblocking(sockets[0]);
    try setNonblocking(sockets[1]);

    const ReadMsg = union(enum) {
        start,
    };
    const WriteMsg = union(enum) {
        start,
    };

    const Actors = struct {
        const Reader = struct {
            pub const Msg = ReadMsg;

            stream: std.Io.net.Stream,
            out: *[4]u8,
            len: *usize,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var buffer: [8]u8 = undefined;
                        var reader = self.stream.reader(ctx.io(), &buffer);
                        self.len.* = try reader.interface.readSliceShort(self.out[0..]);
                    },
                }
            }
        };

        const Writer = struct {
            pub const Msg = WriteMsg;

            stream: std.Io.net.Stream,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var buffer: [8]u8 = undefined;
                        var writer = self.stream.writer(ctx.io(), &buffer);
                        try writer.interface.writeAll("pong");
                        try writer.interface.flush();
                    },
                }
            }
        };
    };

    var posix_io = DefaultIo.init();
    defer posix_io.deinit();

    var rt = try Runtime.init(testing.allocator, .{ .io = posix_io.driver() });
    defer rt.deinit();

    var out: [4]u8 = undefined;
    var len: usize = 0;
    const reader = try rt.spawn(Actors.Reader{
        .stream = streamFromFd(sockets[0]),
        .out = &out,
        .len = &len,
    });
    const writer = try rt.spawn(Actors.Writer{
        .stream = streamFromFd(sockets[1]),
    });

    try reader.send(.start);
    try writer.send(.start);
    try rt.run();

    try testing.expectEqualStrings("pong", out[0..len]);
}

test "posix io driver parks socket writes until peer reads" {
    const testing = std.testing;

    if (!@hasDecl(std.posix.system, "socketpair")) return error.SkipZigTest;

    var sockets: [2]std.posix.fd_t = undefined;
    switch (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.SkipZigTest,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);
    try setNonblocking(sockets[0]);
    try setNonblocking(sockets[1]);

    const filled = try fillSocketUntilWouldBlock(sockets[1]);
    try testing.expect(filled > 0);

    const ReadMsg = union(enum) {
        start,
    };
    const WriteMsg = union(enum) {
        start,
    };

    const Actors = struct {
        const Reader = struct {
            pub const Msg = ReadMsg;

            stream: std.Io.net.Stream,
            expected_bytes: usize,
            saw_marker: *bool,
            read_bytes: *usize,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var reader_buffer: [512]u8 = undefined;
                        var out: [4096]u8 = undefined;
                        var reader = self.stream.reader(ctx.io(), &reader_buffer);

                        while (self.read_bytes.* < self.expected_bytes) {
                            const remaining = self.expected_bytes - self.read_bytes.*;
                            const len = try reader.interface.readSliceShort(out[0..@min(out.len, remaining)]);
                            for (out[0..len]) |byte| {
                                if (byte == 'z') self.saw_marker.* = true;
                            }
                            self.read_bytes.* += len;
                        }
                    },
                }
            }
        };

        const Writer = struct {
            pub const Msg = WriteMsg;

            stream: std.Io.net.Stream,
            done: *bool,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => {
                        var buffer: [0]u8 = .{};
                        var writer = self.stream.writer(ctx.io(), &buffer);
                        try writer.interface.writeAll("z");
                        try writer.interface.flush();
                        self.done.* = true;
                    },
                }
            }
        };
    };

    var posix_io = DefaultIo.init();
    defer posix_io.deinit();

    var rt = try Runtime.init(testing.allocator, .{ .io = posix_io.driver() });
    defer rt.deinit();

    var saw_marker = false;
    var read_bytes: usize = 0;
    var writer_done = false;

    const writer = try rt.spawn(Actors.Writer{
        .stream = streamFromFd(sockets[1]),
        .done = &writer_done,
    });
    const reader = try rt.spawn(Actors.Reader{
        .stream = streamFromFd(sockets[0]),
        .expected_bytes = filled + 1,
        .saw_marker = &saw_marker,
        .read_bytes = &read_bytes,
    });

    try writer.send(.start);
    try reader.send(.start);
    try rt.run();

    try testing.expect(writer_done);
    try testing.expect(saw_marker);
    try testing.expectEqual(filled + 1, read_bytes);
}

fn streamFromFd(fd: std.posix.fd_t) std.Io.net.Stream {
    return .{
        .socket = .{
            .handle = fd,
            .address = .{ .ip4 = .loopback(0) },
        },
    };
}

fn fillSocketUntilWouldBlock(fd: std.posix.fd_t) !usize {
    const max_fill = 16 * 1024 * 1024;
    var buffer: [4096]u8 = undefined;
    @memset(&buffer, 'x');

    var total: usize = 0;
    while (total < max_fill) {
        const rc = std.posix.system.write(fd, &buffer, buffer.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const written: usize = @intCast(rc);
                if (written == 0) return error.SkipZigTest;
                total += written;
            },
            .INTR => continue,
            .AGAIN => return total,
            else => return error.SkipZigTest,
        }
    }

    return error.SkipZigTest;
}

fn closeFd(fd: std.posix.fd_t) void {
    _ = std.posix.system.close(fd);
}

fn setNonblocking(fd: std.posix.fd_t) !void {
    var flags: usize = while (true) {
        const rc = std.posix.system.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
        switch (std.posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    };
    flags |= @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
    while (true) {
        switch (std.posix.errno(std.posix.system.fcntl(fd, std.posix.F.SETFL, flags))) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}
