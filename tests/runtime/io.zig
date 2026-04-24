const common = @import("common.zig");
const std = common.std;
const Ctx = common.Ctx;
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
