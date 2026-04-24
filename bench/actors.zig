const builtin = @import("builtin");
const std = @import("std");
const zart = @import("zart");

const KiB = 1024;
const MiB = 1024 * KiB;

const StopMsg = union(enum) {
    stop,
};

const StopActor = struct {
    pub const Msg = StopMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .stop => return,
        }
    }
};

const PingMsg = union(enum) {
    ping,
    stop,
};

const PingActor = struct {
    pub const Msg = PingMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .ping => {},
                .stop => return,
            }
        }
    }
};

const NToNMsg = union(enum) {
    start: struct {
        id: usize,
        peers: []const zart.Actor(NToNMsg),
        sends: usize,
    },
    ping,
    stop,
};

const NToNActor = struct {
    pub const Msg = NToNMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .start => |start| {
                    for (0..start.sends) |offset| {
                        const peer = start.peers[(start.id + offset + 1) % start.peers.len];
                        try peer.send(.ping);
                    }
                },
                .ping => {},
                .stop => return,
            }
        }
    }
};

const IoBenchMsg = union(enum) {
    operate: usize,
    stop,
};

const IoBenchActor = struct {
    pub const Msg = IoBenchMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        while (true) {
            switch (try ctx.recv()) {
                .operate => |count| {
                    var buffer: [1]u8 = undefined;
                    var buffers = [_][]u8{buffer[0..]};
                    for (0..count) |_| {
                        const result = try std.Io.operate(ctx.io(), .{
                            .file_read_streaming = .{
                                .file = .stdin(),
                                .data = &buffers,
                            },
                        });
                        switch (result) {
                            .file_read_streaming => |read_result| _ = try read_result,
                            else => unreachable,
                        }
                    }
                },
                .stop => return,
            }
        }
    }
};

const ImmediateIoDriver = struct {
    completed: usize = 0,

    const Self = @This();

    fn driver(self: *Self) zart.IoDriver {
        return .{
            .context = self,
            .submit_fn = submit,
        };
    }

    fn submit(context: ?*anyopaque, request: *zart.IoRequest) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.completed += 1;
        switch (request.payload) {
            .operate => request.completeOperate(.{ .file_read_streaming = 0 }),
            .sleep => request.completeSleep({}),
            .batch_await_async => request.completeBatchAwaitAsync({}),
            .batch_await_concurrent => request.completeBatchAwaitConcurrent({}),
        }
    }
};

const Options = struct {
    stack_size: usize = zart.Fiber.minimum_stack_size,
    quick: bool = false,
};

const NToNCase = struct {
    actors: usize,
    messages: usize,
};

const Reporter = struct {
    io: std.Io,
    writer: *std.Io.Writer,

    fn section(reporter: Reporter, name: []const u8) !void {
        try reporter.writer.print("\n{s}\n", .{name});
    }

    fn skip(reporter: Reporter, name: []const u8, reason: []const u8) !void {
        try reporter.writer.print("  {s}: skipped ({s})\n", .{ name, reason });
        try reporter.writer.flush();
    }

    fn timing(reporter: Reporter, name: []const u8, count: usize, elapsed_ns: i96) !void {
        const elapsed_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_us));
        const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
        const per_second = if (elapsed_s == 0) 0 else @as(u64, @intFromFloat(@as(f64, @floatFromInt(count)) / elapsed_s));
        try reporter.writer.print("  {s}: {d:.2}us {d}/s\n", .{ name, elapsed_us, per_second });
        try reporter.writer.flush();
    }
};

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    var options: Options = .{};
    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        if (std.mem.eql(u8, args[arg_index], "--stack-size")) {
            arg_index += 1;
            if (arg_index == args.len) return usage(stdout);
            options.stack_size = try std.fmt.parseUnsigned(usize, args[arg_index], 10);
        } else if (std.mem.eql(u8, args[arg_index], "--quick")) {
            options.quick = true;
        } else if (std.mem.eql(u8, args[arg_index], "--help")) {
            return usage(stdout);
        } else {
            return usage(stdout);
        }
    }

    const reporter: Reporter = .{
        .io = init.io,
        .writer = stdout,
    };

    try stdout.print(
        "mode={} stack_size={d} quick={}\n",
        .{
            builtin.mode,
            options.stack_size,
            options.quick,
        },
    );

    try benchSpawn(reporter, options);
    try benchOneToOne(reporter, options);
    try benchNToN(reporter, options);
    try benchIo(reporter, options);
}

fn usage(stdout: *std.Io.Writer) !void {
    try stdout.print(
        \\usage: zig build bench -- [options]
        \\
        \\Options:
        \\  --stack-size BYTES       actor fiber stack size
        \\  --quick                  run reduced counts for local smoke checks
        \\
        \\Examples:
        \\  zig build bench
        \\  zig build bench -- --quick
        \\  zig build bench -Doptimize=ReleaseFast
        \\
    , .{});
}

fn benchSpawn(reporter: Reporter, options: Options) !void {
    try reporter.section("spawn actors");
    const full_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000 };
    const quick_cases = [_]usize{ 1, 10, 100, 1_000, 10_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |actor_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "spawn {d}", .{actor_count});
        defer std.heap.page_allocator.free(name);

        try reporter.writer.print(
            "  {s}: running (live_stack={d:.2}MiB)\n",
            .{ name, bytesToMib(actor_count * options.stack_size) },
        );
        try reporter.writer.flush();

        var rt = try zart.Runtime.init(std.heap.page_allocator, .{
            .stack_size = options.stack_size,
            .internal_allocator = std.heap.page_allocator,
        });
        defer rt.deinit();

        const handles = try std.heap.page_allocator.alloc(zart.Actor(StopMsg), actor_count);
        defer std.heap.page_allocator.free(handles);

        const start = nowNs(reporter.io);
        for (handles) |*handle| {
            handle.* = try rt.spawn(StopActor{});
        }
        const end = nowNs(reporter.io);

        for (handles) |handle| try handle.send(.stop);
        try rt.run();

        try reporter.timing(name, actor_count, end - start);
    }
}

fn benchOneToOne(reporter: Reporter, options: Options) !void {
    try reporter.section("send 1->1");
    const full_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000 };
    const quick_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |message_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "send 1->1 {d}", .{message_count});
        defer std.heap.page_allocator.free(name);

        var rt = try zart.Runtime.init(std.heap.page_allocator, .{
            .stack_size = options.stack_size,
            .internal_allocator = std.heap.page_allocator,
        });
        defer rt.deinit();

        const receiver = try rt.spawn(PingActor{});

        const start = nowNs(reporter.io);
        for (0..message_count) |_| {
            try receiver.send(.ping);
        }
        const end = nowNs(reporter.io);

        try receiver.send(.stop);
        try rt.run();

        try reporter.timing(name, message_count, end - start);
    }
}

fn benchNToN(reporter: Reporter, options: Options) !void {
    try reporter.section("send N->N");
    const full_cases = [_]NToNCase{
        .{ .actors = 1, .messages = 1 },
        .{ .actors = 4, .messages = 10 },
        .{ .actors = 16, .messages = 100 },
        .{ .actors = 64, .messages = 1_000 },
        .{ .actors = 256, .messages = 10_000 },
        .{ .actors = 1024, .messages = 100_000 },
    };
    const quick_cases = [_]NToNCase{
        .{ .actors = 1, .messages = 1 },
        .{ .actors = 4, .messages = 10 },
        .{ .actors = 16, .messages = 100 },
        .{ .actors = 16, .messages = 1_000 },
        .{ .actors = 64, .messages = 10_000 },
    };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |case| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "send N->N {d}", .{case.messages});
        defer std.heap.page_allocator.free(name);

        var rt = try zart.Runtime.init(std.heap.page_allocator, .{
            .stack_size = options.stack_size,
            .internal_allocator = std.heap.page_allocator,
        });
        defer rt.deinit();

        const actors = try std.heap.page_allocator.alloc(zart.Actor(NToNMsg), case.actors);
        defer std.heap.page_allocator.free(actors);

        for (actors) |*actor| {
            actor.* = try rt.spawn(NToNActor{});
        }

        const base_sends = case.messages / case.actors;
        const extra_sends = case.messages % case.actors;
        const actor_count = if (base_sends == 0) extra_sends else case.actors;

        for (actors[0..actor_count], 0..) |actor, id| {
            const sends = base_sends + if (id < extra_sends) @as(usize, 1) else 0;
            try actor.send(.{
                .start = .{
                    .id = id,
                    .peers = actors,
                    .sends = sends,
                },
            });
        }

        const start = nowNs(reporter.io);
        try rt.run();
        const end = nowNs(reporter.io);

        for (actors) |actor| {
            try actor.send(.stop);
        }
        try rt.run();

        try reporter.timing(name, case.messages, end - start);
    }
}

fn benchIo(reporter: Reporter, options: Options) !void {
    try reporter.section("io");
    const full_cases = [_]usize{ 100_000, 1_000_000 };
    const quick_cases = [_]usize{ 10_000, 100_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |operation_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "synthetic operate {d}", .{operation_count});
        defer std.heap.page_allocator.free(name);

        var driver: ImmediateIoDriver = .{};
        var rt = try zart.Runtime.init(std.heap.page_allocator, .{
            .stack_size = options.stack_size,
            .internal_allocator = std.heap.page_allocator,
            .io = driver.driver(),
        });
        defer rt.deinit();

        const actor = try rt.spawn(IoBenchActor{});
        try actor.send(.{ .operate = operation_count });

        const start = nowNs(reporter.io);
        try rt.run();
        const end = nowNs(reporter.io);

        if (driver.completed != operation_count) return error.UnexpectedIoCompletionCount;

        try actor.send(.stop);
        try rt.run();

        try reporter.timing(name, operation_count, end - start);
    }

    try reporter.skip("read files", "pending concrete non-blocking file driver");
    try reporter.skip("read network", "pending concrete non-blocking network driver");
}

fn nowNs(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

fn bytesToMib(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(MiB));
}
