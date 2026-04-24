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
    warmup: usize = 1,
    iterations: usize = 5,
};

const NToNCase = struct {
    actors: usize,
    messages: usize,
};

const FiberBench = struct {
    fn complete(_: ?*anyopaque) anyerror!void {}

    fn yielding(arg: ?*anyopaque) anyerror!void {
        const remaining: *usize = @ptrCast(@alignCast(arg.?));
        while (remaining.* != 0) {
            remaining.* -= 1;
            zart.Fiber.yield();
        }
    }
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

    fn timing(reporter: Reporter, name: []const u8, count: usize, stats: BenchStats) !void {
        const elapsed_s = @as(f64, @floatFromInt(stats.min_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
        const per_second = if (elapsed_s == 0) 0 else @as(u64, @intFromFloat(@as(f64, @floatFromInt(count)) / elapsed_s));
        try reporter.writer.print("  {s}: min=", .{name});
        try printDuration(reporter.writer, @floatFromInt(stats.min_ns));
        try reporter.writer.print(" max=", .{});
        try printDuration(reporter.writer, @floatFromInt(stats.max_ns));
        try reporter.writer.print(" avg=", .{});
        try printDuration(reporter.writer, stats.mean_ns);
        try reporter.writer.print(" mean=", .{});
        try printDuration(reporter.writer, stats.mean_ns);
        try reporter.writer.print(" median=", .{});
        try printDuration(reporter.writer, stats.median_ns);
        try reporter.writer.print(" stddev=", .{});
        try printDuration(reporter.writer, stats.stddev_ns);
        try reporter.writer.print(" ops/s={d} iterations={d}\n", .{ per_second, stats.iterations });
        try reporter.writer.flush();
    }
};

const BenchStats = struct {
    min_ns: i96,
    max_ns: i96,
    mean_ns: f64,
    median_ns: f64,
    stddev_ns: f64,
    iterations: usize,

    fn fromSamples(samples: []i96) BenchStats {
        std.debug.assert(samples.len != 0);

        var min_ns: i96 = samples[0];
        var max_ns: i96 = samples[0];
        var total_ns: i128 = 0;
        for (samples) |sample| {
            min_ns = @min(min_ns, sample);
            max_ns = @max(max_ns, sample);
            total_ns += sample;
        }

        const iterations_f = @as(f64, @floatFromInt(samples.len));
        const mean_ns = @as(f64, @floatFromInt(total_ns)) / iterations_f;

        var variance_ns: f64 = 0;
        for (samples) |sample| {
            const delta = @as(f64, @floatFromInt(sample)) - mean_ns;
            variance_ns += delta * delta;
        }
        variance_ns /= iterations_f;

        std.mem.sort(i96, samples, {}, lessThanI96);
        const median_ns = if (samples.len % 2 == 1)
            @as(f64, @floatFromInt(samples[samples.len / 2]))
        else
            (@as(f64, @floatFromInt(samples[(samples.len / 2) - 1])) +
                @as(f64, @floatFromInt(samples[samples.len / 2]))) / 2.0;

        return .{
            .min_ns = min_ns,
            .max_ns = max_ns,
            .mean_ns = mean_ns,
            .median_ns = median_ns,
            .stddev_ns = std.math.sqrt(variance_ns),
            .iterations = samples.len,
        };
    }
};

fn lessThanI96(_: void, lhs: i96, rhs: i96) bool {
    return lhs < rhs;
}

fn printDuration(writer: *std.Io.Writer, ns: f64) !void {
    const abs_ns = @abs(ns);
    if (abs_ns < 1_000.0) {
        try writer.print("{d:.2}ns", .{ns});
        return;
    }

    const us = ns / 1_000.0;
    if (@abs(us) < 1_000.0) {
        try writer.print("{d:.2}µs", .{us});
        return;
    }

    const ms = us / 1_000.0;
    if (@abs(ms) < 1_000.0) {
        try writer.print("{d:.2}ms", .{ms});
        return;
    }

    try writer.print("{d:.2}s", .{ms / 1_000.0});
}

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
        } else if (std.mem.eql(u8, args[arg_index], "--warmup")) {
            arg_index += 1;
            if (arg_index == args.len) return usage(stdout);
            options.warmup = try std.fmt.parseUnsigned(usize, args[arg_index], 10);
        } else if (std.mem.eql(u8, args[arg_index], "--iterations")) {
            arg_index += 1;
            if (arg_index == args.len) return usage(stdout);
            options.iterations = try std.fmt.parseUnsigned(usize, args[arg_index], 10);
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

    if (options.iterations == 0) return error.InvalidBenchmarkConfig;

    try stdout.print(
        "mode={} stack_size={d} quick={} warmup={d} iterations={d}\n",
        .{
            builtin.mode,
            options.stack_size,
            options.quick,
            options.warmup,
            options.iterations,
        },
    );

    try benchFiber(reporter, options);
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
        \\  --warmup N               unmeasured warmup rounds per benchmark case
        \\  --iterations N           measured iterations per benchmark case
        \\
        \\Examples:
        \\  zig build bench
        \\  zig build bench -- --quick
        \\  zig build bench -- --quick --warmup 3 --iterations 5
        \\  zig build bench -Doptimize=ReleaseFast
        \\
    , .{});
}

fn runMeasured(
    reporter: Reporter,
    options: Options,
    name: []const u8,
    count: usize,
    context: anytype,
    comptime measure: anytype,
) !void {
    for (0..options.warmup) |_| {
        _ = try measure(reporter, options, context);
    }

    const samples = try std.heap.page_allocator.alloc(i96, options.iterations);
    defer std.heap.page_allocator.free(samples);

    for (samples) |*sample| {
        sample.* = try measure(reporter, options, context);
    }

    try reporter.timing(name, count, BenchStats.fromSamples(samples));
}

fn benchFiber(reporter: Reporter, options: Options) !void {
    try reporter.section("fibers");
    const full_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000 };
    const quick_cases = [_]usize{ 1, 10, 100, 1_000, 10_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |fiber_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "fiber init {d}", .{fiber_count});
        defer std.heap.page_allocator.free(name);
        try runMeasured(reporter, options, name, fiber_count, fiber_count, measureFiberInit);
    }

    for (cases) |fiber_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "fiber run {d}", .{fiber_count});
        defer std.heap.page_allocator.free(name);
        try runMeasured(reporter, options, name, fiber_count, fiber_count, measureFiberRun);
    }

    const yield_counts = if (options.quick)
        &[_]usize{ 1, 10, 100, 1_000, 10_000 }
    else
        &[_]usize{ 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000 };

    for (yield_counts) |yield_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "fiber yield/resume {d}", .{yield_count});
        defer std.heap.page_allocator.free(name);
        try runMeasured(reporter, options, name, yield_count, yield_count, measureFiberYield);
    }
}

fn measureFiberInit(reporter: Reporter, options: Options, fiber_count: usize) !i96 {
    const stacks = try std.heap.page_allocator.alignedAlloc(
        u8,
        std.mem.Alignment.fromByteUnits(zart.Fiber.stack_alignment),
        fiber_count * options.stack_size,
    );
    defer std.heap.page_allocator.free(stacks);

    const fibers = try std.heap.page_allocator.alloc(zart.Fiber, fiber_count);
    defer std.heap.page_allocator.free(fibers);

    const start = nowNs(reporter.io);
    for (fibers, 0..) |*fiber, index| {
        const stack = stacks[index * options.stack_size ..][0..options.stack_size];
        fiber.* = try zart.Fiber.init(@alignCast(stack), FiberBench.complete, null);
    }
    const end = nowNs(reporter.io);

    for (fibers) |*fiber| fiber.deinit();

    return end - start;
}

fn measureFiberRun(reporter: Reporter, options: Options, fiber_count: usize) !i96 {
    const stacks = try std.heap.page_allocator.alignedAlloc(
        u8,
        std.mem.Alignment.fromByteUnits(zart.Fiber.stack_alignment),
        fiber_count * options.stack_size,
    );
    defer std.heap.page_allocator.free(stacks);

    const fibers = try std.heap.page_allocator.alloc(zart.Fiber, fiber_count);
    defer std.heap.page_allocator.free(fibers);

    for (fibers, 0..) |*fiber, index| {
        const stack = stacks[index * options.stack_size ..][0..options.stack_size];
        fiber.* = try zart.Fiber.init(@alignCast(stack), FiberBench.complete, null);
    }
    defer {
        for (fibers) |*fiber| fiber.deinit();
    }

    const start = nowNs(reporter.io);
    for (fibers) |*fiber| {
        const status = try fiber.run();
        if (status != .completed) return error.UnexpectedFiberStatus;
    }
    const end = nowNs(reporter.io);

    return end - start;
}

fn measureFiberYield(reporter: Reporter, options: Options, yield_count: usize) !i96 {
    const stack = try std.heap.page_allocator.alignedAlloc(
        u8,
        std.mem.Alignment.fromByteUnits(zart.Fiber.stack_alignment),
        options.stack_size,
    );
    defer std.heap.page_allocator.free(stack);

    var remaining = yield_count;
    var fiber = try zart.Fiber.init(stack, FiberBench.yielding, &remaining);
    defer fiber.deinit();

    const start = nowNs(reporter.io);
    for (0..yield_count) |_| {
        const status = try fiber.run();
        if (status != .suspended) return error.UnexpectedFiberStatus;
    }
    const status = try fiber.run();
    if (status != .completed) return error.UnexpectedFiberStatus;
    const end = nowNs(reporter.io);

    return end - start;
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

        try runMeasured(reporter, options, name, actor_count, actor_count, measureSpawn);
    }
}

fn measureSpawn(reporter: Reporter, options: Options, actor_count: usize) !i96 {
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

    return end - start;
}

fn benchOneToOne(reporter: Reporter, options: Options) !void {
    try reporter.section("send 1->1");
    const full_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000 };
    const quick_cases = [_]usize{ 1, 10, 100, 1_000, 10_000, 100_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |message_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "send 1->1 {d}", .{message_count});
        defer std.heap.page_allocator.free(name);

        try runMeasured(reporter, options, name, message_count, message_count, measureOneToOne);
    }
}

fn measureOneToOne(reporter: Reporter, options: Options, message_count: usize) !i96 {
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

    return end - start;
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

        try runMeasured(reporter, options, name, case.messages, case, measureNToN);
    }
}

fn measureNToN(reporter: Reporter, options: Options, case: NToNCase) !i96 {
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

    return end - start;
}

fn benchIo(reporter: Reporter, options: Options) !void {
    try reporter.section("io");
    const full_cases = [_]usize{ 100_000, 1_000_000 };
    const quick_cases = [_]usize{ 10_000, 100_000 };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |operation_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "synthetic operate {d}", .{operation_count});
        defer std.heap.page_allocator.free(name);

        try runMeasured(reporter, options, name, operation_count, operation_count, measureIo);
    }

    try reporter.skip("read files", "pending concrete non-blocking file driver");
    try reporter.skip("read network", "pending concrete non-blocking network driver");
}

fn measureIo(reporter: Reporter, options: Options, operation_count: usize) !i96 {
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

    return end - start;
}

fn nowNs(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

fn bytesToMib(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(MiB));
}
