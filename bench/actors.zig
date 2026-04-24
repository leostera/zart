const builtin = @import("builtin");
const std = @import("std");
const zart = @import("zart");

const posix = std.posix;

const KiB = 1024;
const MiB = 1024 * KiB;
const GiB = 1024 * MiB;

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

const FileReadMsg = union(enum) {
    read: struct {
        file: std.Io.File,
        operations: usize,
        total_read: *usize,
    },
};

const FileReadActor = struct {
    pub const Msg = FileReadMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .read => |request| {
                var buffer: [4096]u8 = undefined;
                var buffers = [_][]u8{buffer[0..]};
                var total: usize = 0;
                for (0..request.operations) |_| {
                    total += try request.file.readPositional(ctx.io(), &buffers, 0);
                }
                request.total_read.* = total;
            },
        }
    }
};

const SocketReadMsg = union(enum) {
    read: struct {
        stream: std.Io.net.Stream,
        data: []u8,
    },
};

const SocketReadActor = struct {
    pub const Msg = SocketReadMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .read => |request| {
                var buffer: [4096]u8 = undefined;
                var reader = request.stream.reader(ctx.io(), &buffer);
                try reader.interface.readSliceAll(request.data);
            },
        }
    }
};

const SocketWriteMsg = union(enum) {
    write: struct {
        stream: std.Io.net.Stream,
        data: []const u8,
    },
};

const SocketWriteActor = struct {
    pub const Msg = SocketWriteMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .write => |request| {
                var buffer: [4096]u8 = undefined;
                var writer = request.stream.writer(ctx.io(), &buffer);
                try writer.interface.writeAll(request.data);
                try writer.interface.flush();
            },
        }
    }
};

const SocketThroughputReadMsg = union(enum) {
    read: struct {
        stream: std.Io.net.Stream,
        total_bytes: usize,
        io_buffer: []u8,
        read_buffer: []u8,
        bytes_read: *usize,
    },
};

const SocketThroughputReadActor = struct {
    pub const Msg = SocketThroughputReadMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .read => |request| {
                var reader = request.stream.reader(ctx.io(), request.io_buffer);
                var total: usize = 0;
                while (total < request.total_bytes) {
                    const remaining = request.total_bytes - total;
                    const len = @min(request.read_buffer.len, remaining);
                    try reader.interface.readSliceAll(request.read_buffer[0..len]);
                    total += len;
                }
                request.bytes_read.* = total;
            },
        }
    }
};

const SocketThroughputWriteMsg = union(enum) {
    write: struct {
        stream: std.Io.net.Stream,
        total_bytes: usize,
        chunk: []const u8,
        buffer: []u8,
        bytes_written: *usize,
    },
};

const SocketThroughputWriteActor = struct {
    pub const Msg = SocketThroughputWriteMsg;

    pub fn run(_: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .write => |request| {
                var writer = request.stream.writer(ctx.io(), request.buffer);
                var total: usize = 0;
                while (total < request.total_bytes) {
                    const remaining = request.total_bytes - total;
                    const len = @min(request.chunk.len, remaining);
                    try writer.interface.writeAll(request.chunk[0..len]);
                    total += len;
                }
                try writer.interface.flush();
                request.bytes_written.* = total;
            },
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
            .file_read_positional,
            .file_write_positional,
            .net_read,
            .net_write,
            => unreachable,
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

const SmpCase = struct {
    workers: usize,
    actors: usize,
    messages: usize,
};

const SocketThroughputCase = struct {
    total_bytes: usize,
    chunk_size: usize,
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
        try reporter.printStatsPrefix(name, stats);
        try reporter.writer.print(" ops/s={d} iterations={d}\n", .{ per_second, stats.iterations });
        try reporter.writer.flush();
    }

    fn throughput(reporter: Reporter, name: []const u8, bytes: usize, stats: BenchStats) !void {
        const best_bytes_per_second = bytesPerSecond(bytes, @floatFromInt(stats.min_ns));
        const mean_bytes_per_second = bytesPerSecond(bytes, stats.mean_ns);

        try reporter.printStatsPrefix(name, stats);
        try reporter.writer.print(" best=", .{});
        try printByteRate(reporter.writer, best_bytes_per_second);
        try reporter.writer.print(" mean_rate=", .{});
        try printByteRate(reporter.writer, mean_bytes_per_second);
        try reporter.writer.print(" best_B/s={d} iterations={d}\n", .{
            @as(u64, @intFromFloat(best_bytes_per_second)),
            stats.iterations,
        });
        try reporter.writer.flush();
    }

    fn printStatsPrefix(reporter: Reporter, name: []const u8, stats: BenchStats) !void {
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

fn bytesPerSecond(bytes: usize, ns: f64) f64 {
    if (ns == 0) return 0;
    return @as(f64, @floatFromInt(bytes)) / (ns / @as(f64, @floatFromInt(std.time.ns_per_s)));
}

fn printByteRate(writer: *std.Io.Writer, bytes_per_second: f64) !void {
    const abs_rate = @abs(bytes_per_second);
    if (abs_rate < @as(f64, @floatFromInt(KiB))) {
        try writer.print("{d:.2}B/s", .{bytes_per_second});
        return;
    }

    const kib = bytes_per_second / @as(f64, @floatFromInt(KiB));
    if (@abs(kib) < @as(f64, @floatFromInt(KiB))) {
        try writer.print("{d:.2}KiB/s", .{kib});
        return;
    }

    const mib = bytes_per_second / @as(f64, @floatFromInt(MiB));
    if (@abs(mib) < @as(f64, @floatFromInt(KiB))) {
        try writer.print("{d:.2}MiB/s", .{mib});
        return;
    }

    try writer.print("{d:.2}GiB/s", .{bytes_per_second / @as(f64, @floatFromInt(GiB))});
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
    try benchSocketThroughput(reporter, options);
    try benchSmpScaling(reporter, options);
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

fn runMeasuredThroughput(
    reporter: Reporter,
    options: Options,
    name: []const u8,
    byte_count: usize,
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

    try reporter.throughput(name, byte_count, BenchStats.fromSamples(samples));
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

fn benchSmpScaling(reporter: Reporter, options: Options) !void {
    try reporter.section("smp scaling");
    const full_cases = [_]SmpCase{
        .{ .workers = 1, .actors = 256, .messages = 100_000 },
        .{ .workers = 2, .actors = 256, .messages = 100_000 },
        .{ .workers = 4, .actors = 256, .messages = 100_000 },
        .{ .workers = 8, .actors = 256, .messages = 100_000 },
    };
    const quick_cases = [_]SmpCase{
        .{ .workers = 1, .actors = 64, .messages = 10_000 },
        .{ .workers = 2, .actors = 64, .messages = 10_000 },
        .{ .workers = 4, .actors = 64, .messages = 10_000 },
    };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |case| {
        const name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "smp N->N workers={d} actors={d} messages={d}",
            .{ case.workers, case.actors, case.messages },
        );
        defer std.heap.page_allocator.free(name);

        try runMeasured(reporter, options, name, case.messages, case, measureSmpNToN);
    }
}

fn measureSmpNToN(reporter: Reporter, options: Options, case: SmpCase) !i96 {
    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .stack_size = options.stack_size,
        .worker_count = case.workers,
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

    for (actors[0..actor_count], 0..) |actor_handle, id| {
        const sends = base_sends + if (id < extra_sends) @as(usize, 1) else 0;
        try actor_handle.send(.{
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

    for (actors) |actor_handle| {
        try actor_handle.send(.stop);
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

    const file_cases = if (options.quick)
        &[_]usize{ 100, 1_000 }
    else
        &[_]usize{ 1_000, 10_000 };
    for (file_cases) |operation_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "read file ops {d}", .{operation_count});
        defer std.heap.page_allocator.free(name);

        try runMeasured(reporter, options, name, operation_count, operation_count, measureFileRead);
    }

    const network_cases = if (options.quick)
        &[_]usize{ 1_000, 10_000 }
    else
        &[_]usize{ 10_000, 100_000 };
    for (network_cases) |byte_count| {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "socket read/write bytes {d}", .{byte_count});
        defer std.heap.page_allocator.free(name);

        try runMeasured(reporter, options, name, byte_count, byte_count, measureSocketReadWrite);
    }
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

fn measureFileRead(reporter: Reporter, options: Options, operation_count: usize) !i96 {
    var cache_dir = try std.Io.Dir.cwd().createDirPathOpen(reporter.io, ".zig-cache/tmp", .{});
    defer cache_dir.close(reporter.io);

    var file = try cache_dir.createFile(reporter.io, "zart-actor-bench-read.bin", .{ .read = true });
    defer file.close(reporter.io);

    var contents: [4096]u8 = undefined;
    @memset(&contents, 'z');
    try file.writeStreamingAll(reporter.io, &contents);

    var driver = try zart.io.Default.init();
    defer driver.deinit();

    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .stack_size = options.stack_size,
        .internal_allocator = std.heap.page_allocator,
        .io = driver.driver(),
    });
    defer rt.deinit();

    var total_read: usize = 0;
    const actor = try rt.spawn(FileReadActor{});
    try actor.send(.{ .read = .{
        .file = file,
        .operations = operation_count,
        .total_read = &total_read,
    } });

    const start = nowNs(reporter.io);
    try rt.run();
    const end = nowNs(reporter.io);

    if (total_read != operation_count * contents.len) return error.UnexpectedFileReadCount;

    return end - start;
}

fn measureSocketReadWrite(reporter: Reporter, options: Options, byte_count: usize) !i96 {
    if (!@hasDecl(std.posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]std.posix.fd_t = undefined;
    switch (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);
    try setNonblocking(sockets[0]);
    try setNonblocking(sockets[1]);

    const write_data = try std.heap.page_allocator.alloc(u8, byte_count);
    defer std.heap.page_allocator.free(write_data);
    const read_data = try std.heap.page_allocator.alloc(u8, byte_count);
    defer std.heap.page_allocator.free(read_data);
    @memset(write_data, 'n');

    var driver = try zart.io.Default.init();
    defer driver.deinit();

    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .stack_size = options.stack_size,
        .internal_allocator = std.heap.page_allocator,
        .io = driver.driver(),
    });
    defer rt.deinit();

    const reader = try rt.spawn(SocketReadActor{});
    const writer = try rt.spawn(SocketWriteActor{});

    try reader.send(.{ .read = .{
        .stream = streamFromFd(sockets[0]),
        .data = read_data,
    } });
    try writer.send(.{ .write = .{
        .stream = streamFromFd(sockets[1]),
        .data = write_data,
    } });

    const start = nowNs(reporter.io);
    try rt.run();
    const end = nowNs(reporter.io);

    if (!std.mem.eql(u8, write_data, read_data)) return error.UnexpectedNetworkRead;

    return end - start;
}

fn benchSocketThroughput(reporter: Reporter, options: Options) !void {
    try reporter.section("socket throughput");

    const full_cases = [_]SocketThroughputCase{
        .{ .total_bytes = 16 * MiB, .chunk_size = 4 * KiB },
        .{ .total_bytes = 64 * MiB, .chunk_size = 64 * KiB },
        .{ .total_bytes = 256 * MiB, .chunk_size = 256 * KiB },
    };
    const quick_cases = [_]SocketThroughputCase{
        .{ .total_bytes = 1 * MiB, .chunk_size = 4 * KiB },
        .{ .total_bytes = 8 * MiB, .chunk_size = 64 * KiB },
    };
    const cases = if (options.quick) quick_cases[0..] else full_cases[0..];

    for (cases) |case| {
        const actor_name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "actor ctx.io socket total={d} chunk={d}",
            .{ case.total_bytes, case.chunk_size },
        );
        defer std.heap.page_allocator.free(actor_name);
        try runMeasuredThroughput(reporter, options, actor_name, case.total_bytes, case, measureActorSocketThroughput);

        const raw_name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "raw std.io socket total={d} chunk={d}",
            .{ case.total_bytes, case.chunk_size },
        );
        defer std.heap.page_allocator.free(raw_name);
        try runMeasuredThroughput(reporter, options, raw_name, case.total_bytes, case, measureRawStdIoSocketThroughput);

        const raw_posix_name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "raw posix read/write socket total={d} chunk={d}",
            .{ case.total_bytes, case.chunk_size },
        );
        defer std.heap.page_allocator.free(raw_posix_name);
        try runMeasuredThroughput(reporter, options, raw_posix_name, case.total_bytes, case, measureRawPosixSocketThroughput);

        const raw_vectored_name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "raw posix readv/sendmsg socket total={d} chunk={d}",
            .{ case.total_bytes, case.chunk_size },
        );
        defer std.heap.page_allocator.free(raw_vectored_name);
        try runMeasuredThroughput(reporter, options, raw_vectored_name, case.total_bytes, case, measureRawPosixVectoredSocketThroughput);

        const raw_pump_name = try std.fmt.allocPrint(
            std.heap.page_allocator,
            "raw posix nonblocking pump socket total={d} chunk={d}",
            .{ case.total_bytes, case.chunk_size },
        );
        defer std.heap.page_allocator.free(raw_pump_name);
        try runMeasuredThroughput(reporter, options, raw_pump_name, case.total_bytes, case, measureRawPosixNonblockingPumpSocketThroughput);
    }
}

fn measureActorSocketThroughput(reporter: Reporter, options: Options, case: SocketThroughputCase) !i96 {
    if (!@hasDecl(std.posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]std.posix.fd_t = undefined;
    switch (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);
    try setNonblocking(sockets[0]);
    try setNonblocking(sockets[1]);

    const write_chunk = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_chunk);
    const write_io_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_io_buffer);
    const read_io_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_io_buffer);
    const read_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_buffer);
    @memset(write_chunk, 't');

    var driver = try zart.io.Default.init();
    defer driver.deinit();

    var rt = try zart.Runtime.init(std.heap.page_allocator, .{
        .stack_size = options.stack_size,
        .worker_count = 4,
        .internal_allocator = std.heap.page_allocator,
        .io = driver.driver(),
    });
    defer rt.deinit();

    var bytes_read: usize = 0;
    var bytes_written: usize = 0;
    const reader = try rt.spawn(SocketThroughputReadActor{});
    const writer = try rt.spawn(SocketThroughputWriteActor{});

    try reader.send(.{ .read = .{
        .stream = streamFromFd(sockets[0]),
        .total_bytes = case.total_bytes,
        .io_buffer = read_io_buffer,
        .read_buffer = read_buffer,
        .bytes_read = &bytes_read,
    } });
    try writer.send(.{ .write = .{
        .stream = streamFromFd(sockets[1]),
        .total_bytes = case.total_bytes,
        .chunk = write_chunk,
        .buffer = write_io_buffer,
        .bytes_written = &bytes_written,
    } });

    const start = nowNs(reporter.io);
    try rt.run();
    const end = nowNs(reporter.io);

    if (bytes_read != case.total_bytes) return error.UnexpectedNetworkRead;
    if (bytes_written != case.total_bytes) return error.UnexpectedNetworkWrite;

    return end - start;
}

const RawStdIoSocketWriterArgs = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    total_bytes: usize,
    chunk: []const u8,
    io_buffer: []u8,
    start: *std.atomic.Value(bool),
    bytes_written: usize = 0,
    err: ?anyerror = null,
};

fn rawStdIoSocketWriter(args: *RawStdIoSocketWriterArgs) void {
    rawStdIoSocketWriterRun(args) catch |err| {
        args.err = err;
    };
}

fn rawStdIoSocketWriterRun(args: *RawStdIoSocketWriterArgs) !void {
    while (!args.start.load(.acquire)) std.atomic.spinLoopHint();

    var writer = args.stream.writer(args.io, args.io_buffer);
    var total: usize = 0;
    while (total < args.total_bytes) {
        const remaining = args.total_bytes - total;
        const len = @min(args.chunk.len, remaining);
        try writer.interface.writeAll(args.chunk[0..len]);
        total += len;
    }
    try writer.interface.flush();
    args.bytes_written = total;
}

fn measureRawStdIoSocketThroughput(reporter: Reporter, _: Options, case: SocketThroughputCase) !i96 {
    if (!@hasDecl(std.posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]std.posix.fd_t = undefined;
    switch (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);

    const write_chunk = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_chunk);
    const write_io_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_io_buffer);
    const read_io_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_io_buffer);
    const read_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_buffer);
    @memset(write_chunk, 't');

    var start_gate = std.atomic.Value(bool).init(false);
    var writer_args: RawStdIoSocketWriterArgs = .{
        .io = reporter.io,
        .stream = streamFromFd(sockets[1]),
        .total_bytes = case.total_bytes,
        .chunk = write_chunk,
        .io_buffer = write_io_buffer,
        .start = &start_gate,
    };

    const thread = try std.Thread.spawn(.{}, rawStdIoSocketWriter, .{&writer_args});
    var joined = false;
    defer if (!joined) thread.join();

    var reader = streamFromFd(sockets[0]).reader(reporter.io, read_io_buffer);
    var bytes_read: usize = 0;

    const start = nowNs(reporter.io);
    start_gate.store(true, .release);
    while (bytes_read < case.total_bytes) {
        const remaining = case.total_bytes - bytes_read;
        const len = @min(read_buffer.len, remaining);
        try reader.interface.readSliceAll(read_buffer[0..len]);
        bytes_read += len;
    }
    thread.join();
    joined = true;
    const end = nowNs(reporter.io);

    if (writer_args.err) |err| return err;
    if (bytes_read != case.total_bytes) return error.UnexpectedNetworkRead;
    if (writer_args.bytes_written != case.total_bytes) return error.UnexpectedNetworkWrite;

    return end - start;
}

const RawPosixSocketWriterArgs = struct {
    fd: posix.fd_t,
    total_bytes: usize,
    chunk: []const u8,
    start: *std.atomic.Value(bool),
    bytes_written: usize = 0,
    err: ?anyerror = null,
};

fn rawPosixSocketWriter(args: *RawPosixSocketWriterArgs) void {
    rawPosixSocketWriterRun(args) catch |err| {
        args.err = err;
    };
}

fn rawPosixSocketWriterRun(args: *RawPosixSocketWriterArgs) !void {
    while (!args.start.load(.acquire)) std.atomic.spinLoopHint();

    var total: usize = 0;
    while (total < args.total_bytes) {
        const remaining = args.total_bytes - total;
        const len = @min(args.chunk.len, remaining);
        try rawWriteAll(args.fd, args.chunk[0..len]);
        total += len;
    }
    args.bytes_written = total;
}

fn measureRawPosixSocketThroughput(reporter: Reporter, _: Options, case: SocketThroughputCase) !i96 {
    if (!@hasDecl(posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]posix.fd_t = undefined;
    switch (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);

    const write_chunk = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_chunk);
    const read_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_buffer);
    @memset(write_chunk, 't');

    var start_gate = std.atomic.Value(bool).init(false);
    var writer_args: RawPosixSocketWriterArgs = .{
        .fd = sockets[1],
        .total_bytes = case.total_bytes,
        .chunk = write_chunk,
        .start = &start_gate,
    };

    const thread = try std.Thread.spawn(.{}, rawPosixSocketWriter, .{&writer_args});
    var joined = false;
    defer if (!joined) thread.join();

    var bytes_read: usize = 0;
    const start = nowNs(reporter.io);
    start_gate.store(true, .release);
    while (bytes_read < case.total_bytes) {
        const remaining = case.total_bytes - bytes_read;
        const len = @min(read_buffer.len, remaining);
        try rawReadAll(sockets[0], read_buffer[0..len]);
        bytes_read += len;
    }
    thread.join();
    joined = true;
    const end = nowNs(reporter.io);

    if (writer_args.err) |err| return err;
    if (bytes_read != case.total_bytes) return error.UnexpectedNetworkRead;
    if (writer_args.bytes_written != case.total_bytes) return error.UnexpectedNetworkWrite;

    return end - start;
}

const RawPosixVectoredSocketWriterArgs = struct {
    fd: posix.fd_t,
    total_bytes: usize,
    chunk: []const u8,
    start: *std.atomic.Value(bool),
    bytes_written: usize = 0,
    err: ?anyerror = null,
};

fn rawPosixVectoredSocketWriter(args: *RawPosixVectoredSocketWriterArgs) void {
    rawPosixVectoredSocketWriterRun(args) catch |err| {
        args.err = err;
    };
}

fn rawPosixVectoredSocketWriterRun(args: *RawPosixVectoredSocketWriterArgs) !void {
    while (!args.start.load(.acquire)) std.atomic.spinLoopHint();

    var total: usize = 0;
    while (total < args.total_bytes) {
        const remaining = args.total_bytes - total;
        const len = @min(args.chunk.len, remaining);
        try rawSendmsgAll(args.fd, args.chunk[0..len]);
        total += len;
    }
    args.bytes_written = total;
}

fn measureRawPosixVectoredSocketThroughput(reporter: Reporter, _: Options, case: SocketThroughputCase) !i96 {
    if (!@hasDecl(posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]posix.fd_t = undefined;
    switch (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);

    const write_chunk = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_chunk);
    const read_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_buffer);
    @memset(write_chunk, 't');

    var start_gate = std.atomic.Value(bool).init(false);
    var writer_args: RawPosixVectoredSocketWriterArgs = .{
        .fd = sockets[1],
        .total_bytes = case.total_bytes,
        .chunk = write_chunk,
        .start = &start_gate,
    };

    const thread = try std.Thread.spawn(.{}, rawPosixVectoredSocketWriter, .{&writer_args});
    var joined = false;
    defer if (!joined) thread.join();

    var bytes_read: usize = 0;
    const start = nowNs(reporter.io);
    start_gate.store(true, .release);
    while (bytes_read < case.total_bytes) {
        const remaining = case.total_bytes - bytes_read;
        const len = @min(read_buffer.len, remaining);
        try rawReadvAll(sockets[0], read_buffer[0..len]);
        bytes_read += len;
    }
    thread.join();
    joined = true;
    const end = nowNs(reporter.io);

    if (writer_args.err) |err| return err;
    if (bytes_read != case.total_bytes) return error.UnexpectedNetworkRead;
    if (writer_args.bytes_written != case.total_bytes) return error.UnexpectedNetworkWrite;

    return end - start;
}

fn measureRawPosixNonblockingPumpSocketThroughput(reporter: Reporter, _: Options, case: SocketThroughputCase) !i96 {
    if (!@hasDecl(posix.system, "socketpair")) return error.UnsupportedBenchmark;

    var sockets: [2]posix.fd_t = undefined;
    switch (posix.errno(posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &sockets))) {
        .SUCCESS => {},
        else => return error.UnsupportedBenchmark,
    }
    defer closeFd(sockets[0]);
    defer closeFd(sockets[1]);
    try setNonblocking(sockets[0]);
    try setNonblocking(sockets[1]);

    const write_chunk = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(write_chunk);
    const read_buffer = try std.heap.page_allocator.alloc(u8, case.chunk_size);
    defer std.heap.page_allocator.free(read_buffer);
    @memset(write_chunk, 't');

    var bytes_written: usize = 0;
    var bytes_read: usize = 0;

    const start = nowNs(reporter.io);
    while (bytes_read < case.total_bytes) {
        var progressed = false;

        if (bytes_written < case.total_bytes) {
            const remaining = case.total_bytes - bytes_written;
            const len = @min(write_chunk.len, remaining);
            const written = rawWriteNonblocking(sockets[1], write_chunk[0..len]) catch |err| switch (err) {
                error.WouldBlock => 0,
                else => |e| return e,
            };
            bytes_written += written;
            progressed = progressed or written != 0;
        }

        if (bytes_read < case.total_bytes) {
            const remaining = case.total_bytes - bytes_read;
            const len = @min(read_buffer.len, remaining);
            const read = rawReadNonblocking(sockets[0], read_buffer[0..len]) catch |err| switch (err) {
                error.WouldBlock => 0,
                else => |e| return e,
            };
            bytes_read += read;
            progressed = progressed or read != 0;
        }

        if (!progressed) try pollSocketPair(sockets);
    }
    const end = nowNs(reporter.io);

    if (bytes_written != case.total_bytes) return error.UnexpectedNetworkWrite;
    if (bytes_read != case.total_bytes) return error.UnexpectedNetworkRead;

    return end - start;
}

fn rawReadAll(fd: posix.fd_t, buffer: []u8) !void {
    var offset: usize = 0;
    while (offset < buffer.len) {
        const n = try rawRead(fd, buffer[offset..]);
        if (n == 0) return error.UnexpectedEndOfStream;
        offset += n;
    }
}

fn rawWriteAll(fd: posix.fd_t, buffer: []const u8) !void {
    var offset: usize = 0;
    while (offset < buffer.len) {
        const n = try rawWrite(fd, buffer[offset..]);
        if (n == 0) return error.UnexpectedEndOfStream;
        offset += n;
    }
}

fn rawReadvAll(fd: posix.fd_t, buffer: []u8) !void {
    var offset: usize = 0;
    while (offset < buffer.len) {
        const n = try rawReadv(fd, buffer[offset..]);
        if (n == 0) return error.UnexpectedEndOfStream;
        offset += n;
    }
}

fn rawSendmsgAll(fd: posix.fd_t, buffer: []const u8) !void {
    var offset: usize = 0;
    while (offset < buffer.len) {
        const n = try rawSendmsg(fd, buffer[offset..]);
        if (n == 0) return error.UnexpectedEndOfStream;
        offset += n;
    }
}

fn rawRead(fd: posix.fd_t, buffer: []u8) !usize {
    while (true) {
        const rc = posix.system.read(fd, buffer.ptr, buffer.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn rawWrite(fd: posix.fd_t, buffer: []const u8) !usize {
    while (true) {
        const rc = posix.system.write(fd, buffer.ptr, buffer.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn rawReadNonblocking(fd: posix.fd_t, buffer: []u8) !usize {
    const rc = posix.system.read(fd, buffer.ptr, buffer.len);
    return switch (posix.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INTR => rawReadNonblocking(fd, buffer),
        .AGAIN => error.WouldBlock,
        else => error.Unexpected,
    };
}

fn rawWriteNonblocking(fd: posix.fd_t, buffer: []const u8) !usize {
    const rc = posix.system.write(fd, buffer.ptr, buffer.len);
    return switch (posix.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INTR => rawWriteNonblocking(fd, buffer),
        .AGAIN => error.WouldBlock,
        else => error.Unexpected,
    };
}

fn rawReadv(fd: posix.fd_t, buffer: []u8) !usize {
    var iovec: [1]posix.iovec = .{.{
        .base = buffer.ptr,
        .len = buffer.len,
    }};

    while (true) {
        const rc = posix.system.readv(fd, &iovec, iovec.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn rawSendmsg(fd: posix.fd_t, buffer: []const u8) !usize {
    var iovec: [1]posix.iovec_const = .{.{
        .base = buffer.ptr,
        .len = buffer.len,
    }};
    var msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iovec,
        .iovlen = iovec.len,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };

    while (true) {
        const rc = posix.system.sendmsg(fd, &msg, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn pollSocketPair(sockets: [2]posix.fd_t) !void {
    var pollfds = [_]posix.pollfd{
        .{
            .fd = sockets[0],
            .events = @intCast(posix.POLL.IN),
            .revents = 0,
        },
        .{
            .fd = sockets[1],
            .events = @intCast(posix.POLL.OUT),
            .revents = 0,
        },
    };
    _ = try posix.poll(&pollfds, -1);
}

fn streamFromFd(fd: std.posix.fd_t) std.Io.net.Stream {
    return .{
        .socket = .{
            .handle = fd,
            .address = .{ .ip4 = .loopback(0) },
        },
    };
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

fn nowNs(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

fn bytesToMib(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(MiB));
}
