const std = @import("std");
const zart = @import("zart");

const ServeMsg = union(enum) {
    serve,
};

const Options = struct {
    port: u16 = 8080,
    max_requests: ?usize = null,
    help: bool = false,
};

const Connection = struct {
    pub const Msg = ServeMsg;

    stream: std.Io.net.Stream,
    request_index: usize,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .serve => try self.serve(ctx),
        }
    }

    fn serve(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        defer self.stream.close(std.Options.debug_io);

        const started_at = std.Io.Timestamp.now(std.Options.debug_io, .real);
        const started = std.Io.Timestamp.now(std.Options.debug_io, .awake);

        var request_storage: [4096]u8 = undefined;
        const request = try readHttpRequest(self.stream, ctx.io(), &request_storage);
        const request_line = parseRequestLine(request);

        var body_storage: [512]u8 = undefined;
        const body = try std.fmt.bufPrint(&body_storage,
            \\hello from zart
            \\connection actor: {d}
            \\request: {s}
            \\
        , .{ self.request_index, request_line.raw });

        var writer_storage: [1024]u8 = undefined;
        var writer = self.stream.writer(ctx.io(), &writer_storage);
        try writer.interface.print(
            "HTTP/1.1 200 OK\r\n" ++
                "content-type: text/plain; charset=utf-8\r\n" ++
                "content-length: {d}\r\n" ++
                "connection: close\r\n" ++
                "\r\n",
            .{body.len},
        );
        try writer.interface.writeAll(body);
        try writer.interface.flush();

        const finished = std.Io.Timestamp.now(std.Options.debug_io, .awake);
        logRequest(started_at, request_line, started.durationTo(finished));
    }
};

pub fn main(init: std.process.Init) !void {
    const options = try parseArgs(init);
    if (options.help) {
        printUsage();
        return;
    }

    var actor_io = try zart.io.Default.init();
    defer actor_io.deinit();

    var rt = try zart.Runtime.init(init.gpa, .{
        .io = actor_io.driver(),
    });
    defer rt.deinit();

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", options.port);
    var server = try address.listen(init.io, .{
        .reuse_address = true,
    });
    defer server.deinit(init.io);

    std.debug.print("listening on http://127.0.0.1:{d}\n", .{options.port});
    std.debug.print("try: curl http://127.0.0.1:{d}/hello\n", .{options.port});

    var accepted: usize = 0;
    while (options.max_requests == null or accepted < options.max_requests.?) {
        const stream = try server.accept(init.io);
        errdefer stream.close(init.io);

        try setNonblocking(stream.socket.handle);

        accepted += 1;
        const connection = try rt.spawn(Connection{
            .stream = stream,
            .request_index = accepted,
        });
        try connection.send(.serve);
        try rt.run();
    }
}

fn parseArgs(init: std.process.Init) !Options {
    var options: Options = .{};
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            options.help = true;
            return options;
        } else if (std.mem.eql(u8, arg, "--port")) {
            const value = args.next() orelse return error.MissingPort;
            options.port = try std.fmt.parseInt(u16, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-requests")) {
            const value = args.next() orelse return error.MissingMaxRequests;
            options.max_requests = try std.fmt.parseInt(usize, value, 10);
        } else {
            printUsage();
            return error.UnknownArgument;
        }
    }

    return options;
}

fn printUsage() void {
    std.debug.print(
        \\usage: zig build example-http_server -- [--port N] [--max-requests N]
        \\
        \\examples:
        \\  zig build example-http_server
        \\  zig build example-http_server -- --port 8081 --max-requests 1
        \\
    , .{});
}

fn readHttpRequest(stream: std.Io.net.Stream, io: std.Io, storage: []u8) ![]const u8 {
    var reader_storage: [1024]u8 = undefined;
    var reader = stream.reader(io, &reader_storage);

    var used: usize = 0;
    while (used < storage.len) {
        const limit = @min(storage.len - used, 512);
        var buffers = [_][]u8{storage[used..][0..limit]};
        const n = reader.interface.readVec(&buffers) catch |err| switch (err) {
            error.EndOfStream => break,
            else => |e| return e,
        };
        used += n;
        if (std.mem.indexOf(u8, storage[0..used], "\r\n\r\n") != null) break;
        if (n == 0) break;
    }

    return storage[0..used];
}

const RequestLine = struct {
    raw: []const u8,
    method: []const u8,
    path: []const u8,
};

fn parseRequestLine(request: []const u8) RequestLine {
    const end = std.mem.indexOf(u8, request, "\r\n") orelse request.len;
    const raw = request[0..end];

    var fields = std.mem.tokenizeScalar(u8, raw, ' ');
    return .{
        .raw = raw,
        .method = fields.next() orelse "-",
        .path = fields.next() orelse "-",
    };
}

fn logRequest(started_at: std.Io.Timestamp, request_line: RequestLine, elapsed: std.Io.Duration) void {
    var timestamp_buffer: [32]u8 = undefined;
    const timestamp = formatTimestamp(&timestamp_buffer, started_at) catch "unknown-time";

    var duration_buffer: [32]u8 = undefined;
    const duration = formatDuration(&duration_buffer, elapsed) catch "unknown-duration";

    std.debug.print("{s} {s} {s:<32} {s}\n", .{
        timestamp,
        request_line.method,
        request_line.path,
        duration,
    });
}

fn formatTimestamp(buffer: []u8, timestamp: std.Io.Timestamp) ![]const u8 {
    const nanoseconds = timestamp.toNanoseconds();
    if (nanoseconds < 0) return std.fmt.bufPrint(buffer, "{d}ns", .{nanoseconds});

    const total_seconds: u64 = @intCast(@divTrunc(nanoseconds, std.time.ns_per_s));
    const millisecond: u16 = @intCast(@divTrunc(@mod(nanoseconds, std.time.ns_per_s), std.time.ns_per_ms));

    const epoch_seconds: std.time.epoch.EpochSeconds = .{ .secs = total_seconds };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();

    return std.fmt.bufPrint(buffer, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
        millisecond,
    });
}

fn formatDuration(buffer: []u8, duration: std.Io.Duration) ![]const u8 {
    const ns = duration.toNanoseconds();
    const abs_ns = @abs(ns);
    if (abs_ns < std.time.ns_per_us) {
        return std.fmt.bufPrint(buffer, "{d}ns", .{ns});
    }

    const ns_float: f64 = @floatFromInt(ns);
    if (abs_ns < std.time.ns_per_ms) {
        return std.fmt.bufPrint(buffer, "{d:.2}us", .{ns_float / std.time.ns_per_us});
    }
    if (abs_ns < std.time.ns_per_s) {
        return std.fmt.bufPrint(buffer, "{d:.2}ms", .{ns_float / std.time.ns_per_ms});
    }

    return std.fmt.bufPrint(buffer, "{d:.2}s", .{ns_float / std.time.ns_per_s});
}

fn setNonblocking(fd: std.Io.net.Socket.Handle) !void {
    var flags: usize = while (true) {
        const rc = std.posix.system.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
        switch (std.posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    };
    flags |= @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
    while (true) {
        switch (std.posix.errno(std.posix.system.fcntl(fd, std.posix.F.SETFL, flags))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}
