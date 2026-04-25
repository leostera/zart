//! POSIX non-blocking actor I/O driver.
//!
//! This driver retries operations that return `WouldBlock` when `poll(2)`
//! reports the underlying file descriptor as ready. It is useful for sockets,
//! pipes, terminals, and non-blocking file descriptors. Regular filesystem
//! reads may still complete synchronously on platforms where regular files do
//! not provide readiness-based async disk I/O.

const std = @import("std");
const RuntimeIo = @import("../runtime/io.zig").RuntimeIo;

const posix = std.posix;
const net = std.Io.net;
const File = std.Io.File;

const Interest = enum {
    read,
    write,
};

pub const Posix = struct {
    mutex: std.Io.Mutex = .init,
    pending_head: ?*RuntimeIo.Request = null,
    pending_tail: ?*RuntimeIo.Request = null,
    pending_count: usize = 0,
    wake_read: posix.fd_t,
    wake_write: posix.fd_t,

    const Self = @This();

    pub fn init() !Self {
        const wake_pipe = try createWakePipe();
        return .{
            .wake_read = wake_pipe[0],
            .wake_write = wake_pipe[1],
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.pending_count != 0 or self.pending_head != null or self.pending_tail != null) {
            @panic("Posix I/O driver deinit called with pending requests");
        }
        closeFd(self.wake_read);
        closeFd(self.wake_write);
        self.* = undefined;
    }

    pub fn driver(self: *Self) RuntimeIo.Driver {
        return .{
            .context = self,
            .submit_fn = submit,
            .poll_fn = poll,
            .wake_fn = wake,
        };
    }

    fn submit(context: ?*anyopaque, request: *RuntimeIo.Request) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        if (self.tryComplete(request)) return;

        self.mutex.lockUncancelable(std.Options.debug_io);
        if (std.debug.runtime_safety) self.assertNotPending(request);
        self.enqueue(request);
        self.mutex.unlock(std.Options.debug_io);
        self.wakePoller();
    }

    fn poll(context: ?*anyopaque, mode: RuntimeIo.PollMode) !void {
        const self: *Self = @ptrCast(@alignCast(context.?));

        var pollfds_buffer: [256]posix.pollfd = undefined;
        while (true) {
            self.mutex.lockUncancelable(std.Options.debug_io);
            const pollfds = self.fillPollfds(&pollfds_buffer);
            self.mutex.unlock(std.Options.debug_io);
            if (pollfds.len <= 1) return;

            const timeout: i32 = switch (mode) {
                .nonblocking => 0,
                .wait => -1,
            };
            const ready = try posix.poll(pollfds, timeout);
            if (ready == 0) return;
            if (pollfds[0].revents != 0) self.drainWakePipe();

            self.mutex.lockUncancelable(std.Options.debug_io);
            const completed_any = self.retryReady(pollfds[1..]);
            const empty = self.pending_head == null;
            self.mutex.unlock(std.Options.debug_io);
            if (mode == .nonblocking or completed_any or empty) return;
        }
    }

    fn wake(context: ?*anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.wakePoller();
    }

    fn enqueue(self: *Self, request: *RuntimeIo.Request) void {
        request.next = null;
        if (self.pending_tail) |tail| {
            tail.next = request;
        } else {
            self.pending_head = request;
        }
        self.pending_tail = request;
        self.pending_count += 1;
    }

    fn fillPollfds(self: *Self, buffer: []posix.pollfd) []posix.pollfd {
        std.debug.assert(buffer.len != 0);

        var count: usize = 1;
        buffer[0] = .{
            .fd = self.wake_read,
            .events = pollEvents(.read),
            .revents = 0,
        };

        var node = self.pending_head;
        while (node) |request| : (node = request.next) {
            if (count == buffer.len) break;
            buffer[count] = .{
                .fd = fdOf(request),
                .events = pollEvents(interestOf(request)),
                .revents = 0,
            };
            count += 1;
        }
        return buffer[0..count];
    }

    fn retryReady(self: *Self, pollfds: []const posix.pollfd) bool {
        var previous: ?*RuntimeIo.Request = null;
        var current = self.pending_head;
        var index: usize = 0;
        var completed_any = false;

        while (current) |request| {
            const next = request.next;
            const ready = if (index < pollfds.len)
                pollfds[index].revents != 0
            else
                false;
            index += 1;

            if (ready and self.tryComplete(request)) {
                if (previous) |prev| {
                    prev.next = next;
                } else {
                    self.pending_head = next;
                }
                if (self.pending_tail == request) self.pending_tail = previous;
                request.next = null;
                self.pending_count -= 1;
                completed_any = true;
            } else {
                previous = request;
            }

            current = next;
        }

        if (std.debug.runtime_safety) self.assertPendingCountConsistent();
        return completed_any;
    }

    fn assertNotPending(self: *const Self, request: *const RuntimeIo.Request) void {
        var node = self.pending_head;
        while (node) |current| : (node = current.next) {
            if (current == request) @panic("POSIX I/O request submitted while already pending");
        }
    }

    fn assertPendingCountConsistent(self: *const Self) void {
        var count: usize = 0;
        var node = self.pending_head;
        var last: ?*RuntimeIo.Request = null;
        while (node) |current| : (node = current.next) {
            count += 1;
            last = current;
        }
        if (count != self.pending_count or last != self.pending_tail) {
            @panic("POSIX I/O pending queue corrupted");
        }
    }

    fn wakePoller(self: *Self) void {
        var byte: [1]u8 = .{1};
        while (true) {
            const rc = posix.system.write(self.wake_write, &byte, byte.len);
            switch (posix.errno(rc)) {
                .SUCCESS, .AGAIN => return,
                .INTR => continue,
                else => return,
            }
        }
    }

    fn drainWakePipe(self: *Self) void {
        var buffer: [64]u8 = undefined;
        while (true) {
            const rc = posix.system.read(self.wake_read, &buffer, buffer.len);
            switch (posix.errno(rc)) {
                .SUCCESS => if (rc == 0) return,
                .AGAIN => return,
                .INTR => continue,
                else => return,
            }
        }
    }

    fn tryComplete(_: *Self, request: *RuntimeIo.Request) bool {
        switch (request.payload) {
            .operate => |operation| switch (operation.operation) {
                .file_read_streaming => |op| {
                    const result = fileReadStreaming(op.file, op.data) catch |err| switch (err) {
                        error.WouldBlock => return false,
                        error.Canceled => {
                            request.completeOperate(error.Canceled);
                            return true;
                        },
                        else => |e| {
                            request.completeOperate(.{ .file_read_streaming = e });
                            return true;
                        },
                    };
                    request.completeOperate(.{ .file_read_streaming = result });
                    return true;
                },
                .file_write_streaming => |op| {
                    const result = fileWriteStreaming(op.file, op.header, op.data, op.splat) catch |err| switch (err) {
                        error.WouldBlock => return false,
                        error.Canceled => {
                            request.completeOperate(error.Canceled);
                            return true;
                        },
                        else => |e| {
                            request.completeOperate(.{ .file_write_streaming = e });
                            return true;
                        },
                    };
                    request.completeOperate(.{ .file_write_streaming = result });
                    return true;
                },
                .net_receive => {
                    request.completeOperate(error.Canceled);
                    return true;
                },
                .device_io_control => {
                    request.completeOperate(error.Canceled);
                    return true;
                },
            },
            .file_read_positional => |op| {
                const result = fileReadPositional(op.file, op.data, op.offset) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => |e| {
                        request.completeFileReadPositional(e);
                        return true;
                    },
                };
                request.completeFileReadPositional(result);
                return true;
            },
            .file_write_positional => |op| {
                const result = fileWritePositional(op.file, op.header, op.data, op.splat, op.offset) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => |e| {
                        request.completeFileWritePositional(e);
                        return true;
                    },
                };
                request.completeFileWritePositional(result);
                return true;
            },
            .net_accept => |op| {
                const result = netAccept(op.server, op.options) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => |e| {
                        request.completeNetAccept(e);
                        return true;
                    },
                };
                request.completeNetAccept(result);
                return true;
            },
            .net_read => |op| {
                const result = netRead(op.handle, op.data) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => |e| {
                        request.completeNetRead(e);
                        return true;
                    },
                };
                request.completeNetRead(result);
                return true;
            },
            .net_write => |op| {
                const result = netWrite(op.handle, op.header, op.data, op.splat) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    else => |e| {
                        request.completeNetWrite(e);
                        return true;
                    },
                };
                request.completeNetWrite(result);
                return true;
            },
            .sleep => {
                request.completeSleep({});
                return true;
            },
            .batch_await_async => {
                request.completeBatchAwaitAsync({});
                return true;
            },
            .batch_await_concurrent => {
                request.completeBatchAwaitConcurrent({});
                return true;
            },
        }
    }
};

fn createWakePipe() ![2]posix.fd_t {
    var fds: [2]posix.fd_t = undefined;
    switch (posix.errno(posix.system.pipe(&fds))) {
        .SUCCESS => {},
        .NFILE => return error.SystemFdQuotaExceeded,
        .MFILE => return error.ProcessFdQuotaExceeded,
        else => |err| return posix.unexpectedErrno(err),
    }
    errdefer {
        closeFd(fds[0]);
        closeFd(fds[1]);
    }

    try setNonblocking(fds[0]);
    try setNonblocking(fds[1]);
    return fds;
}

fn setNonblocking(fd: posix.fd_t) !void {
    var flags: usize = while (true) {
        const rc = posix.system.fcntl(fd, posix.F.GETFL, @as(usize, 0));
        switch (posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => |err| return posix.unexpectedErrno(err),
        }
    };
    flags |= @as(usize, 1 << @bitOffsetOf(posix.O, "NONBLOCK"));
    while (true) {
        switch (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, flags))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn closeFd(fd: posix.fd_t) void {
    _ = posix.system.close(fd);
}

fn fdOf(request: *const RuntimeIo.Request) posix.fd_t {
    return switch (request.payload) {
        .operate => |op| switch (op.operation) {
            .file_read_streaming => |file_op| file_op.file.handle,
            .file_write_streaming => |file_op| file_op.file.handle,
            .net_receive => |net_op| net_op.socket_handle,
            .device_io_control => |dev_op| dev_op.file.handle,
        },
        .file_read_positional => |op| op.file.handle,
        .file_write_positional => |op| op.file.handle,
        .net_accept => |op| op.server,
        .net_read => |op| op.handle,
        .net_write => |op| op.handle,
        .sleep, .batch_await_async, .batch_await_concurrent => -1,
    };
}

fn interestOf(request: *const RuntimeIo.Request) Interest {
    return switch (request.payload) {
        .operate => |op| switch (op.operation) {
            .file_read_streaming, .net_receive => .read,
            .file_write_streaming => .write,
            .device_io_control => .read,
        },
        .file_read_positional, .net_accept, .net_read => .read,
        .file_write_positional, .net_write => .write,
        .sleep, .batch_await_async, .batch_await_concurrent => .read,
    };
}

fn pollEvents(interest: Interest) i16 {
    return switch (interest) {
        .read => @intCast(posix.POLL.IN),
        .write => @intCast(posix.POLL.OUT),
    };
}

fn fileReadStreaming(file: File, data: []const []u8) File.ReadStreamingError!usize {
    const buffer = firstMutableBuffer(data) orelse return 0;
    const n = try posix.read(file.handle, buffer);
    if (n == 0) return error.EndOfStream;
    return n;
}

fn fileReadPositional(file: File, data: []const []u8, offset: u64) File.ReadPositionalError!usize {
    const buffer = firstMutableBuffer(data) orelse return 0;
    return pread(file.handle, buffer, offset);
}

fn fileWriteStreaming(file: File, header: []const u8, data: []const []const u8, splat: usize) File.Writer.Error!usize {
    if (header.len != 0) return writeFd(file.handle, header);
    return writeData(file.handle, data, splat);
}

fn fileWritePositional(
    file: File,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
    offset: u64,
) File.WritePositionalError!usize {
    if (header.len != 0) return pwrite(file.handle, header, offset);
    const buffer = firstConstBuffer(data, splat) orelse return 0;
    return pwrite(file.handle, buffer, offset);
}

const PosixAddress = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,
};

fn netAccept(server: net.Socket.Handle, options: net.Server.AcceptOptions) (net.Server.AcceptError || error{WouldBlock})!net.Socket {
    _ = options;

    var storage: PosixAddress = undefined;
    var addr_len: posix.socklen_t = @sizeOf(PosixAddress);
    const fd: posix.fd_t = while (true) {
        const rc = posix.system.accept(server, &storage.any, &addr_len);
        switch (posix.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .CONNABORTED => return error.ConnectionAborted,
            .INVAL => return error.SocketNotListening,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .PROTO => return error.ProtocolFailure,
            .PERM => return error.BlockedByFirewall,
            else => |err| return posix.unexpectedErrno(err),
        }
    };
    errdefer closeFd(fd);

    try setNonblocking(fd);
    return .{
        .handle = fd,
        .address = addressFromPosix(&storage),
    };
}

fn netRead(handle: net.Socket.Handle, data: [][]u8) (net.Stream.Reader.Error || error{WouldBlock})!usize {
    const buffer = firstMutableBuffer(data) orelse return 0;
    return posix.read(handle, buffer) catch |err| switch (err) {
        error.WouldBlock => error.WouldBlock,
        error.SystemResources => error.SystemResources,
        error.ConnectionResetByPeer => error.ConnectionResetByPeer,
        error.SocketUnconnected => error.SocketUnconnected,
        error.AccessDenied => error.AccessDenied,
        else => error.Unexpected,
    };
}

fn netWrite(
    handle: net.Socket.Handle,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) (net.Stream.Writer.Error || error{WouldBlock})!usize {
    if (header.len != 0) return writeSocketFd(handle, header);
    const buffer = firstConstBuffer(data, splat) orelse return 0;
    return writeSocketFd(handle, buffer);
}

fn addressFromPosix(posix_address: *const PosixAddress) net.IpAddress {
    return switch (posix_address.any.family) {
        posix.AF.INET => .{ .ip4 = address4FromPosix(&posix_address.in) },
        posix.AF.INET6 => .{ .ip6 = address6FromPosix(&posix_address.in6) },
        else => .{ .ip4 = .loopback(0) },
    };
}

fn address4FromPosix(in: *const posix.sockaddr.in) net.Ip4Address {
    return .{
        .port = std.mem.bigToNative(u16, in.port),
        .bytes = @bitCast(in.addr),
    };
}

fn address6FromPosix(in6: *const posix.sockaddr.in6) net.Ip6Address {
    return .{
        .port = std.mem.bigToNative(u16, in6.port),
        .bytes = in6.addr,
        .flow = in6.flowinfo,
        .interface = .{ .index = in6.scope_id },
    };
}

fn writeData(fd: posix.fd_t, data: []const []const u8, splat: usize) (File.Writer.Error || error{WouldBlock})!usize {
    const buffer = firstConstBuffer(data, splat) orelse return 0;
    return writeFd(fd, buffer);
}

fn firstMutableBuffer(data: []const []u8) ?[]u8 {
    for (data) |buffer| {
        if (buffer.len != 0) return buffer;
    }
    return null;
}

fn firstConstBuffer(data: []const []const u8, splat: usize) ?[]const u8 {
    if (data.len == 0) return null;
    for (data[0 .. data.len - 1]) |buffer| {
        if (buffer.len != 0) return buffer;
    }
    if (splat == 0) return null;
    const pattern = data[data.len - 1];
    return if (pattern.len == 0) null else pattern;
}

fn pread(fd: posix.fd_t, buffer: []u8, offset: u64) File.ReadPositionalError!usize {
    while (true) {
        const rc = posix.system.pread(fd, buffer.ptr, buffer.len, @bitCast(offset));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .NXIO, .SPIPE, .OVERFLOW => return error.Unseekable,
            .BADF => return error.NotOpenForReading,
            .ACCES => return error.AccessDenied,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn pwrite(fd: posix.fd_t, buffer: []const u8, offset: u64) File.WritePositionalError!usize {
    while (true) {
        const rc = posix.system.pwrite(fd, buffer.ptr, buffer.len, @bitCast(offset));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .ACCES => return error.AccessDenied,
            .PERM => return error.PermissionDenied,
            .PIPE => return error.BrokenPipe,
            .BUSY => return error.DeviceBusy,
            .TXTBSY => return error.FileBusy,
            .NXIO, .SPIPE, .OVERFLOW => return error.Unseekable,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn writeFd(fd: posix.fd_t, buffer: []const u8) (File.Writer.Error || error{WouldBlock})!usize {
    if (buffer.len == 0) return 0;
    while (true) {
        const rc = posix.system.write(fd, buffer.ptr, buffer.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .ACCES => return error.AccessDenied,
            .PERM => return error.PermissionDenied,
            .PIPE => return error.BrokenPipe,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .BADF => return error.NotOpenForWriting,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn writeSocketFd(fd: posix.fd_t, buffer: []const u8) (net.Stream.Writer.Error || error{WouldBlock})!usize {
    if (buffer.len == 0) return 0;
    while (true) {
        const rc = posix.system.write(fd, buffer.ptr, buffer.len);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .CANCELED => return error.Canceled,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .NETUNREACH => return error.NetworkUnreachable,
            .HOSTUNREACH => return error.HostUnreachable,
            .NETDOWN => return error.NetworkDown,
            .CONNREFUSED => return error.ConnectionRefused,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .NOTCONN => return error.SocketUnconnected,
            .DESTADDRREQ => return error.SocketNotBound,
            .BADF => return error.Unexpected,
            .PIPE => return error.SocketUnconnected,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}
