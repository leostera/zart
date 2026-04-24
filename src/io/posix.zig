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
    pending_head: ?*RuntimeIo.Request = null,
    pending_tail: ?*RuntimeIo.Request = null,

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn deinit(self: *Self) void {
        std.debug.assert(self.pending_head == null);
        self.* = undefined;
    }

    pub fn driver(self: *Self) RuntimeIo.Driver {
        return .{
            .context = self,
            .submit_fn = submit,
            .poll_fn = poll,
        };
    }

    fn submit(context: ?*anyopaque, request: *RuntimeIo.Request) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        if (self.tryComplete(request)) return;
        self.enqueue(request);
    }

    fn poll(context: ?*anyopaque, mode: RuntimeIo.PollMode) !void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        if (self.pending_head == null) return;

        var pollfds_buffer: [256]posix.pollfd = undefined;
        while (true) {
            const pollfds = self.fillPollfds(&pollfds_buffer);
            if (pollfds.len == 0) return;

            const timeout: i32 = switch (mode) {
                .nonblocking => 0,
                .wait => -1,
            };
            const ready = try posix.poll(pollfds, timeout);
            if (ready == 0) return;

            self.retryReady(pollfds);
            if (mode == .nonblocking or self.pending_head == null) return;
        }
    }

    fn enqueue(self: *Self, request: *RuntimeIo.Request) void {
        request.next = null;
        if (self.pending_tail) |tail| {
            tail.next = request;
        } else {
            self.pending_head = request;
        }
        self.pending_tail = request;
    }

    fn fillPollfds(self: *Self, buffer: []posix.pollfd) []posix.pollfd {
        var count: usize = 0;
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

    fn retryReady(self: *Self, pollfds: []const posix.pollfd) void {
        var previous: ?*RuntimeIo.Request = null;
        var current = self.pending_head;
        var index: usize = 0;

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
            } else {
                previous = request;
            }

            current = next;
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
        .file_read_positional, .net_read => .read,
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
