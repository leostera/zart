//! kqueue-backed non-blocking actor I/O driver.
//!
//! Readiness events carry the `RuntimeIo.Request` pointer in `udata`, so the
//! backend can complete the actor request directly without scanning all pending
//! descriptors.

const std = @import("std");
const RuntimeIo = @import("../runtime/io.zig").RuntimeIo;
const posix_io = @import("posix.zig");

const posix = std.posix;

const wake_ident: usize = 1;
const wake_udata: usize = 1;

pub const Kqueue = struct {
    mutex: std.Io.Mutex = .init,
    pending_head: ?*RuntimeIo.Request = null,
    pending_tail: ?*RuntimeIo.Request = null,
    pending_count: usize = 0,
    kq_fd: posix.fd_t,

    const Self = @This();

    pub fn init() !Self {
        const kq_fd = try createKqueue();
        errdefer posix_io.closeFd(kq_fd);

        var self: Self = .{ .kq_fd = kq_fd };
        try self.registerWakeEvent();
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.pending_count != 0 or self.pending_head != null or self.pending_tail != null) {
            @panic("kqueue I/O driver deinit called with pending requests");
        }
        posix_io.closeFd(self.kq_fd);
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
        if (posix_io.tryComplete(request)) return;

        self.mutex.lockUncancelable(std.Options.debug_io);
        if (std.debug.runtime_safety) self.assertNotPending(request);
        self.enqueue(request);
        self.mutex.unlock(std.Options.debug_io);

        self.armRequest(request) catch {
            self.mutex.lockUncancelable(std.Options.debug_io);
            _ = self.remove(request);
            self.mutex.unlock(std.Options.debug_io);
            @panic("failed to register kqueue actor I/O request");
        };
    }

    fn poll(context: ?*anyopaque, mode: RuntimeIo.PollMode) !void {
        const self: *Self = @ptrCast(@alignCast(context.?));

        var events_buffer: [256]posix.Kevent = undefined;
        var zero_timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };

        while (true) {
            const timeout: ?*const posix.timespec = switch (mode) {
                .nonblocking => &zero_timeout,
                .wait => null,
            };
            const ready = try kevent(self.kq_fd, &.{}, &events_buffer, timeout);
            if (ready == 0) return;

            var completed_any = false;
            for (events_buffer[0..ready]) |event| {
                if (event.udata == wake_udata) continue;
                const request: *RuntimeIo.Request = @ptrFromInt(event.udata);
                completed_any = self.retryReady(request) or completed_any;
            }

            if (mode == .nonblocking or completed_any or self.isEmpty()) return;
        }
    }

    fn wake(context: ?*anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.triggerWakeEvent() catch {};
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

    fn remove(self: *Self, request: *RuntimeIo.Request) bool {
        var previous: ?*RuntimeIo.Request = null;
        var current = self.pending_head;
        while (current) |node| {
            const next = node.next;
            if (node == request) {
                if (previous) |prev| {
                    prev.next = next;
                } else {
                    self.pending_head = next;
                }
                if (self.pending_tail == request) self.pending_tail = previous;
                request.next = null;
                self.pending_count -= 1;
                if (std.debug.runtime_safety) self.assertPendingCountConsistent();
                return true;
            }
            previous = node;
            current = next;
        }
        return false;
    }

    fn retryReady(self: *Self, request: *RuntimeIo.Request) bool {
        if (posix_io.tryComplete(request)) {
            self.mutex.lockUncancelable(std.Options.debug_io);
            const removed = self.remove(request);
            self.mutex.unlock(std.Options.debug_io);
            if (!removed and std.debug.runtime_safety) @panic("kqueue completed request that was not pending");
            return true;
        }

        self.armRequest(request) catch {
            self.mutex.lockUncancelable(std.Options.debug_io);
            _ = self.remove(request);
            self.mutex.unlock(std.Options.debug_io);
            @panic("failed to rearm kqueue actor I/O request");
        };
        return false;
    }

    fn armRequest(self: *Self, request: *RuntimeIo.Request) !void {
        var change = requestEvent(request);
        _ = try kevent(self.kq_fd, (&change)[0..1], &.{}, null);
    }

    fn registerWakeEvent(self: *Self) !void {
        var change: posix.Kevent = .{
            .ident = wake_ident,
            .filter = @intCast(std.c.EVFILT.USER),
            .flags = @intCast(std.c.EV.ADD | std.c.EV.CLEAR),
            .fflags = 0,
            .data = 0,
            .udata = wake_udata,
        };
        _ = try kevent(self.kq_fd, (&change)[0..1], &.{}, null);
    }

    fn triggerWakeEvent(self: *Self) !void {
        var change: posix.Kevent = .{
            .ident = wake_ident,
            .filter = @intCast(std.c.EVFILT.USER),
            .flags = 0,
            .fflags = std.c.NOTE.TRIGGER,
            .data = 0,
            .udata = wake_udata,
        };
        _ = try kevent(self.kq_fd, (&change)[0..1], &.{}, null);
    }

    fn isEmpty(self: *Self) bool {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        return self.pending_head == null;
    }

    fn assertNotPending(self: *const Self, request: *const RuntimeIo.Request) void {
        var node = self.pending_head;
        while (node) |current| : (node = current.next) {
            if (current == request) @panic("kqueue I/O request submitted while already pending");
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
            @panic("kqueue I/O pending queue corrupted");
        }
    }
};

fn requestEvent(request: *RuntimeIo.Request) posix.Kevent {
    var flags: u32 = std.c.EV.ADD | std.c.EV.ONESHOT;
    if (comptime @hasDecl(std.c.EV, "UDATA_SPECIFIC")) {
        flags |= std.c.EV.UDATA_SPECIFIC;
    }

    return .{
        .ident = @intCast(posix_io.fdOf(request)),
        .filter = filterOf(posix_io.interestOf(request)),
        .flags = @intCast(flags),
        .fflags = 0,
        .data = 0,
        .udata = @intFromPtr(request),
    };
}

fn filterOf(interest: posix_io.Interest) @TypeOf(@as(posix.Kevent, undefined).filter) {
    return switch (interest) {
        .read => @intCast(std.c.EVFILT.READ),
        .write => @intCast(std.c.EVFILT.WRITE),
    };
}

fn createKqueue() !posix.fd_t {
    const rc = posix.system.kqueue();
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn kevent(
    kq_fd: posix.fd_t,
    changelist: []const posix.Kevent,
    eventlist: []posix.Kevent,
    timeout: ?*const posix.timespec,
) !usize {
    while (true) {
        const rc = posix.system.kevent(
            kq_fd,
            changelist.ptr,
            std.math.cast(c_int, changelist.len) orelse return error.Overflow,
            eventlist.ptr,
            std.math.cast(c_int, eventlist.len) orelse return error.Overflow,
            timeout,
        );
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .NOENT => return error.EventNotFound,
            .NOMEM => return error.SystemResources,
            .SRCH => return error.ProcessNotFound,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}
