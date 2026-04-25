//! kqueue-backed non-blocking actor I/O driver.
//!
//! Readiness events carry a stable fd/interest entry in `udata`. Each entry owns
//! a FIFO of actor waiters, and every waiter points at the exact actor/request
//! that must be retried when the fd becomes ready.

const std = @import("std");
const RuntimeIo = @import("../runtime/io.zig").RuntimeIo;
const posix_io = @import("posix.zig");

const Allocator = std.mem.Allocator;
const posix = std.posix;

const wake_ident: usize = 1;
const wake_udata: usize = 1;
const EntryMap = std.AutoHashMapUnmanaged(u64, *FdEntry);
const IoWaiter = RuntimeIo.Request.IoWaiter;

pub const Kqueue = struct {
    mutex: std.Io.Mutex = .init,
    allocator: Allocator,
    entries: EntryMap = .empty,
    pending_head: ?*RuntimeIo.Request = null,
    pending_tail: ?*RuntimeIo.Request = null,
    pending_count: usize = 0,
    kq_fd: posix.fd_t,

    const Self = @This();

    pub fn init() !Self {
        return initWithAllocator(std.heap.smp_allocator);
    }

    pub fn initWithAllocator(allocator: Allocator) !Self {
        const kq_fd = try createKqueue();
        errdefer posix_io.closeFd(kq_fd);

        var self: Self = .{
            .allocator = allocator,
            .kq_fd = kq_fd,
        };
        try self.registerWakeEvent();
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.pending_count != 0 or self.pending_head != null or self.pending_tail != null) {
            @panic("kqueue I/O driver deinit called with pending requests");
        }
        var values = self.entries.valueIterator();
        while (values.next()) |entry_ptr| self.allocator.destroy(entry_ptr.*);
        self.entries.deinit(self.allocator);
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
        request.io_waiter.reset(request);
        if (posix_io.tryComplete(request)) return;

        self.mutex.lockUncancelable(std.Options.debug_io);
        if (std.debug.runtime_safety) self.assertNotPending(request);
        const entry = self.ensureEntryLocked(posix_io.fdOf(request), posix_io.interestOf(request)) catch {
            self.mutex.unlock(std.Options.debug_io);
            @panic("failed to allocate kqueue actor I/O waiter");
        };
        self.enqueue(request);
        entry.append(request);
        if (!entry.registered) {
            self.registerEntry(entry) catch {
                _ = entry.remove(request);
                _ = self.remove(request);
                self.mutex.unlock(std.Options.debug_io);
                @panic("failed to register kqueue actor I/O request");
            };
            entry.registered = true;
        }
        self.mutex.unlock(std.Options.debug_io);
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
                const entry: *FdEntry = @ptrFromInt(event.udata);
                completed_any = self.retryReady(entry) or completed_any;
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

    fn prepend(self: *Self, request: *RuntimeIo.Request) void {
        request.next = self.pending_head;
        self.pending_head = request;
        if (self.pending_tail == null) self.pending_tail = request;
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

    fn retryReady(self: *Self, entry: *FdEntry) bool {
        var completed_any = false;

        while (true) {
            self.mutex.lockUncancelable(std.Options.debug_io);
            const request = entry.firstRequest() orelse {
                self.mutex.unlock(std.Options.debug_io);
                return completed_any;
            };
            const waiter_removed_before_retry = entry.remove(request);
            const pending_removed_before_retry = self.remove(request);
            self.mutex.unlock(std.Options.debug_io);

            if (!posix_io.tryComplete(request)) {
                self.mutex.lockUncancelable(std.Options.debug_io);
                self.prepend(request);
                entry.prepend(request);
                self.mutex.unlock(std.Options.debug_io);
                if (std.debug.runtime_safety and (!waiter_removed_before_retry or !pending_removed_before_retry)) {
                    @panic("kqueue retried request that was not pending");
                }
                return completed_any;
            }

            self.mutex.lockUncancelable(std.Options.debug_io);
            if (entry.isEmpty() and entry.registered) {
                self.deregisterEntry(entry) catch {};
                entry.registered = false;
            }
            self.mutex.unlock(std.Options.debug_io);

            if (std.debug.runtime_safety and (!pending_removed_before_retry or !waiter_removed_before_retry)) {
                @panic("kqueue completed request that was not pending");
            }
            completed_any = true;
        }
    }

    fn ensureEntryLocked(self: *Self, fd: posix.fd_t, interest: posix_io.Interest) !*FdEntry {
        const key = entryKey(fd, interest);
        if (self.entries.get(key)) |entry| return entry;

        const entry = try self.allocator.create(FdEntry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{
            .fd = fd,
            .interest = interest,
        };
        try self.entries.put(self.allocator, key, entry);
        return entry;
    }

    fn registerEntry(self: *Self, entry: *FdEntry) !void {
        var change = entryEvent(entry, std.c.EV.ADD | std.c.EV.CLEAR);
        _ = try kevent(self.kq_fd, (&change)[0..1], &.{}, null);
    }

    fn deregisterEntry(self: *Self, entry: *FdEntry) !void {
        var change = entryEvent(entry, std.c.EV.DELETE);
        _ = kevent(self.kq_fd, (&change)[0..1], &.{}, null) catch |err| switch (err) {
            error.EventNotFound => return,
            else => |e| return e,
        };
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

const FdEntry = struct {
    fd: posix.fd_t,
    interest: posix_io.Interest,
    registered: bool = false,
    waiter_head: ?*IoWaiter = null,
    waiter_tail: ?*IoWaiter = null,

    fn append(entry: *FdEntry, request: *RuntimeIo.Request) void {
        const waiter = &request.io_waiter;
        waiter.reset(request);
        waiter.next = null;
        if (entry.waiter_tail) |tail| {
            tail.next = waiter;
        } else {
            entry.waiter_head = waiter;
        }
        entry.waiter_tail = waiter;
    }

    fn prepend(entry: *FdEntry, request: *RuntimeIo.Request) void {
        const waiter = &request.io_waiter;
        waiter.reset(request);
        waiter.next = entry.waiter_head;
        entry.waiter_head = waiter;
        if (entry.waiter_tail == null) entry.waiter_tail = waiter;
    }

    fn remove(entry: *FdEntry, request: *RuntimeIo.Request) bool {
        var previous: ?*IoWaiter = null;
        var current = entry.waiter_head;
        while (current) |waiter| {
            const next = waiter.next;
            if (waiter.request == request) {
                if (previous) |prev| {
                    prev.next = next;
                } else {
                    entry.waiter_head = next;
                }
                if (entry.waiter_tail == waiter) entry.waiter_tail = previous;
                waiter.next = null;
                waiter.request = null;
                waiter.actor = null;
                return true;
            }
            previous = waiter;
            current = next;
        }
        return false;
    }

    fn firstRequest(entry: *const FdEntry) ?*RuntimeIo.Request {
        const waiter = entry.waiter_head orelse return null;
        const request = waiter.request orelse return null;
        if (waiter.actor != request.actor) return null;
        return request;
    }

    fn isEmpty(entry: *const FdEntry) bool {
        return entry.waiter_head == null;
    }
};

fn entryEvent(entry: *FdEntry, flags: u32) posix.Kevent {
    return .{
        .ident = @intCast(entry.fd),
        .filter = filterOf(entry.interest),
        .flags = @intCast(flags),
        .fflags = 0,
        .data = 0,
        .udata = @intFromPtr(entry),
    };
}

fn entryKey(fd: posix.fd_t, interest: posix_io.Interest) u64 {
    const fd_part: u64 = @intCast(fd);
    const interest_part: u64 = switch (interest) {
        .read => 0,
        .write => 1,
    };
    return (fd_part << 1) | interest_part;
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
