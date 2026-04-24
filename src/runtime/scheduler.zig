//! Scheduler-local runnable queue.
//!
//! This module is deliberately small today. The single-threaded runtime uses
//! one `Scheduler`; the SMP runtime will give each worker its own scheduler
//! plus remote injection and stealing paths.

const std = @import("std");

pub fn Scheduler(comptime ActorHeader: type) type {
    return struct {
        run_head: ?*ActorHeader = null,
        run_tail: ?*ActorHeader = null,
        current_actor: ?*ActorHeader = null,

        const Self = @This();

        pub fn enqueue(scheduler: *Self, target: *ActorHeader) bool {
            if (target.queued.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return false;
            scheduler.enqueueClaimed(target);
            return true;
        }

        pub fn enqueueClaimed(scheduler: *Self, target: *ActorHeader) void {
            target.next_run = null;

            if (scheduler.run_tail) |tail| {
                tail.next_run = target;
            } else {
                scheduler.run_head = target;
            }
            scheduler.run_tail = target;
        }

        pub fn dequeue(scheduler: *Self) ?*ActorHeader {
            const target = scheduler.run_head orelse return null;
            scheduler.run_head = target.next_run;
            if (scheduler.run_head == null) scheduler.run_tail = null;

            target.next_run = null;
            target.queued.store(false, .release);
            return target;
        }

        pub fn hasQueued(scheduler: *const Self) bool {
            return scheduler.run_head != null;
        }

        pub fn setCurrent(scheduler: *Self, actor: ?*ActorHeader) void {
            scheduler.current_actor = actor;
        }
    };
}

test "scheduler queues actors FIFO and skips duplicate enqueues" {
    const testing = std.testing;

    const Header = struct {
        queued: std.atomic.Value(bool) = .init(false),
        next_run: ?*@This() = null,
        id: usize,
    };

    var scheduler: Scheduler(Header) = .{};
    var first: Header = .{ .id = 1 };
    var second: Header = .{ .id = 2 };

    try testing.expect(scheduler.enqueue(&first));
    try testing.expect(!scheduler.enqueue(&first));
    try testing.expect(scheduler.enqueue(&second));

    try testing.expectEqual(@as(?*Header, &first), scheduler.dequeue());
    try testing.expectEqual(@as(?*Header, &second), scheduler.dequeue());
    try testing.expectEqual(@as(?*Header, null), scheduler.dequeue());
    try testing.expect(!first.queued.load(.acquire));
    try testing.expect(!second.queued.load(.acquire));
}
