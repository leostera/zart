//! Scheduler worker state.

const std = @import("std");
const injection_queue = @import("injection_queue.zig");
const parker = @import("parker.zig");
const scheduler = @import("scheduler.zig");

pub const WorkerId = struct {
    index: usize,
};

pub fn Worker(comptime ActorHeader: type) type {
    return struct {
        _: void align(std.atomic.cache_line) = {},
        id: WorkerId,
        scheduler_mutex: std.Io.Mutex = .init,
        scheduler: Scheduler = .{},
        injections: InjectionQueue = .{},
        parker: parker.Parker = .{},
        steal_count: std.atomic.Value(usize) = .init(0),

        const Self = @This();
        const InjectionQueue = injection_queue.InjectionQueue(ActorHeader);
        const Scheduler = scheduler.Scheduler(ActorHeader);

        pub fn init(id: WorkerId) Self {
            return .{ .id = id };
        }

        pub fn enqueue(worker: *Self, io: std.Io, actor: *ActorHeader) bool {
            worker.scheduler_mutex.lockUncancelable(io);
            defer worker.scheduler_mutex.unlock(io);

            return worker.scheduler.enqueue(actor);
        }

        pub fn inject(worker: *Self, actor: *ActorHeader) bool {
            if (actor.queued.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return false;
            worker.injections.push(actor);
            return true;
        }

        pub fn injectAndNotify(worker: *Self, io: std.Io, actor: *ActorHeader) void {
            if (worker.inject(actor)) worker.parker.notify(io);
        }

        pub fn dequeue(worker: *Self, io: std.Io) ?*ActorHeader {
            worker.scheduler_mutex.lockUncancelable(io);
            if (worker.scheduler.dequeue()) |actor| {
                worker.scheduler_mutex.unlock(io);
                return actor;
            }
            worker.scheduler_mutex.unlock(io);

            const actor = worker.injections.pop() orelse return null;
            actor.queued.store(false, .release);
            return actor;
        }

        pub fn steal(worker: *Self, io: std.Io) ?*ActorHeader {
            worker.scheduler_mutex.lockUncancelable(io);
            defer worker.scheduler_mutex.unlock(io);

            const actor = worker.scheduler.dequeue() orelse return null;
            _ = worker.steal_count.fetchAdd(1, .acq_rel);
            return actor;
        }

        pub fn setCurrent(worker: *Self, actor: ?*ActorHeader) void {
            worker.scheduler.setCurrent(actor);
        }

        pub fn currentActor(worker: *const Self) ?*ActorHeader {
            return worker.scheduler.current_actor;
        }

        pub fn wait(worker: *Self, io: std.Io) parker.Parker.WaitResult {
            return worker.parker.wait(io);
        }

        pub fn notify(worker: *Self, io: std.Io) void {
            worker.parker.notify(io);
        }

        pub fn close(worker: *Self, io: std.Io) void {
            worker.parker.close(io);
        }
    };
}

test "worker delegates local scheduling" {
    const testing = std.testing;

    const Header = struct {
        queued: std.atomic.Value(bool) = .init(false),
        next_run: ?*@This() = null,
        id: usize,
    };

    var worker = Worker(Header).init(.{ .index = 0 });
    var actor: Header = .{ .id = 1 };

    try testing.expect(worker.enqueue(testing.io, &actor));
    worker.setCurrent(&actor);

    try testing.expectEqual(@as(?*Header, &actor), worker.currentActor());
    try testing.expectEqual(@as(?*Header, &actor), worker.dequeue(testing.io));
}

test "worker consumes remote injections after local queue" {
    const testing = std.testing;

    const Header = struct {
        queued: std.atomic.Value(bool) = .init(false),
        next_run: ?*@This() = null,
        id: usize,
    };

    var worker = Worker(Header).init(.{ .index = 0 });
    var local: Header = .{ .id = 1 };
    var remote: Header = .{ .id = 2 };

    try testing.expect(worker.inject(&remote));
    try testing.expect(worker.enqueue(testing.io, &local));

    try testing.expectEqual(@as(?*Header, &local), worker.dequeue(testing.io));
    try testing.expectEqual(@as(?*Header, &remote), worker.dequeue(testing.io));
    try testing.expect(!remote.queued.load(.acquire));
}

test "worker steals from another worker local queue" {
    const testing = std.testing;

    const Header = struct {
        queued: std.atomic.Value(bool) = .init(false),
        next_run: ?*@This() = null,
        id: usize,
    };

    var victim = Worker(Header).init(.{ .index = 0 });
    var actor: Header = .{ .id = 1 };

    try testing.expect(victim.enqueue(testing.io, &actor));
    try testing.expectEqual(@as(?*Header, &actor), victim.steal(testing.io));
    try testing.expectEqual(@as(usize, 1), victim.steal_count.load(.acquire));
    try testing.expect(!actor.queued.load(.acquire));
}

test "worker notifies parker for remote injections" {
    const testing = std.testing;

    const Header = struct {
        queued: std.atomic.Value(bool) = .init(false),
        next_run: ?*@This() = null,
        id: usize,
    };

    var worker = Worker(Header).init(.{ .index = 0 });
    var remote: Header = .{ .id = 1 };

    worker.injectAndNotify(testing.io, &remote);

    try testing.expectEqual(parker.Parker.WaitResult.notified, worker.wait(testing.io));
    try testing.expectEqual(@as(?*Header, &remote), worker.dequeue(testing.io));
}
