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
        id: WorkerId,
        scheduler: Scheduler = .{},
        injections: InjectionQueue = .{},
        parker: parker.Parker = .{},

        const Self = @This();
        const InjectionQueue = injection_queue.InjectionQueue(ActorHeader);
        const Scheduler = scheduler.Scheduler(ActorHeader);

        pub fn init(id: WorkerId) Self {
            return .{ .id = id };
        }

        pub fn enqueue(worker: *Self, actor: *ActorHeader) void {
            worker.scheduler.enqueue(actor);
        }

        pub fn inject(worker: *Self, actor: *ActorHeader) bool {
            if (actor.queued.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return false;
            worker.injections.push(actor);
            return true;
        }

        pub fn injectAndNotify(worker: *Self, io: std.Io, actor: *ActorHeader) void {
            if (worker.inject(actor)) worker.parker.notify(io);
        }

        pub fn dequeue(worker: *Self) ?*ActorHeader {
            if (worker.scheduler.dequeue()) |actor| return actor;
            const actor = worker.injections.pop() orelse return null;
            actor.queued.store(false, .release);
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

    worker.enqueue(&actor);
    worker.setCurrent(&actor);

    try testing.expectEqual(@as(?*Header, &actor), worker.currentActor());
    try testing.expectEqual(@as(?*Header, &actor), worker.dequeue());
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
    worker.enqueue(&local);

    try testing.expectEqual(@as(?*Header, &local), worker.dequeue());
    try testing.expectEqual(@as(?*Header, &remote), worker.dequeue());
    try testing.expect(!remote.queued.load(.acquire));
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
    try testing.expectEqual(@as(?*Header, &remote), worker.dequeue());
}
