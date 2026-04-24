//! Scheduler worker state.

const parker = @import("parker.zig");
const scheduler = @import("scheduler.zig");

pub const WorkerId = struct {
    index: usize,
};

pub fn Worker(comptime ActorHeader: type) type {
    return struct {
        id: WorkerId,
        scheduler: Scheduler = .{},
        parker: parker.Parker = .{},

        const Self = @This();
        const Scheduler = scheduler.Scheduler(ActorHeader);

        pub fn init(id: WorkerId) Self {
            return .{ .id = id };
        }

        pub fn enqueue(worker: *Self, actor: *ActorHeader) void {
            worker.scheduler.enqueue(actor);
        }

        pub fn dequeue(worker: *Self) ?*ActorHeader {
            return worker.scheduler.dequeue();
        }

        pub fn setCurrent(worker: *Self, actor: ?*ActorHeader) void {
            worker.scheduler.setCurrent(actor);
        }

        pub fn currentActor(worker: *const Self) ?*ActorHeader {
            return worker.scheduler.current_actor;
        }
    };
}

test "worker delegates local scheduling" {
    const testing = @import("std").testing;

    const Header = struct {
        queued: @import("std").atomic.Value(bool) = .init(false),
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
