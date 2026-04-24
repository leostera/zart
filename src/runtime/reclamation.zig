//! Deferred actor destruction.
//!
//! The single-threaded runtime drains this list at scheduler safe points. The
//! SMP runtime will extend this module with epoch/hazard checks before freeing
//! retired actor cells.

pub fn RetiredActors(comptime ActorHeader: type) type {
    return struct {
        head: ?*ActorHeader = null,

        const Self = @This();

        pub fn retire(retired: *Self, actor: *ActorHeader) void {
            actor.next_retired = retired.head;
            retired.head = actor;
        }

        pub fn drain(retired: *Self, runtime: anytype) void {
            while (retired.head) |actor| {
                retired.head = actor.next_retired;
                actor.next_retired = null;
                actor.destroy_fn(runtime, actor);
            }
        }
    };
}

test "retired actors drain through actor destroy functions" {
    const std = @import("std");
    const testing = std.testing;

    const Header = struct {
        next_retired: ?*@This() = null,
        destroyed: bool = false,
        destroy_fn: *const fn (*usize, *@This()) void = destroy,

        fn destroy(count: *usize, actor: *@This()) void {
            actor.destroyed = true;
            count.* += 1;
        }
    };

    var retired: RetiredActors(Header) = .{};
    var first: Header = .{};
    var second: Header = .{};
    var destroyed: usize = 0;

    retired.retire(&first);
    retired.retire(&second);
    retired.drain(&destroyed);

    try testing.expectEqual(@as(usize, 2), destroyed);
    try testing.expect(first.destroyed);
    try testing.expect(second.destroyed);
    try testing.expectEqual(@as(?*Header, null), retired.head);
}
