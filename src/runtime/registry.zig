//! Actor slot registry with generation checks for stale handle invalidation.

const std = @import("std");
const trace = @import("trace.zig");

const Allocator = std.mem.Allocator;
const ActorId = trace.ActorId;

pub fn Registry(comptime ActorHeader: type) type {
    return struct {
        slots: std.ArrayList(Slot) = .empty,
        first_free: ?usize = null,

        const Self = @This();

        const Slot = struct {
            generation: std.atomic.Value(u64) = .init(1),
            actor: std.atomic.Value(?*ActorHeader) = .init(null),
            next_free: ?usize = null,
        };

        pub fn deinit(registry: *Self, allocator: Allocator, runtime: anytype) void {
            for (registry.slots.items) |*slot| {
                if (slot.actor.load(.acquire)) |actor| {
                    actor.destroy_fn(runtime, actor);
                    slot.actor.store(null, .release);
                }
            }
            registry.slots.deinit(allocator);
            registry.* = undefined;
        }

        pub fn reserve(registry: *Self, allocator: Allocator) !ActorId {
            if (registry.first_free) |index| {
                const slot = &registry.slots.items[index];
                std.debug.assert(slot.actor.load(.acquire) == null);

                registry.first_free = slot.next_free;
                slot.next_free = null;

                return .{
                    .index = index,
                    .generation = slot.generation.load(.acquire),
                };
            }

            const index = registry.slots.items.len;
            try registry.slots.append(allocator, .{});

            return .{
                .index = index,
                .generation = registry.slots.items[index].generation.load(.acquire),
            };
        }

        pub fn cancelReserve(registry: *Self, actor_id: ActorId) void {
            const slot = &registry.slots.items[actor_id.index];
            std.debug.assert(slot.actor.load(.acquire) == null);
            std.debug.assert(slot.generation.load(.acquire) == actor_id.generation);
            std.debug.assert(slot.next_free == null);

            slot.next_free = registry.first_free;
            registry.first_free = actor_id.index;
        }

        pub fn publish(registry: *Self, actor: *ActorHeader) void {
            const slot = &registry.slots.items[actor.id.index];
            std.debug.assert(slot.actor.load(.acquire) == null);
            std.debug.assert(slot.generation.load(.acquire) == actor.id.generation);
            slot.actor.store(actor, .release);
        }

        pub fn get(registry: *Self, actor_id: ActorId) ?*ActorHeader {
            if (actor_id.index >= registry.slots.items.len) return null;

            const slot = &registry.slots.items[actor_id.index];
            if (slot.generation.load(.acquire) != actor_id.generation) return null;
            const actor = slot.actor.load(.acquire) orelse return null;
            if (slot.generation.load(.acquire) != actor_id.generation) return null;
            return actor;
        }

        pub fn destroy(registry: *Self, runtime: anytype, actor: *ActorHeader) void {
            registry.remove(actor);
            actor.destroy_fn(runtime, actor);
        }

        pub fn remove(registry: *Self, actor: *ActorHeader) void {
            const index = actor.id.index;
            const slot = &registry.slots.items[index];
            std.debug.assert(slot.actor.load(.acquire) == actor);
            std.debug.assert(slot.generation.load(.acquire) == actor.id.generation);

            slot.actor.store(null, .release);
            var next_generation = slot.generation.load(.acquire) +% 1;
            if (next_generation == 0) next_generation = 1;
            slot.generation.store(next_generation, .release);
            slot.next_free = registry.first_free;
            registry.first_free = index;
        }
    };
}

test "registry rejects stale ids after slot reuse" {
    const testing = std.testing;

    const Header = struct {
        id: ActorId,
        destroy_fn: *const fn (*usize, *@This()) void = destroy,

        fn destroy(count: *usize, _: *@This()) void {
            count.* += 1;
        }
    };

    var registry: Registry(Header) = .{};
    var cleanup_count: usize = 0;
    defer registry.deinit(testing.allocator, &cleanup_count);

    const first_id = try registry.reserve(testing.allocator);
    var first: Header = .{ .id = first_id };
    registry.publish(&first);
    try testing.expectEqual(@as(?*Header, &first), registry.get(first_id));

    registry.remove(&first);
    try testing.expectEqual(@as(?*Header, null), registry.get(first_id));

    const second_id = try registry.reserve(testing.allocator);
    try testing.expectEqual(first_id.index, second_id.index);
    try testing.expect(first_id.generation != second_id.generation);

    var second: Header = .{ .id = second_id };
    registry.publish(&second);
    try testing.expectEqual(@as(?*Header, null), registry.get(first_id));
    try testing.expectEqual(@as(?*Header, &second), registry.get(second_id));

    var destroyed: usize = 0;
    registry.destroy(&destroyed, &second);
    try testing.expectEqual(@as(usize, 1), destroyed);
}
