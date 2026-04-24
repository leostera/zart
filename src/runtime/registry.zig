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
            generation: u64 = 1,
            actor: ?*ActorHeader = null,
            next_free: ?usize = null,
        };

        pub fn deinit(registry: *Self, allocator: Allocator, runtime: anytype) void {
            for (registry.slots.items) |*slot| {
                if (slot.actor) |actor| {
                    actor.destroy_fn(runtime, actor);
                    slot.actor = null;
                }
            }
            registry.slots.deinit(allocator);
            registry.* = undefined;
        }

        pub fn reserve(registry: *Self, allocator: Allocator) !ActorId {
            if (registry.first_free) |index| {
                const slot = &registry.slots.items[index];
                std.debug.assert(slot.actor == null);

                registry.first_free = slot.next_free;
                slot.next_free = null;

                return .{
                    .index = index,
                    .generation = slot.generation,
                };
            }

            const index = registry.slots.items.len;
            try registry.slots.append(allocator, .{});

            return .{
                .index = index,
                .generation = registry.slots.items[index].generation,
            };
        }

        pub fn cancelReserve(registry: *Self, actor_id: ActorId) void {
            const slot = &registry.slots.items[actor_id.index];
            std.debug.assert(slot.actor == null);
            std.debug.assert(slot.generation == actor_id.generation);
            std.debug.assert(slot.next_free == null);

            slot.next_free = registry.first_free;
            registry.first_free = actor_id.index;
        }

        pub fn publish(registry: *Self, actor: *ActorHeader) void {
            const slot = &registry.slots.items[actor.id.index];
            std.debug.assert(slot.actor == null);
            std.debug.assert(slot.generation == actor.id.generation);
            slot.actor = actor;
        }

        pub fn get(registry: *Self, actor_id: ActorId) ?*ActorHeader {
            if (actor_id.index >= registry.slots.items.len) return null;

            const slot = &registry.slots.items[actor_id.index];
            if (slot.generation != actor_id.generation) return null;
            return slot.actor;
        }

        pub fn destroy(registry: *Self, runtime: anytype, actor: *ActorHeader) void {
            const index = actor.id.index;
            const slot = &registry.slots.items[index];
            std.debug.assert(slot.actor == actor);
            std.debug.assert(slot.generation == actor.id.generation);

            actor.destroy_fn(runtime, actor);
            slot.actor = null;
            slot.generation +%= 1;
            if (slot.generation == 0) slot.generation = 1;
            slot.next_free = registry.first_free;
            registry.first_free = index;
        }
    };
}
