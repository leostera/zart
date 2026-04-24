//! Actor slot registry with generation checks for stale handle invalidation.

const std = @import("std");
const trace = @import("trace.zig");

const Allocator = std.mem.Allocator;
const ActorId = trace.ActorId;

pub fn Registry(comptime ActorHeader: type) type {
    return struct {
        mutex: std.Io.Mutex = .init,
        slabs: std.atomic.Value(?*Slab) = .init(null),
        slot_count: std.atomic.Value(usize) = .init(0),
        next_index: usize = 0,
        first_free: ?usize = null,

        const Self = @This();
        const slots_per_slab = 1024;

        const Slot = struct {
            generation: std.atomic.Value(u64) = .init(1),
            actor: std.atomic.Value(?*ActorHeader) = .init(null),
            next_free: ?usize = null,
        };

        const Slab = struct {
            base_index: usize,
            slots: []Slot,
            next: ?*Slab,
        };

        pub fn deinit(registry: *Self, allocator: Allocator, runtime: anytype) void {
            var slab = registry.slabs.load(.acquire);
            while (slab) |current| {
                for (current.slots) |*slot| {
                    if (slot.actor.load(.acquire)) |actor| {
                        actor.destroy_fn(runtime, actor);
                        slot.actor.store(null, .release);
                    }
                }
                slab = current.next;
                allocator.free(current.slots);
                allocator.destroy(current);
            }
            registry.* = undefined;
        }

        pub fn reserve(registry: *Self, allocator: Allocator, sync_io: std.Io) !ActorId {
            registry.mutex.lockUncancelable(sync_io);
            defer registry.mutex.unlock(sync_io);

            if (registry.first_free) |index| {
                const slot = registry.slotAt(index) orelse unreachable;
                std.debug.assert(slot.actor.load(.acquire) == null);

                registry.first_free = slot.next_free;
                slot.next_free = null;

                return .{
                    .index = index,
                    .generation = slot.generation.load(.acquire),
                };
            }

            const index = registry.next_index;
            const slot = try registry.ensureSlot(allocator, index);
            registry.next_index += 1;
            registry.slot_count.store(registry.next_index, .release);

            return .{
                .index = index,
                .generation = slot.generation.load(.acquire),
            };
        }

        pub fn cancelReserve(registry: *Self, sync_io: std.Io, actor_id: ActorId) void {
            registry.mutex.lockUncancelable(sync_io);
            defer registry.mutex.unlock(sync_io);

            const slot = registry.slotAt(actor_id.index) orelse unreachable;
            std.debug.assert(slot.actor.load(.acquire) == null);
            std.debug.assert(slot.generation.load(.acquire) == actor_id.generation);
            std.debug.assert(slot.next_free == null);

            slot.next_free = registry.first_free;
            registry.first_free = actor_id.index;
        }

        pub fn publish(registry: *Self, actor: *ActorHeader) void {
            const slot = registry.slotAt(actor.id.index) orelse unreachable;
            std.debug.assert(slot.actor.load(.acquire) == null);
            std.debug.assert(slot.generation.load(.acquire) == actor.id.generation);
            slot.actor.store(actor, .release);
        }

        pub fn get(registry: *Self, actor_id: ActorId) ?*ActorHeader {
            if (actor_id.index >= registry.slot_count.load(.acquire)) return null;

            const slot = registry.slotAt(actor_id.index) orelse return null;
            if (slot.generation.load(.acquire) != actor_id.generation) return null;
            const actor = slot.actor.load(.acquire) orelse return null;
            if (slot.generation.load(.acquire) != actor_id.generation) return null;
            return actor;
        }

        pub fn forEachActor(registry: *Self, sync_io: std.Io, visitor: anytype) !void {
            registry.mutex.lockUncancelable(sync_io);
            defer registry.mutex.unlock(sync_io);

            var slab = registry.slabs.load(.acquire);
            while (slab) |current| : (slab = current.next) {
                for (current.slots) |*slot| {
                    if (slot.actor.load(.acquire)) |actor| try visitor.visit(actor);
                }
            }
        }

        pub fn destroy(registry: *Self, sync_io: std.Io, runtime: anytype, actor: *ActorHeader) void {
            registry.remove(sync_io, actor);
            actor.destroy_fn(runtime, actor);
        }

        pub fn remove(registry: *Self, sync_io: std.Io, actor: *ActorHeader) void {
            registry.mutex.lockUncancelable(sync_io);
            defer registry.mutex.unlock(sync_io);

            const index = actor.id.index;
            const slot = registry.slotAt(index) orelse unreachable;
            std.debug.assert(slot.actor.load(.acquire) == actor);
            std.debug.assert(slot.generation.load(.acquire) == actor.id.generation);

            slot.actor.store(null, .release);
            var next_generation = slot.generation.load(.acquire) +% 1;
            if (next_generation == 0) next_generation = 1;
            slot.generation.store(next_generation, .release);
            slot.next_free = registry.first_free;
            registry.first_free = index;
        }

        fn ensureSlot(registry: *Self, allocator: Allocator, index: usize) !*Slot {
            if (registry.slotAt(index)) |slot| return slot;

            const slab = try allocator.create(Slab);
            errdefer allocator.destroy(slab);

            const slots = try allocator.alloc(Slot, slots_per_slab);
            errdefer allocator.free(slots);
            for (slots) |*slot| slot.* = .{};

            const base_index = index - (index % slots_per_slab);
            slab.* = .{
                .base_index = base_index,
                .slots = slots,
                .next = registry.slabs.load(.acquire),
            };
            registry.slabs.store(slab, .release);

            return &slots[index - base_index];
        }

        fn slotAt(registry: *Self, index: usize) ?*Slot {
            var slab = registry.slabs.load(.acquire);
            while (slab) |current| : (slab = current.next) {
                if (index >= current.base_index and index < current.base_index + current.slots.len) {
                    return &current.slots[index - current.base_index];
                }
            }
            return null;
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

    const first_id = try registry.reserve(testing.allocator, testing.io);
    var first: Header = .{ .id = first_id };
    registry.publish(&first);
    try testing.expectEqual(@as(?*Header, &first), registry.get(first_id));

    registry.remove(testing.io, &first);
    try testing.expectEqual(@as(?*Header, null), registry.get(first_id));

    const second_id = try registry.reserve(testing.allocator, testing.io);
    try testing.expectEqual(first_id.index, second_id.index);
    try testing.expect(first_id.generation != second_id.generation);

    var second: Header = .{ .id = second_id };
    registry.publish(&second);
    try testing.expectEqual(@as(?*Header, null), registry.get(first_id));
    try testing.expectEqual(@as(?*Header, &second), registry.get(second_id));

    var destroyed: usize = 0;
    registry.destroy(testing.io, &destroyed, &second);
    try testing.expectEqual(@as(usize, 1), destroyed);
}

test "registry reserves unique ids across concurrent callers" {
    const testing = std.testing;

    const ThreadCount = 4;
    const PerThread = 128;
    const Total = ThreadCount * PerThread;

    const Header = struct {
        id: ActorId,
        destroy_fn: *const fn (*usize, *@This()) void = destroy,

        fn destroy(count: *usize, _: *@This()) void {
            count.* += 1;
        }
    };

    const Producer = struct {
        registry: *Registry(Header),
        ids: []ActorId,
        failed: *std.atomic.Value(bool),

        fn run(self: @This()) void {
            for (self.ids) |*id| {
                id.* = self.registry.reserve(testing.allocator, testing.io) catch {
                    self.failed.store(true, .release);
                    return;
                };
            }
        }
    };

    var registry: Registry(Header) = .{};
    var cleanup_count: usize = 0;
    defer registry.deinit(testing.allocator, &cleanup_count);

    var ids: [Total]ActorId = undefined;
    var failed = std.atomic.Value(bool).init(false);
    var threads: [ThreadCount]std.Thread = undefined;

    for (&threads, 0..) |*thread, producer| {
        const start = producer * PerThread;
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{Producer{
            .registry = &registry,
            .ids = ids[start..][0..PerThread],
            .failed = &failed,
        }});
    }
    for (threads) |thread| thread.join();

    try testing.expect(!failed.load(.acquire));

    var seen = [_]bool{false} ** Total;
    for (ids) |id| {
        try testing.expect(id.index < Total);
        try testing.expect(!seen[id.index]);
        seen[id.index] = true;
    }
    for (seen) |item_seen| try testing.expect(item_seen);
}
