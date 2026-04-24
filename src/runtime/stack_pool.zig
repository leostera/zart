//! Fixed-size stack slab allocator for actor fibers.

const std = @import("std");
const Fiber = @import("../Fiber.zig");

const Allocator = std.mem.Allocator;

pub const StackPool = struct {
    allocator: Allocator,
    stack_size: usize,
    slot_stride: usize,
    slab_size: usize,
    slabs: std.ArrayList(Slab) = .empty,
    free_list: ?*FreeNode = null,
    live_count: usize = 0,
    next_slab_slots: usize = 1,

    const Self = @This();

    const Slab = struct {
        memory: []align(Fiber.stack_alignment) u8,
    };

    const FreeNode = struct {
        next: ?*FreeNode,
    };

    pub fn init(allocator: Allocator, stack_size: usize, slab_size: usize) Self {
        const slot_size = @max(stack_size, @sizeOf(FreeNode));
        return .{
            .allocator = allocator,
            .stack_size = stack_size,
            .slot_stride = std.mem.alignForward(usize, slot_size, Fiber.stack_alignment),
            .slab_size = @max(slab_size, slot_size),
        };
    }

    pub fn matchesPolicy(pool: *const Self, stack_size: usize, slab_size: usize) bool {
        const slot_size = @max(stack_size, @sizeOf(FreeNode));
        const slot_stride = std.mem.alignForward(usize, slot_size, Fiber.stack_alignment);
        return pool.stack_size == stack_size and
            pool.slot_stride == slot_stride and
            pool.slab_size == @max(slab_size, slot_size);
    }

    pub fn preallocateSlab(pool: *Self) !void {
        if (pool.free_list != null) return;
        try pool.addSlabWithSlotCount(pool.maxSlabSlots());
        pool.next_slab_slots = pool.maxSlabSlots();
    }

    pub fn deinit(pool: *Self) void {
        std.debug.assert(pool.live_count == 0);
        for (pool.slabs.items) |slab| {
            pool.allocator.free(slab.memory);
        }
        pool.slabs.deinit(pool.allocator);
        pool.* = undefined;
    }

    pub fn alloc(pool: *Self) ![]align(Fiber.stack_alignment) u8 {
        if (pool.free_list == null) try pool.addSlab();

        const node = pool.free_list orelse unreachable;
        pool.free_list = node.next;
        pool.live_count += 1;

        const bytes: [*]align(Fiber.stack_alignment) u8 = @ptrCast(@alignCast(node));
        return bytes[0..pool.stack_size];
    }

    pub fn free(pool: *Self, stack: []align(Fiber.stack_alignment) u8) void {
        std.debug.assert(stack.len == pool.stack_size);

        const node: *FreeNode = @ptrCast(@alignCast(stack.ptr));
        node.* = .{ .next = pool.free_list };
        pool.free_list = node;
        pool.live_count -= 1;
    }

    fn addSlab(pool: *Self) !void {
        const max_slab_slots = pool.maxSlabSlots();
        const slot_count = @min(pool.next_slab_slots, max_slab_slots);
        try pool.addSlabWithSlotCount(slot_count);
        pool.next_slab_slots = @min(slot_count *| 2, max_slab_slots);
    }

    fn addSlabWithSlotCount(pool: *Self, slot_count: usize) !void {
        const memory = try pool.allocator.alignedAlloc(
            u8,
            std.mem.Alignment.fromByteUnits(Fiber.stack_alignment),
            slot_count * pool.slot_stride,
        );
        errdefer pool.allocator.free(memory);

        try pool.slabs.append(pool.allocator, .{ .memory = memory });

        var index = slot_count;
        while (index != 0) {
            index -= 1;
            const ptr = memory.ptr + index * pool.slot_stride;
            const node: *FreeNode = @ptrCast(@alignCast(ptr));
            node.* = .{ .next = pool.free_list };
            pool.free_list = node;
        }
    }

    fn maxSlabSlots(pool: *const Self) usize {
        return @max(@divFloor(pool.slab_size, pool.slot_stride), 1);
    }
};

test "stack pool returns aligned fixed-size stacks and reuses freed slots" {
    const testing = std.testing;

    var pool = StackPool.init(testing.allocator, Fiber.minimum_stack_size, Fiber.minimum_stack_size * 2);
    defer pool.deinit();

    const first = try pool.alloc();
    const second = try pool.alloc();

    try testing.expectEqual(Fiber.minimum_stack_size, first.len);
    try testing.expectEqual(Fiber.minimum_stack_size, second.len);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(first.ptr) % Fiber.stack_alignment);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(second.ptr) % Fiber.stack_alignment);

    pool.free(second);
    const reused = try pool.alloc();
    try testing.expectEqual(@intFromPtr(second.ptr), @intFromPtr(reused.ptr));

    pool.free(reused);
    pool.free(first);
}
