//! Intrusive MPSC queue for remote scheduler injections.
//!
//! `ActorHeader.next_run` is reused as the link field. Producers may push
//! concurrently. Only the owning worker may pop.

const std = @import("std");

pub fn InjectionQueue(comptime ActorHeader: type) type {
    return struct {
        incoming: std.atomic.Value(?*ActorHeader) = .init(null),
        local_head: ?*ActorHeader = null,

        const Self = @This();

        pub fn push(queue: *Self, actor: *ActorHeader) void {
            var head = queue.incoming.load(.monotonic);
            while (true) {
                actor.next_run = head;
                head = queue.incoming.cmpxchgWeak(head, actor, .release, .monotonic) orelse return;
            }
        }

        pub fn pop(queue: *Self) ?*ActorHeader {
            if (queue.local_head == null) queue.refillLocal();

            const actor = queue.local_head orelse return null;
            queue.local_head = actor.next_run;
            actor.next_run = null;
            return actor;
        }

        pub fn hasPending(queue: *const Self) bool {
            return queue.local_head != null or queue.incoming.load(.acquire) != null;
        }

        fn refillLocal(queue: *Self) void {
            var stack = queue.incoming.swap(null, .acquire);
            var reversed: ?*ActorHeader = null;
            while (stack) |actor| {
                stack = actor.next_run;
                actor.next_run = reversed;
                reversed = actor;
            }
            queue.local_head = reversed;
        }
    };
}

test "injection queue accepts concurrent producers" {
    const testing = std.testing;

    const ProducerCount = 4;
    const PerProducer = 256;
    const Total = ProducerCount * PerProducer;

    const Header = struct {
        next_run: ?*@This() = null,
        id: usize,
    };

    const Producer = struct {
        queue: *InjectionQueue(Header),
        items: []Header,

        fn run(self: @This()) void {
            for (self.items) |*item| self.queue.push(item);
        }
    };

    var queue: InjectionQueue(Header) = .{};
    var items: [Total]Header = undefined;
    for (&items, 0..) |*item, id| item.* = .{ .id = id };

    var threads: [ProducerCount]std.Thread = undefined;
    for (&threads, 0..) |*thread, producer| {
        const start = producer * PerProducer;
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{Producer{
            .queue = &queue,
            .items = items[start..][0..PerProducer],
        }});
    }
    for (threads) |thread| thread.join();

    var seen = [_]bool{false} ** Total;
    var count: usize = 0;
    while (queue.pop()) |item| {
        try testing.expect(item.id < Total);
        try testing.expect(!seen[item.id]);
        seen[item.id] = true;
        count += 1;
    }

    try testing.expectEqual(@as(usize, Total), count);
    for (seen) |item_seen| try testing.expect(item_seen);
}
