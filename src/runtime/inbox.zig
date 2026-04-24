//! Multi-producer, single-consumer FIFO inbox storage for one actor.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn Inbox(comptime Msg: type) type {
    return struct {
        allocator: Allocator,
        incoming: std.atomic.Value(?*Node),
        local_head: ?*Node,

        const Self = @This();

        const Node = struct {
            next: ?*Node,
            msg: Msg,
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .incoming = .init(null),
                .local_head = null,
            };
        }

        pub fn deinit(inbox: *Self) void {
            inbox.discard(inbox.local_head);
            inbox.discard(inbox.incoming.swap(null, .acquire));
            inbox.* = undefined;
        }

        /// Structurally copies `msg` into the inbox.
        ///
        /// This is safe for many producers. The actor owning this inbox must be
        /// the only consumer calling `pop`.
        pub fn push(inbox: *Self, msg: Msg) !void {
            const node = try inbox.allocator.create(Node);
            node.* = .{
                .next = null,
                .msg = msg,
            };

            var head = inbox.incoming.load(.monotonic);
            while (true) {
                node.next = head;
                head = inbox.incoming.cmpxchgWeak(head, node, .release, .monotonic) orelse return;
            }
        }

        pub fn pop(inbox: *Self) ?Msg {
            if (inbox.local_head == null) inbox.refillLocal();

            const node = inbox.local_head orelse return null;
            inbox.local_head = node.next;

            const msg = node.msg;
            inbox.allocator.destroy(node);
            return msg;
        }

        fn refillLocal(inbox: *Self) void {
            var stack = inbox.incoming.swap(null, .acquire);
            var reversed: ?*Node = null;
            while (stack) |node| {
                stack = node.next;
                node.next = reversed;
                reversed = node;
            }
            inbox.local_head = reversed;
        }

        fn discard(inbox: *Self, list: ?*Node) void {
            var next = list;
            while (next) |node| {
                next = node.next;
                inbox.allocator.destroy(node);
            }
        }
    };
}

test "inbox preserves FIFO order for one producer" {
    const testing = std.testing;
    var inbox = Inbox(usize).init(testing.allocator);
    defer inbox.deinit();

    try inbox.push(1);
    try inbox.push(2);
    try inbox.push(3);

    try testing.expectEqual(@as(?usize, 1), inbox.pop());
    try testing.expectEqual(@as(?usize, 2), inbox.pop());
    try testing.expectEqual(@as(?usize, 3), inbox.pop());
    try testing.expectEqual(@as(?usize, null), inbox.pop());
}

test "inbox accepts concurrent producers and one consumer" {
    const testing = std.testing;

    const ProducerCount = 4;
    const MessagesPerProducer = 256;
    const TotalMessages = ProducerCount * MessagesPerProducer;

    const Msg = struct {
        producer: usize,
        sequence: usize,
    };

    const Producer = struct {
        inbox: *Inbox(Msg),
        producer: usize,

        fn run(self: @This()) !void {
            for (0..MessagesPerProducer) |sequence| {
                try self.inbox.push(.{
                    .producer = self.producer,
                    .sequence = sequence,
                });
            }
        }
    };

    var inbox = Inbox(Msg).init(testing.allocator);
    defer inbox.deinit();

    var threads: [ProducerCount]std.Thread = undefined;
    for (&threads, 0..) |*thread, producer| {
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{Producer{
            .inbox = &inbox,
            .producer = producer,
        }});
    }
    for (threads) |thread| thread.join();

    var seen = [_][MessagesPerProducer]bool{[_]bool{false} ** MessagesPerProducer} ** ProducerCount;
    var count: usize = 0;
    while (inbox.pop()) |msg| {
        try testing.expect(msg.producer < ProducerCount);
        try testing.expect(msg.sequence < MessagesPerProducer);
        try testing.expect(!seen[msg.producer][msg.sequence]);
        seen[msg.producer][msg.sequence] = true;
        count += 1;
    }

    try testing.expectEqual(@as(usize, TotalMessages), count);
    for (seen) |producer_seen| {
        for (producer_seen) |message_seen| try testing.expect(message_seen);
    }
}
