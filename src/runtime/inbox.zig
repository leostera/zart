//! Single-consumer FIFO inbox storage for one actor.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn Inbox(comptime Msg: type) type {
    return struct {
        allocator: Allocator,
        head: ?*Node,
        tail: ?*Node,

        const Self = @This();

        const Node = struct {
            next: ?*Node,
            msg: Msg,
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .head = null,
                .tail = null,
            };
        }

        pub fn deinit(inbox: *Self) void {
            var next = inbox.head;
            while (next) |node| {
                next = node.next;
                inbox.allocator.destroy(node);
            }
            inbox.* = undefined;
        }

        pub fn push(inbox: *Self, msg: Msg) !void {
            const node = try inbox.allocator.create(Node);
            node.* = .{
                .next = null,
                .msg = msg,
            };

            if (inbox.tail) |tail| {
                tail.next = node;
            } else {
                inbox.head = node;
            }
            inbox.tail = node;
        }

        pub fn pop(inbox: *Self) ?Msg {
            const node = inbox.head orelse return null;
            inbox.head = node.next;
            if (inbox.head == null) inbox.tail = null;

            const msg = node.msg;
            inbox.allocator.destroy(node);
            return msg;
        }
    };
}
