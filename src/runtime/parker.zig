//! Worker parking primitive.
//!
//! This is intentionally outside the actor hot path. Schedulers use it only
//! after local work, remote injections, stealing, and I/O polling all fail.

const std = @import("std");

pub const Parker = struct {
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    wakeups: usize = 0,
    closed: bool = false,

    pub const WaitResult = enum {
        notified,
        closed,
    };

    pub fn notify(parker: *Parker, io: std.Io) void {
        parker.mutex.lockUncancelable(io);
        defer parker.mutex.unlock(io);

        parker.wakeups += 1;
        parker.condition.signal(io);
    }

    pub fn notifyAll(parker: *Parker, io: std.Io) void {
        parker.mutex.lockUncancelable(io);
        defer parker.mutex.unlock(io);

        parker.wakeups += 1;
        parker.condition.broadcast(io);
    }

    pub fn close(parker: *Parker, io: std.Io) void {
        parker.mutex.lockUncancelable(io);
        defer parker.mutex.unlock(io);

        parker.closed = true;
        parker.condition.broadcast(io);
    }

    pub fn wait(parker: *Parker, io: std.Io) WaitResult {
        parker.mutex.lockUncancelable(io);
        defer parker.mutex.unlock(io);

        while (parker.wakeups == 0 and !parker.closed) {
            parker.condition.waitUncancelable(io, &parker.mutex);
        }

        if (parker.wakeups != 0) {
            parker.wakeups -= 1;
            return .notified;
        }

        return .closed;
    }
};

test "parker consumes notification sent before wait" {
    const testing = std.testing;

    var parker: Parker = .{};
    parker.notify(testing.io);

    try testing.expectEqual(Parker.WaitResult.notified, parker.wait(testing.io));
}

test "parker wakes a waiting thread" {
    const testing = std.testing;

    const Worker = struct {
        parker: *Parker,
        result: *Parker.WaitResult,
        ready: *std.atomic.Value(bool),

        fn run(self: @This()) void {
            self.ready.store(true, .release);
            self.result.* = self.parker.wait(testing.io);
        }
    };

    var parker: Parker = .{};
    var result: Parker.WaitResult = .closed;
    var ready = std.atomic.Value(bool).init(false);

    const thread = try std.Thread.spawn(.{}, Worker.run, .{Worker{
        .parker = &parker,
        .result = &result,
        .ready = &ready,
    }});

    while (!ready.load(.acquire)) std.atomic.spinLoopHint();
    parker.notify(testing.io);
    thread.join();

    try testing.expectEqual(Parker.WaitResult.notified, result);
}

test "parker close wakes a waiting thread" {
    const testing = std.testing;

    const Worker = struct {
        parker: *Parker,
        result: *Parker.WaitResult,
        ready: *std.atomic.Value(bool),

        fn run(self: @This()) void {
            self.ready.store(true, .release);
            self.result.* = self.parker.wait(testing.io);
        }
    };

    var parker: Parker = .{};
    var result: Parker.WaitResult = .notified;
    var ready = std.atomic.Value(bool).init(false);

    const thread = try std.Thread.spawn(.{}, Worker.run, .{Worker{
        .parker = &parker,
        .result = &result,
        .ready = &ready,
    }});

    while (!ready.load(.acquire)) std.atomic.spinLoopHint();
    parker.close(testing.io);
    thread.join();

    try testing.expectEqual(Parker.WaitResult.closed, result);
}
