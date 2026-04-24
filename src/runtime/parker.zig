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

const TestBarrier = struct {
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    ready: bool = false,

    fn signal(barrier: *TestBarrier, io: std.Io) void {
        barrier.mutex.lockUncancelable(io);
        defer barrier.mutex.unlock(io);

        barrier.ready = true;
        barrier.condition.signal(io);
    }

    fn wait(barrier: *TestBarrier, io: std.Io) void {
        barrier.mutex.lockUncancelable(io);
        defer barrier.mutex.unlock(io);

        while (!barrier.ready) barrier.condition.waitUncancelable(io, &barrier.mutex);
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
        ready: *TestBarrier,

        fn run(self: @This()) void {
            self.ready.signal(testing.io);
            self.result.* = self.parker.wait(testing.io);
        }
    };

    var parker: Parker = .{};
    var result: Parker.WaitResult = .closed;
    var ready: TestBarrier = .{};

    const thread = try std.Thread.spawn(.{}, Worker.run, .{Worker{
        .parker = &parker,
        .result = &result,
        .ready = &ready,
    }});

    ready.wait(testing.io);
    parker.notify(testing.io);
    thread.join();

    try testing.expectEqual(Parker.WaitResult.notified, result);
}

test "parker close wakes a waiting thread" {
    const testing = std.testing;

    const Worker = struct {
        parker: *Parker,
        result: *Parker.WaitResult,
        ready: *TestBarrier,

        fn run(self: @This()) void {
            self.ready.signal(testing.io);
            self.result.* = self.parker.wait(testing.io);
        }
    };

    var parker: Parker = .{};
    var result: Parker.WaitResult = .notified;
    var ready: TestBarrier = .{};

    const thread = try std.Thread.spawn(.{}, Worker.run, .{Worker{
        .parker = &parker,
        .result = &result,
        .ready = &ready,
    }});

    ready.wait(testing.io);
    parker.close(testing.io);
    thread.join();

    try testing.expectEqual(Parker.WaitResult.closed, result);
}
