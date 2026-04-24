const common = @import("common.zig");
const std = common.std;
const Actor = common.Actor;
const Ctx = common.Ctx;
const Runtime = common.Runtime;

test "function actor counter" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const CounterMsg = union(enum) {
        inc: u64,
        get: Actor(Reply),
        stop,
    };

    const Actors = struct {
        fn counter(ctx: *Ctx(CounterMsg)) !void {
            var value: u64 = 0;

            while (true) {
                switch (try ctx.recv()) {
                    .inc => |n| value += n,
                    .get => |reply_to| try reply_to.send(.{ .value = value }),
                    .stop => return,
                }
            }
        }

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const counter = try rt.spawn(Actors.counter);

    try counter.send(.{ .inc = 40 });
    try counter.send(.{ .inc = 2 });
    try counter.send(.{ .get = collector });
    try counter.send(.stop);

    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "struct actor counter" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const CounterMsg = union(enum) {
        inc: u64,
        get: Actor(Reply),
        stop,
    };

    const Actors = struct {
        const Counter = struct {
            pub const Msg = CounterMsg;

            initial: u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                var value = self.initial;

                while (true) {
                    switch (try ctx.recv()) {
                        .inc => |n| value += n,
                        .get => |reply_to| try reply_to.send(.{ .value = value }),
                        .stop => return,
                    }
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const counter = try rt.spawn(Actors.Counter{ .initial = 10 });

    try counter.send(.{ .inc = 32 });
    try counter.send(.{ .get = collector });
    try counter.send(.stop);

    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "actor can spawn child actor" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const ChildMsg = union(enum) {
        report: Actor(Reply),
    };

    const ParentMsg = union(enum) {
        start: Actor(Reply),
    };

    const Actors = struct {
        const Child = struct {
            pub const Msg = ChildMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .report => |reply_to| try reply_to.send(.{ .value = 42 }),
                }
            }
        };

        const Parent = struct {
            pub const Msg = ParentMsg;

            pub fn run(_: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |reply_to| {
                        const child = try ctx.spawn(Child{});
                        try child.send(.{ .report = reply_to });
                    },
                }
            }
        };

        const Collector = struct {
            pub const Msg = Reply;

            slot: *u64,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .value => |n| self.slot.* = n,
                }
            }
        };
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const parent = try rt.spawn(Actors.Parent{});

    try parent.send(.{ .start = collector });
    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "message sends structurally copy values but preserve references" {
    const testing = std.testing;

    const Payload = struct {
        value: u64,
        reference: *u64,
    };

    const ObserverMsg = union(enum) {
        sample: Payload,
    };

    const Observer = struct {
        pub const Msg = ObserverMsg;

        copied_value: *u64,
        referenced_value: *u64,

        pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
            switch (try ctx.recv()) {
                .sample => |payload| {
                    self.copied_value.* = payload.value;
                    self.referenced_value.* = payload.reference.*;
                },
            }
        }
    };

    var rt = try Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var referenced: u64 = 1;
    var copied_value: u64 = 0;
    var referenced_value: u64 = 0;
    const observer = try rt.spawn(Observer{
        .copied_value = &copied_value,
        .referenced_value = &referenced_value,
    });

    var payload: Payload = .{
        .value = 7,
        .reference = &referenced,
    };
    try observer.send(.{ .sample = payload });

    payload.value = 99;
    referenced = 42;

    try rt.run();

    try testing.expectEqual(@as(u64, 7), copied_value);
    try testing.expectEqual(@as(u64, 42), referenced_value);
}
