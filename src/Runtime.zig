const std = @import("std");
const Fiber = @import("Fiber.zig");

const Allocator = std.mem.Allocator;

pub const ActorId = struct {
    index: usize,
    generation: u32,
};

pub const MessageTrace = struct {
    from: ?ActorId,
    to: ActorId,
};

pub const FailureTrace = struct {
    actor: ActorId,
    err: anyerror,
};

pub const TraceEvent = union(enum) {
    actor_spawned: ActorId,
    actor_resumed: ActorId,
    actor_waiting: ActorId,
    actor_yielded: ActorId,
    actor_completed: ActorId,
    actor_failed: FailureTrace,
    message_sent: MessageTrace,
    message_received: ActorId,
};

pub const Tracer = struct {
    context: ?*anyopaque = null,
    event_fn: *const fn (?*anyopaque, TraceEvent) void = noop,

    pub fn record(tracer: Tracer, event: TraceEvent) void {
        tracer.event_fn(tracer.context, event);
    }

    fn noop(_: ?*anyopaque, _: TraceEvent) void {}
};

const ActorState = enum {
    runnable,
    running,
    waiting,
    completed,
    failed,
};

const ActorHeader = struct {
    runtime: *Runtime,
    id: ActorId,
    msg_type: usize,
    state: ActorState,
    queued: bool,
    budget_remaining: usize,
    next_run: ?*ActorHeader,
    fiber: Fiber,
    stack: []align(Fiber.stack_alignment) u8,
    send_fn: *const fn (*ActorHeader, *const anyopaque) anyerror!void,
    destroy_fn: *const fn (*Runtime, *ActorHeader) void,
};

pub const Runtime = struct {
    pub const Options = struct {
        stack_size: usize = 64 * 1024,
        /// Number of explicit yield checkpoints an actor may pass per scheduler turn.
        execution_budget: usize = 64,
        internal_allocator: ?Allocator = null,
        tracer: Tracer = .{},
    };

    allocator: Allocator,
    internal_allocator: Allocator,
    options: Options,
    actors: std.ArrayList(?*ActorHeader),
    run_head: ?*ActorHeader,
    run_tail: ?*ActorHeader,
    current_actor: ?*ActorHeader,

    pub fn init(allocator: Allocator, options: Options) Runtime {
        return .{
            .allocator = allocator,
            .internal_allocator = options.internal_allocator orelse std.heap.smp_allocator,
            .options = options,
            .actors = .empty,
            .run_head = null,
            .run_tail = null,
            .current_actor = null,
        };
    }

    pub fn deinit(rt: *Runtime) void {
        for (rt.actors.items) |maybe_actor| {
            if (maybe_actor) |actor| actor.destroy_fn(rt, actor);
        }
        rt.actors.deinit(rt.internal_allocator);
        rt.* = undefined;
    }

    pub fn spawn(rt: *Runtime, actor: anytype) !Mailbox(MessageOf(@TypeOf(actor))) {
        const Actor = @TypeOf(actor);
        const Msg = MessageOf(Actor);

        return switch (@typeInfo(Actor)) {
            .@"fn" => rt.spawnFunction(Msg, actor),
            .@"struct" => rt.spawnStruct(Msg, Actor, actor),
            else => @compileError("spawn expects a function actor or a struct actor with pub const Msg and pub fn run"),
        };
    }

    pub fn send(rt: *Runtime, comptime Msg: type, actor_id: ActorId, msg: Msg) !void {
        const actor = try rt.resolve(Msg, actor_id);
        rt.trace(.{
            .message_sent = .{
                .from = if (rt.current_actor) |current| current.id else null,
                .to = actor.id,
            },
        });
        try actor.send_fn(actor, &msg);
        rt.wake(actor);
    }

    pub fn run(rt: *Runtime) !void {
        while (rt.dequeue()) |actor| {
            if (actor.state != .runnable) continue;

            actor.state = .running;
            actor.budget_remaining = rt.executionBudget();
            rt.trace(.{ .actor_resumed = actor.id });
            rt.current_actor = actor;
            const status = actor.fiber.run() catch |err| {
                rt.current_actor = null;
                return err;
            };
            rt.current_actor = null;
            switch (status) {
                .created => unreachable,
                .running => unreachable,
                .suspended => switch (actor.state) {
                    .running => {
                        actor.state = .runnable;
                        rt.enqueue(actor);
                    },
                    .runnable, .waiting => {},
                    .completed, .failed => unreachable,
                },
                .completed => {
                    actor.state = .completed;
                    rt.trace(.{ .actor_completed = actor.id });
                },
                .failed => {
                    actor.state = .failed;
                    if (actor.fiber.failure()) |err| {
                        rt.trace(.{ .actor_failed = .{ .actor = actor.id, .err = err } });
                        return err;
                    }
                },
            }
        }
    }

    fn spawnFunction(rt: *Runtime, comptime Msg: type, comptime entry: anytype) !Mailbox(Msg) {
        const Cell = FunctionActorCell(Msg, entry);
        return rt.spawnCell(Msg, Cell, {});
    }

    fn spawnStruct(rt: *Runtime, comptime Msg: type, comptime Actor: type, actor: Actor) !Mailbox(Msg) {
        const Cell = StructActorCell(Msg, Actor);
        return rt.spawnCell(Msg, Cell, actor);
    }

    fn spawnCell(rt: *Runtime, comptime Msg: type, comptime Cell: type, actor_value: anytype) !Mailbox(Msg) {
        const cell = try rt.internal_allocator.create(Cell);
        errdefer rt.internal_allocator.destroy(cell);

        const stack = try rt.internal_allocator.alignedAlloc(
            u8,
            std.mem.Alignment.fromByteUnits(Fiber.stack_alignment),
            rt.options.stack_size,
        );
        errdefer rt.internal_allocator.free(stack);

        const actor_id: ActorId = .{
            .index = rt.actors.items.len,
            .generation = 1,
        };

        cell.* = Cell.init(rt, actor_id, stack, actor_value);
        cell.header.fiber = try Fiber.init(stack, Cell.fiberEntry, cell);
        errdefer cell.header.fiber.deinit();

        try rt.actors.append(rt.internal_allocator, &cell.header);
        rt.trace(.{ .actor_spawned = actor_id });
        rt.enqueue(&cell.header);

        return .{
            .actor_id = actor_id,
            .runtime = rt,
        };
    }

    fn resolve(rt: *Runtime, comptime Msg: type, actor_id: ActorId) !*ActorHeader {
        if (actor_id.index >= rt.actors.items.len) return error.InvalidMailbox;

        const actor = rt.actors.items[actor_id.index] orelse return error.InvalidMailbox;
        if (actor.id.generation != actor_id.generation) return error.InvalidMailbox;
        if (actor.msg_type != typeId(Msg)) return error.WrongMessageType;

        return switch (actor.state) {
            .completed, .failed => error.ActorDead,
            .runnable, .running, .waiting => actor,
        };
    }

    fn wake(rt: *Runtime, actor: *ActorHeader) void {
        switch (actor.state) {
            .waiting => {
                actor.state = .runnable;
                rt.enqueue(actor);
            },
            .runnable, .running => {},
            .completed, .failed => {},
        }
    }

    fn enqueue(rt: *Runtime, actor: *ActorHeader) void {
        if (actor.queued) return;
        actor.queued = true;
        actor.next_run = null;

        if (rt.run_tail) |tail| {
            tail.next_run = actor;
        } else {
            rt.run_head = actor;
        }
        rt.run_tail = actor;
    }

    fn dequeue(rt: *Runtime) ?*ActorHeader {
        const actor = rt.run_head orelse return null;
        rt.run_head = actor.next_run;
        if (rt.run_head == null) rt.run_tail = null;

        actor.next_run = null;
        actor.queued = false;
        return actor;
    }

    fn executionBudget(rt: *const Runtime) usize {
        return @max(rt.options.execution_budget, 1);
    }

    fn trace(rt: *Runtime, event: TraceEvent) void {
        rt.options.tracer.record(event);
    }
};

pub fn Mailbox(comptime Msg: type) type {
    return struct {
        pub const Message = Msg;

        actor_id: ActorId,
        runtime: *Runtime,

        pub fn send(mailbox: @This(), msg: Msg) !void {
            try mailbox.runtime.send(Msg, mailbox.actor_id, msg);
        }
    };
}

pub fn Ctx(comptime Msg: type) type {
    return struct {
        pub const Message = Msg;

        runtime: *Runtime,
        actor: *ActorHeader,
        inbox: *Inbox(Msg),
        self_mailbox: Mailbox(Msg),

        pub fn recv(ctx: *@This()) !Msg {
            while (true) {
                if (ctx.inbox.pop()) |msg| {
                    ctx.runtime.trace(.{ .message_received = ctx.actor.id });
                    return msg;
                }

                ctx.runtime.trace(.{ .actor_waiting = ctx.actor.id });
                ctx.actor.state = .waiting;
                Fiber.yield();
            }
        }

        pub fn yield(ctx: *@This()) void {
            if (ctx.actor.budget_remaining > 1) {
                ctx.actor.budget_remaining -= 1;
                return;
            }

            ctx.actor.budget_remaining = 0;
            ctx.runtime.trace(.{ .actor_yielded = ctx.actor.id });
            ctx.actor.state = .runnable;
            ctx.runtime.enqueue(ctx.actor);
            Fiber.yield();
        }

        pub fn spawn(ctx: *@This(), actor: anytype) !Mailbox(MessageOf(@TypeOf(actor))) {
            return ctx.runtime.spawn(actor);
        }

        pub fn self(ctx: *const @This()) Mailbox(Msg) {
            return ctx.self_mailbox;
        }

        pub fn allocator(ctx: *const @This()) Allocator {
            return ctx.runtime.allocator;
        }
    };
}

pub fn MessageOf(comptime Actor: type) type {
    return switch (@typeInfo(Actor)) {
        .@"fn" => messageOfFunction(Actor),
        .@"struct" => {
            if (!@hasDecl(Actor, "Msg")) {
                @compileError("struct actor must declare pub const Msg");
            }
            if (!@hasDecl(Actor, "run")) {
                @compileError("struct actor must declare pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void");
            }
            return Actor.Msg;
        },
        else => @compileError("actor must be a function or struct"),
    };
}

fn messageOfFunction(comptime Fn: type) type {
    const fn_info = @typeInfo(Fn).@"fn";
    if (fn_info.params.len != 1) {
        @compileError("function actor must have the shape fn (*zart.Ctx(Msg)) !void");
    }

    const param_type = fn_info.params[0].type orelse {
        @compileError("function actor context parameter must be typed");
    };
    const ptr_info = switch (@typeInfo(param_type)) {
        .pointer => |pointer| pointer,
        else => @compileError("function actor first parameter must be *zart.Ctx(Msg)"),
    };

    const ctx_type = ptr_info.child;
    if (!@hasDecl(ctx_type, "Message")) {
        @compileError("function actor first parameter must be *zart.Ctx(Msg)");
    }
    return ctx_type.Message;
}

fn FunctionActorCell(comptime Msg: type, comptime entry: anytype) type {
    return struct {
        header: ActorHeader,
        inbox: Inbox(Msg),

        const Self = @This();

        fn init(rt: *Runtime, actor_id: ActorId, stack: []align(Fiber.stack_alignment) u8, _: void) Self {
            return .{
                .header = .{
                    .runtime = rt,
                    .id = actor_id,
                    .msg_type = typeId(Msg),
                    .state = .runnable,
                    .queued = false,
                    .budget_remaining = 0,
                    .next_run = null,
                    .fiber = undefined,
                    .stack = stack,
                    .send_fn = send,
                    .destroy_fn = destroy,
                },
                .inbox = .init(rt.internal_allocator),
            };
        }

        fn fiberEntry(arg: ?*anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(arg.?));
            var ctx: Ctx(Msg) = .{
                .runtime = cell.header.runtime,
                .actor = &cell.header,
                .inbox = &cell.inbox,
                .self_mailbox = .{
                    .actor_id = cell.header.id,
                    .runtime = cell.header.runtime,
                },
            };
            try entry(&ctx);
        }

        fn send(header: *ActorHeader, raw_msg: *const anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(header));
            const msg: *const Msg = @ptrCast(@alignCast(raw_msg));
            try cell.inbox.push(msg.*);
        }

        fn destroy(rt: *Runtime, header: *ActorHeader) void {
            const cell: *Self = @ptrCast(@alignCast(header));
            cell.inbox.deinit();
            cell.header.fiber.deinit();
            rt.internal_allocator.free(cell.header.stack);
            rt.internal_allocator.destroy(cell);
        }
    };
}

fn StructActorCell(comptime Msg: type, comptime Actor: type) type {
    return struct {
        header: ActorHeader,
        inbox: Inbox(Msg),
        actor: Actor,

        const Self = @This();

        fn init(rt: *Runtime, actor_id: ActorId, stack: []align(Fiber.stack_alignment) u8, actor: Actor) Self {
            return .{
                .header = .{
                    .runtime = rt,
                    .id = actor_id,
                    .msg_type = typeId(Msg),
                    .state = .runnable,
                    .queued = false,
                    .budget_remaining = 0,
                    .next_run = null,
                    .fiber = undefined,
                    .stack = stack,
                    .send_fn = send,
                    .destroy_fn = destroy,
                },
                .inbox = .init(rt.internal_allocator),
                .actor = actor,
            };
        }

        fn fiberEntry(arg: ?*anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(arg.?));
            var ctx: Ctx(Msg) = .{
                .runtime = cell.header.runtime,
                .actor = &cell.header,
                .inbox = &cell.inbox,
                .self_mailbox = .{
                    .actor_id = cell.header.id,
                    .runtime = cell.header.runtime,
                },
            };
            try cell.actor.run(&ctx);
        }

        fn send(header: *ActorHeader, raw_msg: *const anyopaque) anyerror!void {
            const cell: *Self = @ptrCast(@alignCast(header));
            const msg: *const Msg = @ptrCast(@alignCast(raw_msg));
            try cell.inbox.push(msg.*);
        }

        fn destroy(rt: *Runtime, header: *ActorHeader) void {
            const cell: *Self = @ptrCast(@alignCast(header));
            cell.inbox.deinit();
            cell.header.fiber.deinit();
            rt.internal_allocator.free(cell.header.stack);
            rt.internal_allocator.destroy(cell);
        }
    };
}

fn Inbox(comptime Msg: type) type {
    return struct {
        allocator: Allocator,
        head: ?*Node,
        tail: ?*Node,

        const Self = @This();

        const Node = struct {
            next: ?*Node,
            msg: Msg,
        };

        fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .head = null,
                .tail = null,
            };
        }

        fn deinit(inbox: *Self) void {
            while (inbox.pop()) |_| {}
        }

        fn push(inbox: *Self, msg: Msg) !void {
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

        fn pop(inbox: *Self) ?Msg {
            const node = inbox.head orelse return null;
            inbox.head = node.next;
            if (inbox.head == null) inbox.tail = null;

            const msg = node.msg;
            inbox.allocator.destroy(node);
            return msg;
        }
    };
}

fn typeId(comptime T: type) usize {
    return @intFromPtr(&TypeId(T).id);
}

fn TypeId(comptime T: type) type {
    return struct {
        const Type = T;
        var id: u8 = 0;
    };
}

test "function actor counter" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const CounterMsg = union(enum) {
        inc: u64,
        get: Mailbox(Reply),
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

    var rt = Runtime.init(testing.allocator, .{});
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
        get: Mailbox(Reply),
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

    var rt = Runtime.init(testing.allocator, .{});
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

test "yield lets another runnable actor run" {
    const testing = std.testing;

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(trace: *@This(), item: u8) void {
            trace.items[trace.len] = item;
            trace.len += 1;
        }

        fn slice(trace: *const @This()) []const u8 {
            return trace.items[0..trace.len];
        }
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const WorkerMsg = union(enum) {
        start: Mailbox(OtherMsg),
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |other| {
                        self.trace.push('a');
                        try other.send(.hit);
                        ctx.yield();
                        self.trace.push('b');
                        ctx.yield();
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('x'),
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{ .execution_budget = 1 });
    defer rt.deinit();

    var trace: Trace = .{};
    const other = try rt.spawn(Actors.Other{ .trace = &trace });
    const worker = try rt.spawn(Actors.Worker{ .trace = &trace });

    try worker.send(.{ .start = other });
    try rt.run();

    try testing.expectEqualStrings("axbc", trace.slice());
}

test "execution budget controls yield switching" {
    const testing = std.testing;

    const Trace = struct {
        items: [8]u8 = undefined,
        len: usize = 0,

        fn push(trace: *@This(), item: u8) void {
            trace.items[trace.len] = item;
            trace.len += 1;
        }

        fn slice(trace: *const @This()) []const u8 {
            return trace.items[0..trace.len];
        }
    };

    const OtherMsg = union(enum) {
        hit,
    };

    const WorkerMsg = union(enum) {
        start: Mailbox(OtherMsg),
    };

    const Actors = struct {
        const Worker = struct {
            pub const Msg = WorkerMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .start => |other| {
                        self.trace.push('a');
                        try other.send(.hit);
                        ctx.yield();
                        self.trace.push('b');
                        ctx.yield();
                        self.trace.push('c');
                    },
                }
            }
        };

        const Other = struct {
            pub const Msg = OtherMsg;

            trace: *Trace,

            pub fn run(self: *@This(), ctx: *Ctx(Msg)) !void {
                switch (try ctx.recv()) {
                    .hit => self.trace.push('x'),
                }
            }
        };
    };

    var rt = Runtime.init(testing.allocator, .{ .execution_budget = 2 });
    defer rt.deinit();

    var trace: Trace = .{};
    const other = try rt.spawn(Actors.Other{ .trace = &trace });
    const worker = try rt.spawn(Actors.Worker{ .trace = &trace });

    try worker.send(.{ .start = other });
    try rt.run();

    try testing.expectEqualStrings("abxc", trace.slice());
}

test "actor can spawn child actor" {
    const testing = std.testing;

    const Reply = union(enum) {
        value: u64,
    };

    const ChildMsg = union(enum) {
        report: Mailbox(Reply),
    };

    const ParentMsg = union(enum) {
        start: Mailbox(Reply),
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

    var rt = Runtime.init(testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Actors.Collector{ .slot = &observed });
    const parent = try rt.spawn(Actors.Parent{});

    try parent.send(.{ .start = collector });
    try rt.run();

    try testing.expectEqual(@as(u64, 42), observed);
}

test "tracer records runtime events" {
    const testing = std.testing;

    const StopMsg = union(enum) {
        stop,
    };

    const Recorder = struct {
        events: [16]TraceEvent = undefined,
        len: usize = 0,

        fn tracer(recorder: *@This()) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *@This() = @ptrCast(@alignCast(context.?));
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };

    const Actor = struct {
        pub const Msg = StopMsg;

        pub fn run(_: *@This(), ctx: *Ctx(StopMsg)) !void {
            switch (try ctx.recv()) {
                .stop => return,
            }
        }
    };

    var recorder: Recorder = .{};
    var rt = Runtime.init(testing.allocator, .{ .tracer = recorder.tracer() });
    defer rt.deinit();

    const mailbox = try rt.spawn(Actor{});
    try mailbox.send(.stop);
    try rt.run();

    try testing.expectEqual(@as(usize, 5), recorder.len);

    switch (recorder.events[0]) {
        .actor_spawned => |actor_id| try testing.expectEqual(mailbox.actor_id, actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[1]) {
        .message_sent => |message| {
            try testing.expectEqual(@as(?ActorId, null), message.from);
            try testing.expectEqual(mailbox.actor_id, message.to);
        },
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[2]) {
        .actor_resumed => |actor_id| try testing.expectEqual(mailbox.actor_id, actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[3]) {
        .message_received => |actor_id| try testing.expectEqual(mailbox.actor_id, actor_id),
        else => return error.UnexpectedTraceEvent,
    }

    switch (recorder.events[4]) {
        .actor_completed => |actor_id| try testing.expectEqual(mailbox.actor_id, actor_id),
        else => return error.UnexpectedTraceEvent,
    }
}
