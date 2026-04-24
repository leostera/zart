//! Test helpers for actor-runtime semantics.
//!
//! This namespace is for reusable actor testing tools, not for zart's own test
//! suite. The helpers are intentionally deterministic so failing interleavings
//! can be reproduced from a seed.

const std = @import("std");
const trace = @import("runtime/trace.zig");

pub const TraceEvent = trace.TraceEvent;
pub const Tracer = trace.Tracer;

/// A deterministic scheduler-policy sample derived from a seed.
pub const Interleaving = struct {
    seed: u64,
    execution_budget: usize,
    io_budget: usize,
};

/// Produces reproducible scheduler policy variations for a seed set.
///
/// This is the small core we can grow into a fuller actor test interleaver:
/// run the same scenario across a seed corpus, vary yield/I/O budgets, and keep
/// the seed in the failure path so the schedule can be replayed.
pub const Interleaver = struct {
    options: Options = .{},

    pub const Options = struct {
        min_execution_budget: usize = 1,
        max_execution_budget: usize = 4,
        min_io_budget: usize = 1,
        max_io_budget: usize = 4,
    };

    pub fn init(options: Options) Interleaver {
        std.debug.assert(options.min_execution_budget != 0);
        std.debug.assert(options.min_io_budget != 0);
        std.debug.assert(options.min_execution_budget <= options.max_execution_budget);
        std.debug.assert(options.min_io_budget <= options.max_io_budget);
        return .{ .options = options };
    }

    pub fn case(interleaver: Interleaver, seed: u64) Interleaving {
        return .{
            .seed = seed,
            .execution_budget = bounded(seed, interleaver.options.min_execution_budget, interleaver.options.max_execution_budget),
            .io_budget = bounded(seed ^ 0x9e37_79b9_7f4a_7c15, interleaver.options.min_io_budget, interleaver.options.max_io_budget),
        };
    }
};

/// A small seed corpus with edge-ish values and high-entropy values.
pub const seeds = [_]u64{
    0x1,
    0x55,
    0x1234_5678,
    0xc001_f00d,
    0x9e37_79b9_7f4a_7c15,
    0xfeed_face_cafe_beef,
};

/// Fixed-capacity trace recorder for deterministic runtime tests.
///
/// The recorder is intentionally allocation-free. It can be installed as
/// `Runtime.Options.tracer` and dumped on test failures alongside the seed that
/// produced the failing interleaving.
pub fn TraceRecorder(comptime capacity: usize) type {
    return struct {
        seed: u64,
        events: [capacity]TraceEvent = undefined,
        len: usize = 0,
        overflow: bool = false,

        const Self = @This();

        pub fn init(seed: u64) Self {
            return .{ .seed = seed };
        }

        pub fn tracer(recorder: *Self) Tracer {
            return .{
                .context = recorder,
                .event_fn = record,
            };
        }

        pub fn slice(recorder: *const Self) []const TraceEvent {
            return recorder.events[0..recorder.len];
        }

        pub fn dump(recorder: *const Self, writer: *std.Io.Writer) !void {
            try writer.print("trace seed=0x{x} events={d} overflow={}\n", .{
                recorder.seed,
                recorder.len,
                recorder.overflow,
            });
            for (recorder.slice(), 0..) |event, index| {
                try writer.print("{d}: ", .{index});
                try writeEvent(writer, event);
                try writer.writeByte('\n');
            }
        }

        fn record(context: ?*anyopaque, event: TraceEvent) void {
            const recorder: *Self = @ptrCast(@alignCast(context.?));
            if (recorder.len == recorder.events.len) {
                recorder.overflow = true;
                return;
            }
            recorder.events[recorder.len] = event;
            recorder.len += 1;
        }
    };
}

fn bounded(seed: u64, min: usize, max: usize) usize {
    const span = max - min + 1;
    return min + @as(usize, @intCast(mix(seed) % span));
}

fn mix(seed: u64) u64 {
    var value = seed +% 0x9e37_79b9_7f4a_7c15;
    value = (value ^ (value >> 30)) *% 0xbf58_476d_1ce4_e5b9;
    value = (value ^ (value >> 27)) *% 0x94d0_49bb_1331_11eb;
    return value ^ (value >> 31);
}

fn writeEvent(writer: *std.Io.Writer, event: TraceEvent) !void {
    switch (event) {
        .actor_spawned => |actor_id| {
            try writer.writeAll("actor_spawned ");
            try writeActorId(writer, actor_id);
        },
        .actor_resumed => |actor_id| {
            try writer.writeAll("actor_resumed ");
            try writeActorId(writer, actor_id);
        },
        .actor_waiting => |actor_id| {
            try writer.writeAll("actor_waiting ");
            try writeActorId(writer, actor_id);
        },
        .actor_yielded => |actor_id| {
            try writer.writeAll("actor_yielded ");
            try writeActorId(writer, actor_id);
        },
        .actor_completed => |actor_id| {
            try writer.writeAll("actor_completed ");
            try writeActorId(writer, actor_id);
        },
        .actor_failed => |failure| {
            try writer.writeAll("actor_failed ");
            try writeActorId(writer, failure.actor);
            try writer.print(" err={s}", .{@errorName(failure.err)});
        },
        .io_submitted => |actor_id| {
            try writer.writeAll("io_submitted ");
            try writeActorId(writer, actor_id);
        },
        .io_completed => |actor_id| {
            try writer.writeAll("io_completed ");
            try writeActorId(writer, actor_id);
        },
        .message_sent => |message| {
            try writer.writeAll("message_sent from=");
            if (message.from) |from| {
                try writeActorId(writer, from);
            } else {
                try writer.writeAll("external");
            }
            try writer.writeAll(" to=");
            try writeActorId(writer, message.to);
        },
        .message_received => |actor_id| {
            try writer.writeAll("message_received ");
            try writeActorId(writer, actor_id);
        },
    }
}

fn writeActorId(writer: *std.Io.Writer, actor_id: trace.ActorId) !void {
    try writer.print("actor(index={d},generation={d})", .{ actor_id.index, actor_id.generation });
}

test "interleaver cases are deterministic and bounded" {
    const testing = std.testing;

    const interleaver = Interleaver.init(.{
        .min_execution_budget = 2,
        .max_execution_budget = 5,
        .min_io_budget = 1,
        .max_io_budget = 3,
    });

    const first = interleaver.case(0xabc);
    const second = interleaver.case(0xabc);

    try testing.expectEqual(first, second);
    try testing.expect(first.execution_budget >= 2);
    try testing.expect(first.execution_budget <= 5);
    try testing.expect(first.io_budget >= 1);
    try testing.expect(first.io_budget <= 3);
}

test "trace recorder captures and dumps replayable events" {
    const testing = std.testing;

    var recorder = TraceRecorder(2).init(0xabc);
    const tracer = recorder.tracer();

    tracer.record(.{ .actor_spawned = .{ .index = 1, .generation = 2 } });
    tracer.record(.{
        .message_sent = .{
            .from = null,
            .to = .{ .index = 1, .generation = 2 },
        },
    });
    tracer.record(.{ .actor_completed = .{ .index = 1, .generation = 2 } });

    try testing.expectEqual(@as(usize, 2), recorder.len);
    try testing.expect(recorder.overflow);

    var buffer: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try recorder.dump(&writer);

    const output = writer.buffered();
    try testing.expect(std.mem.indexOf(u8, output, "seed=0xabc") != null);
    try testing.expect(std.mem.indexOf(u8, output, "actor_spawned actor(index=1,generation=2)") != null);
    try testing.expect(std.mem.indexOf(u8, output, "message_sent from=external") != null);
}
