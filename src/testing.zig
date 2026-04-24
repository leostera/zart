//! Test helpers for actor-runtime semantics.
//!
//! This namespace is for reusable actor testing tools, not for zart's own test
//! suite. The helpers are intentionally deterministic so failing interleavings
//! can be reproduced from a seed.

const std = @import("std");

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
