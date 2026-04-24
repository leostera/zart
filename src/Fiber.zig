//! Cooperative stackful execution contexts.
//!
//! A `Fiber` owns no memory. The caller supplies stack memory to `init`, and
//! remains responsible for keeping that memory alive until the fiber is no
//! longer used.
//!
//! This primitive intentionally contains no scheduler. It only switches between
//! the current execution context and a suspended fiber.
//!
//! Suspended fibers may be migrated between OS threads on supported targets, but
//! concurrent calls to `run` on the same fiber are a scheduler bug. Zig
//! thread-local storage remains OS-thread-local; a migrated fiber observes the
//! thread-local values of the worker that runs it next.

const Fiber = @This();

const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

pub const stack_alignment: comptime_int = 16;
pub const minimum_stack_size: usize = 16 * 1024;
pub const stack_grows_down = true;

const native_os = builtin.os.tag;
const x86_64_windows = builtin.cpu.arch == .x86_64 and native_os == .windows;
const supported = switch (builtin.cpu.arch) {
    .aarch64, .x86_64 => true,
    else => false,
} and !x86_64_windows;

pub const supports_migration = supported;

pub const Status = enum {
    created,
    running,
    suspended,
    completed,
    failed,
};

pub const InitError = error{
    Unsupported,
    StackTooSmall,
};

pub const RunError = error{
    Unsupported,
    AlreadyRunning,
    Completed,
    Failed,
};

pub const Entry = *const fn (arg: ?*anyopaque) anyerror!void;

context: Context = undefined,
stack: []align(stack_alignment) u8,
entry: Entry,
arg: ?*anyopaque,
state: Status,
failure_error: ?anyerror,
resumer_context: ?*Context,
resumer_fiber: ?*Fiber,
prepared: bool,

threadlocal var current_fiber: ?*Fiber = null;

pub fn init(
    stack: []align(stack_alignment) u8,
    entry: Entry,
    arg: ?*anyopaque,
) InitError!Fiber {
    if (!supported) return error.Unsupported;
    if (stack.len < minimum_stack_size) return error.StackTooSmall;

    return .{
        .stack = stack,
        .entry = entry,
        .arg = arg,
        .state = .created,
        .failure_error = null,
        .resumer_context = null,
        .resumer_fiber = null,
        .prepared = false,
    };
}

/// Runs the fiber until it yields, completes, or fails.
pub fn run(fiber: *Fiber) RunError!Status {
    if (!supported) return error.Unsupported;

    switch (fiber.state) {
        .created, .suspended => {},
        .running => return error.AlreadyRunning,
        .completed => return error.Completed,
        .failed => return error.Failed,
    }

    if (current_fiber == fiber) return error.AlreadyRunning;

    if (!fiber.prepared) fiber.prepare();

    var caller_context: Context = undefined;
    const caller_fiber = current_fiber;
    fiber.resumer_context = &caller_context;
    fiber.resumer_fiber = caller_fiber;
    fiber.state = .running;
    current_fiber = fiber;

    switchContext(&caller_context, &fiber.context);

    fiber.resumer_context = null;
    fiber.resumer_fiber = null;

    return fiber.state;
}

/// Suspends the current fiber and returns control to the caller of `run`.
///
/// This does not unwind the stack; `defer` and `errdefer` do not run until the
/// fiber's normal control flow later exits their scopes.
pub fn yield() void {
    const fiber = current_fiber orelse @panic("Fiber.yield called outside of a fiber");
    assert(fiber.state == .running);
    const resumer_context = fiber.resumer_context orelse @panic("fiber has no resumer");

    fiber.state = .suspended;
    current_fiber = fiber.resumer_fiber;

    switchContext(&fiber.context, resumer_context);
}

pub fn current() ?*Fiber {
    return current_fiber;
}

pub fn status(fiber: *const Fiber) Status {
    return fiber.state;
}

pub fn failure(fiber: *const Fiber) ?anyerror {
    return fiber.failure_error;
}

pub fn deinit(fiber: *Fiber) void {
    assert(current_fiber != fiber);
    assert(fiber.state != .running);
    fiber.* = undefined;
}

pub fn stackUsed(fiber: *const Fiber) usize {
    if (!fiber.prepared) return 0;
    const saved_sp = fiber.context.stackPointer();
    const stack_start = @intFromPtr(fiber.stack.ptr);
    const stack_end = stack_start + fiber.stack.len;
    if (saved_sp < stack_start or saved_sp > stack_end) return 0;
    return stack_end - saved_sp;
}

fn prepare(fiber: *Fiber) void {
    assert(!fiber.prepared);
    assert(fiber.state == .created);

    const stack_start = @intFromPtr(fiber.stack.ptr);
    const stack_end = stack_start + fiber.stack.len;
    const frame_addr = std.mem.alignBackward(usize, stack_end - @sizeOf(EntryFrame), stack_alignment);
    assert(frame_addr >= stack_start);

    const frame: *EntryFrame = @ptrFromInt(frame_addr);
    frame.* = .{ .fiber = fiber };

    fiber.context = switch (builtin.cpu.arch) {
        .aarch64 => .{
            .sp = frame_addr,
            .fp = 0,
            .pc = @intFromPtr(&fiberEntry),
        },
        .x86_64 => .{
            .rsp = frame_addr - @sizeOf(usize),
            .rbp = 0,
            .rip = @intFromPtr(&fiberEntry),
        },
        else => unreachable,
    };
    fiber.prepared = true;
}

const EntryFrame = extern struct {
    fiber: *Fiber,
};

fn entryTrampoline(fiber: *Fiber) callconv(.withStackAlign(.c, stack_alignment)) noreturn {
    assert(current_fiber == fiber);
    assert(fiber.state == .running);

    fiber.entry(fiber.arg) catch |err| {
        fiber.failure_error = err;
        fiber.state = .failed;
        finish(fiber);
    };

    fiber.state = .completed;
    finish(fiber);
}

fn finish(fiber: *Fiber) noreturn {
    const resumer_context = fiber.resumer_context orelse @panic("fiber has no resumer");
    current_fiber = fiber.resumer_fiber;
    switchContext(&fiber.context, resumer_context);
    unreachable;
}

fn fiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
            \\ ldr x0, [sp]
            \\ b %[entryTrampoline]
            :
            : [entryTrampoline] "X" (&entryTrampoline),
        ),
        .x86_64 => if (x86_64_windows) {
            asm volatile (
                \\ movq 8(%%rsp), %%rcx
                \\ jmp %[entryTrampoline:P]
                :
                : [entryTrampoline] "X" (&entryTrampoline),
            );
        } else {
            asm volatile (
                \\ movq 8(%%rsp), %%rdi
                \\ jmp %[entryTrampoline:P]
                :
                : [entryTrampoline] "X" (&entryTrampoline),
            );
        },
        else => unreachable,
    }
}

const Context = if (supported) switch (builtin.cpu.arch) {
    .aarch64 => extern struct {
        sp: usize,
        fp: usize,
        pc: usize,

        fn stackPointer(context: @This()) usize {
            return context.sp;
        }
    },
    .x86_64 => extern struct {
        rsp: usize,
        rbp: usize,
        rip: usize,

        fn stackPointer(context: @This()) usize {
            return context.rsp;
        }
    },
    else => unreachable,
} else extern struct {
    fn stackPointer(_: @This()) usize {
        return 0;
    }
};

inline fn switchContext(prev: *Context, next: *Context) void {
    switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
            \\ ldr x2, [x1, #16]
            \\ mov x3, sp
            \\ stp x3, fp, [x0]
            \\ adr x4, 0f
            \\ ldp x3, fp, [x1]
            \\ str x4, [x0, #16]
            \\ mov sp, x3
            \\ br x2
            \\0:
            :
            : [prev] "{x0}" (prev),
              [next] "{x1}" (next),
            : .{
              .x0 = true,
              .x1 = true,
              .x2 = true,
              .x3 = true,
              .x4 = true,
              .x5 = true,
              .x6 = true,
              .x7 = true,
              .x8 = true,
              .x9 = true,
              .x10 = true,
              .x11 = true,
              .x12 = true,
              .x13 = true,
              .x14 = true,
              .x15 = true,
              .x16 = true,
              .x17 = true,
              .x19 = true,
              .x20 = true,
              .x21 = true,
              .x22 = true,
              .x23 = true,
              .x24 = true,
              .x25 = true,
              .x26 = true,
              .x27 = true,
              .x28 = true,
              .x30 = true,
              .z0 = true,
              .z1 = true,
              .z2 = true,
              .z3 = true,
              .z4 = true,
              .z5 = true,
              .z6 = true,
              .z7 = true,
              .z8 = true,
              .z9 = true,
              .z10 = true,
              .z11 = true,
              .z12 = true,
              .z13 = true,
              .z14 = true,
              .z15 = true,
              .z16 = true,
              .z17 = true,
              .z18 = true,
              .z19 = true,
              .z20 = true,
              .z21 = true,
              .z22 = true,
              .z23 = true,
              .z24 = true,
              .z25 = true,
              .z26 = true,
              .z27 = true,
              .z28 = true,
              .z29 = true,
              .z30 = true,
              .z31 = true,
              .p0 = true,
              .p1 = true,
              .p2 = true,
              .p3 = true,
              .p4 = true,
              .p5 = true,
              .p6 = true,
              .p7 = true,
              .p8 = true,
              .p9 = true,
              .p10 = true,
              .p11 = true,
              .p12 = true,
              .p13 = true,
              .p14 = true,
              .p15 = true,
              .fpcr = true,
              .fpsr = true,
              .ffr = true,
              .memory = true,
            }),
        .x86_64 => asm volatile (
            \\ leaq 0f(%%rip), %%rax
            \\ movq %%rsp, 0(%%rdi)
            \\ movq %%rbp, 8(%%rdi)
            \\ movq %%rax, 16(%%rdi)
            \\ movq 0(%%rsi), %%rsp
            \\ movq 8(%%rsi), %%rbp
            \\ jmpq *16(%%rsi)
            \\0:
            :
            : [prev] "{rdi}" (prev),
              [next] "{rsi}" (next),
            : .{
              .rax = true,
              .rcx = true,
              .rdx = true,
              .rbx = true,
              .rsi = true,
              .rdi = true,
              .r8 = true,
              .r9 = true,
              .r10 = true,
              .r11 = true,
              .r12 = true,
              .r13 = true,
              .r14 = true,
              .r15 = true,
              .mm0 = true,
              .mm1 = true,
              .mm2 = true,
              .mm3 = true,
              .mm4 = true,
              .mm5 = true,
              .mm6 = true,
              .mm7 = true,
              .zmm0 = true,
              .zmm1 = true,
              .zmm2 = true,
              .zmm3 = true,
              .zmm4 = true,
              .zmm5 = true,
              .zmm6 = true,
              .zmm7 = true,
              .zmm8 = true,
              .zmm9 = true,
              .zmm10 = true,
              .zmm11 = true,
              .zmm12 = true,
              .zmm13 = true,
              .zmm14 = true,
              .zmm15 = true,
              .zmm16 = true,
              .zmm17 = true,
              .zmm18 = true,
              .zmm19 = true,
              .zmm20 = true,
              .zmm21 = true,
              .zmm22 = true,
              .zmm23 = true,
              .zmm24 = true,
              .zmm25 = true,
              .zmm26 = true,
              .zmm27 = true,
              .zmm28 = true,
              .zmm29 = true,
              .zmm30 = true,
              .zmm31 = true,
              .fpsr = true,
              .fpcr = true,
              .mxcsr = true,
              .rflags = true,
              .dirflag = true,
              .memory = true,
            }),
        else => unreachable,
    }
}

test "basic run and yield" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const ContextState = struct {
        value: u32 = 0,

        fn run(arg: ?*anyopaque) anyerror!void {
            const state: *@This() = @ptrCast(@alignCast(arg.?));
            try testing.expect(current() != null);
            state.value = 1;
            yield();
            state.value = 2;
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var state: ContextState = .{};
    var fiber = try init(&stack, ContextState.run, &state);
    defer fiber.deinit();

    try testing.expectEqual(Status.created, fiber.status());
    try testing.expectEqual(Status.suspended, try fiber.run());
    try testing.expectEqual(@as(u32, 1), state.value);
    try testing.expect(fiber.stackUsed() > 0);
    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expectEqual(@as(u32, 2), state.value);
    try testing.expectEqual(null, fiber.failure());
    try testing.expectError(error.Completed, fiber.run());
}

test "entry error is observable" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const Expected = error{Expected};

    const S = struct {
        fn run(_: ?*anyopaque) anyerror!void {
            return Expected.Expected;
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var fiber = try init(&stack, S.run, null);
    defer fiber.deinit();

    try testing.expectEqual(Status.failed, try fiber.run());
    try testing.expectEqual(Expected.Expected, fiber.failure().?);
    try testing.expectError(error.Failed, fiber.run());
}

test "defers run when entry returns" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const S = struct {
        fn run(arg: ?*anyopaque) anyerror!void {
            const value: *u32 = @ptrCast(@alignCast(arg.?));
            defer value.* += 1;
            value.* += 1;
            yield();
            value.* += 1;
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var value: u32 = 0;
    var fiber = try init(&stack, S.run, &value);
    defer fiber.deinit();

    try testing.expectEqual(Status.suspended, try fiber.run());
    try testing.expectEqual(@as(u32, 1), value);
    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expectEqual(@as(u32, 3), value);
}
