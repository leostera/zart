//! Cooperative stackful execution contexts.
//!
//! A `Fiber` owns no memory. The caller supplies stack memory to `init`, and
//! remains responsible for keeping that memory alive until the fiber is no
//! longer used.
//!
//! This primitive intentionally contains no scheduler. It only switches between
//! the current execution context and a suspended fiber.
//!
//! A suspended fiber may be resumed by another OS thread only when the scheduler
//! provides external synchronization and fiber code does not keep thread-local
//! references, OS-thread-affine resources, or platform TLS-derived pointers live
//! across yield points.
//!
//! Limitations:
//! - No ASan/TSan fiber annotations yet.
//! - No Valgrind stack registration yet.
//! - No reliable unwinding/backtraces across fiber boundaries.
//! - No CET/shadow-stack integration on x86_64.
//! - No C++ exception or `longjmp` crossing fiber boundaries.
//! - No guard-page owning stack helper yet; raw stack overflow corrupts memory.
//! - No WebAssembly backend yet. Wasm needs an Asyncify/continuation backend,
//!   not this native stack-pointer/instruction-pointer implementation.

const Fiber = @This();

const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

fn expectOffset(comptime T: type, comptime field: []const u8, comptime expected: usize) void {
    if (@offsetOf(T, field) != expected) @compileError("fiber assembly layout offset changed");
}

fn expectSize(comptime T: type, comptime expected: usize) void {
    if (@sizeOf(T) != expected) @compileError("fiber assembly layout size changed");
}

pub const stack_alignment: comptime_int = 16;
pub const minimum_stack_size: usize = 16 * 1024;
pub const stack_grows_down = true;

const native_os = builtin.os.tag;
const is_x86_64_sysv = builtin.cpu.arch == .x86_64 and switch (native_os) {
    .linux, .macos, .freebsd, .openbsd, .netbsd, .dragonfly, .solaris => true,
    else => false,
};
const is_aarch64_aapcs64_basic = builtin.cpu.arch == .aarch64 and switch (native_os) {
    .linux, .macos, .freebsd, .openbsd, .netbsd => true,
    else => false,
};

pub const Backend = enum {
    x86_64_sysv,
    aarch64_aapcs64_basic,
    wasm_emscripten_asyncify,
    unsupported,
};

pub const backend: Backend = blk: {
    if (is_x86_64_sysv) break :blk .x86_64_sysv;
    if (is_aarch64_aapcs64_basic) break :blk .aarch64_aapcs64_basic;
    break :blk .unsupported;
};

pub const supported = backend != .unsupported;

/// This fiber primitive does not preserve optional AArch64 SVE state.
pub const supports_sve = false;
/// This fiber primitive does not preserve optional AArch64 SME state.
pub const supports_sme = false;
pub const can_resume_on_different_thread_with_external_sync = switch (backend) {
    .x86_64_sysv, .aarch64_aapcs64_basic => true,
    .wasm_emscripten_asyncify, .unsupported => false,
};
pub const supports_migration = can_resume_on_different_thread_with_external_sync;

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

const X86FpControl = extern struct {
    mxcsr: u32 = 0x1f80,
    x87_cw: u16 = 0x037f,
    _pad: u16 = 0,
};

context: Context = undefined,
self: *Fiber,
stack: []align(stack_alignment) u8,
entry: Entry,
arg: ?*anyopaque,
state: Status,
failure_error: ?anyerror,
resumer_context: ?*Context,
resumer_fiber: ?*Fiber,
prepared: bool,

threadlocal var current_fiber: ?*Fiber = null;

/// Initializes a fiber in-place. After this returns, the `Fiber` object must
/// stay at the same address until `deinit` or `abandonWithoutUnwind`.
pub fn init(
    fiber: *Fiber,
    stack: []align(stack_alignment) u8,
    entry: Entry,
    arg: ?*anyopaque,
) InitError!void {
    if (!supported) return error.Unsupported;
    if (stack.len < minimum_stack_size) return error.StackTooSmall;

    fiber.* = .{
        .self = fiber,
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
    fiber.checkPinned();

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
    fiber.checkPinned();
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
    fiber.checkPinned();
    return fiber.state;
}

pub fn failure(fiber: *const Fiber) ?anyerror {
    fiber.checkPinned();
    return fiber.failure_error;
}

pub fn deinit(fiber: *Fiber) void {
    fiber.checkPinned();
    assert(current_fiber != fiber);
    assert(fiber.state == .created or fiber.state == .completed or fiber.state == .failed);
    fiber.debugInvalidate();
}

/// Discards a suspended fiber without unwinding its stack.
///
/// This is intentionally separate from `deinit` because it does not run defers,
/// release locks, or clean up borrowed state held by the suspended call stack.
/// Only use this when the owner can prove abandoning that stack is safe.
pub fn abandonWithoutUnwind(fiber: *Fiber) void {
    fiber.checkPinned();
    assert(current_fiber != fiber);
    assert(fiber.state == .suspended);
    fiber.debugInvalidate();
}

fn debugInvalidate(fiber: *Fiber) void {
    if (builtin.mode == .Debug) fiber.* = undefined;
}

fn checkPinned(fiber: *const Fiber) void {
    if (@intFromPtr(fiber.self) != @intFromPtr(fiber)) {
        @panic("Fiber object was moved or copied after initialization");
    }
}

/// Returns the distance from the top of the stack to the last saved stack
/// pointer. While the fiber is running the saved stack pointer is stale.
pub fn savedStackDepth(fiber: *const Fiber) usize {
    fiber.checkPinned();
    if (!fiber.prepared) return 0;
    const saved_sp = fiber.context.stackPointer();
    const stack_start = @intFromPtr(fiber.stack.ptr);
    const stack_end = stack_start + fiber.stack.len;
    if (saved_sp < stack_start or saved_sp > stack_end) return 0;
    return stack_end - saved_sp;
}

pub fn stackUsed(fiber: *const Fiber) usize {
    return fiber.savedStackDepth();
}

fn stackSlot(fiber: *Fiber, comptime T: type, addr: usize) *T {
    const stack_start = @intFromPtr(fiber.stack.ptr);
    assert(addr >= stack_start);
    const offset = addr - stack_start;
    assert(offset + @sizeOf(T) <= fiber.stack.len);
    return @ptrCast(@alignCast(fiber.stack[offset..].ptr));
}

fn prepare(fiber: *Fiber) void {
    assert(!fiber.prepared);
    assert(fiber.state == .created);

    const stack_start = @intFromPtr(fiber.stack.ptr);
    const stack_end = stack_start + fiber.stack.len;

    // Fiber stacks grow downward on both currently supported ABIs. The initial
    // stack contains only an `EntryFrame`, which gives the naked entry stub the
    // `Fiber` pointer it must pass to `entryTrampoline`.
    //
    // AArch64 entry layout:
    //
    //   sp -> EntryFrame{ .fiber = fiber }
    //
    // x86_64 SysV entry layout:
    //
    //   rsp + 0 -> zero fake return address
    //   rsp + 8 -> EntryFrame{ .fiber = fiber }
    //
    // x86_64 gets a fake return slot because normal SysV functions are entered
    // by `call`, and therefore see a return address at `(%rsp)`. We enter via
    // `jmp`, so the slot is a deliberate sentinel instead of stack garbage.
    const frame_addr = std.mem.alignBackward(usize, stack_end - @sizeOf(EntryFrame), stack_alignment);
    assert(frame_addr >= stack_start);

    const frame = fiber.stackSlot(EntryFrame, frame_addr);
    frame.* = .{ .fiber = fiber };

    if (comptime is_aarch64_aapcs64_basic) {
        fiber.context = .{
            .sp = frame_addr,
            .pc = @intFromPtr(&fiberEntryAarch64Aapcs64Basic),
            .x19 = 0,
            .x20 = 0,
            .x21 = 0,
            .x22 = 0,
            .x23 = 0,
            .x24 = 0,
            .x25 = 0,
            .x26 = 0,
            .x27 = 0,
            .x28 = 0,
            .fp = 0,
            .lr = 0,
            .d8 = 0,
            .d9 = 0,
            .d10 = 0,
            .d11 = 0,
            .d12 = 0,
            .d13 = 0,
            .d14 = 0,
            .d15 = 0,
        };
    } else if (comptime is_x86_64_sysv) {
        const ret_slot_addr = frame_addr - @sizeOf(usize);
        assert(ret_slot_addr >= stack_start);
        const ret_slot = fiber.stackSlot(usize, ret_slot_addr);
        ret_slot.* = 0;

        const fp_control = captureX86FpControl();
        fiber.context = .{
            .rsp = ret_slot_addr,
            .rip = @intFromPtr(&fiberEntryX86_64SysV),
            .rbx = 0,
            .rbp = 0,
            .r12 = 0,
            .r13 = 0,
            .r14 = 0,
            .r15 = 0,
            .mxcsr = fp_control.mxcsr,
            .x87_cw = fp_control.x87_cw,
            ._pad = 0,
        };
    } else unreachable;

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

// First instruction executed on a freshly prepared AArch64 fiber stack. This is
// naked because there is no caller-created frame yet; any compiler prologue
// would use a stack/register state that only our assembly has defined.
//
// AAPCS64 passes the first argument in `x0`. `sp` points directly at
// `EntryFrame`, so `[sp]` is the stored `Fiber *`. `x30` is cleared so an
// accidental return from the trampoline traps instead of jumping to garbage.
fn fiberEntryAarch64Aapcs64Basic() callconv(.naked) noreturn {
    if (comptime is_aarch64_aapcs64_basic) {
        asm volatile (
            \\ ldr x0, [sp]
            \\ mov x30, xzr
            \\ b %[entryTrampoline]
            :
            : [entryTrampoline] "X" (&entryTrampoline),
        );
    } else unreachable;
}

// First instruction executed on a freshly prepared x86_64 SysV fiber stack.
// `(%rsp)` is the fake return address, so `8(%rsp)` is the stored `Fiber *`.
// SysV passes the first argument in `rdi`.
fn fiberEntryX86_64SysV() callconv(.naked) noreturn {
    if (comptime is_x86_64_sysv) {
        asm volatile (
            \\ movq 8(%%rsp), %%rdi
            \\ jmp %[entryTrampoline:P]
            :
            : [entryTrampoline] "X" (&entryTrampoline),
        );
    } else unreachable;
}

const Context = if (supported) switch (builtin.cpu.arch) {
    .aarch64 => extern struct {
        // Offsets are part of the hand-written assembly contract:
        //   0: sp, 8: pc, 16..88: x19..x28, 96: fp/x29, 104: lr/x30,
        //   112..168: low 64-bit lanes of d8..d15.
        //
        // AAPCS64 requires x19-x29 and SP to be preserved across calls. The
        // low 64 bits of v8-v15 are also preserved; storing `d8`-`d15` captures
        // exactly that required part. Optional SVE/SME state is intentionally
        // not supported by this primitive.
        sp: usize,
        pc: usize,
        x19: usize,
        x20: usize,
        x21: usize,
        x22: usize,
        x23: usize,
        x24: usize,
        x25: usize,
        x26: usize,
        x27: usize,
        x28: usize,
        fp: usize,
        lr: usize,
        d8: u64,
        d9: u64,
        d10: u64,
        d11: u64,
        d12: u64,
        d13: u64,
        d14: u64,
        d15: u64,

        fn stackPointer(context: @This()) usize {
            return context.sp;
        }
    },
    .x86_64 => extern struct {
        // Offsets are part of the hand-written assembly contract:
        //   0: rsp, 8: rip, 16: rbx, 24: rbp, 32..56: r12..r15,
        //   64: MXCSR, 68: x87 control word.
        //
        // Under the SysV ABI these GPRs are callee-saved. MXCSR status bits
        // are caller-saved, but MXCSR control bits and the x87 control word are
        // callee-saved. `stmxcsr`/`ldmxcsr` save and restore the whole MXCSR
        // word because the instruction does not expose a control-bit-only form.
        rsp: usize,
        rip: usize,
        rbx: usize,
        rbp: usize,
        r12: usize,
        r13: usize,
        r14: usize,
        r15: usize,
        mxcsr: u32,
        x87_cw: u16,
        _pad: u16,

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

fn verifyContextLayout() void {
    if (comptime is_aarch64_aapcs64_basic) {
        expectOffset(Context, "sp", 0);
        expectOffset(Context, "pc", 8);
        expectOffset(Context, "x19", 16);
        expectOffset(Context, "x20", 24);
        expectOffset(Context, "x21", 32);
        expectOffset(Context, "x22", 40);
        expectOffset(Context, "x23", 48);
        expectOffset(Context, "x24", 56);
        expectOffset(Context, "x25", 64);
        expectOffset(Context, "x26", 72);
        expectOffset(Context, "x27", 80);
        expectOffset(Context, "x28", 88);
        expectOffset(Context, "fp", 96);
        expectOffset(Context, "lr", 104);
        expectOffset(Context, "d8", 112);
        expectOffset(Context, "d9", 120);
        expectOffset(Context, "d10", 128);
        expectOffset(Context, "d11", 136);
        expectOffset(Context, "d12", 144);
        expectOffset(Context, "d13", 152);
        expectOffset(Context, "d14", 160);
        expectOffset(Context, "d15", 168);
        expectSize(Context, 176);
    } else if (comptime is_x86_64_sysv) {
        expectOffset(Context, "rsp", 0);
        expectOffset(Context, "rip", 8);
        expectOffset(Context, "rbx", 16);
        expectOffset(Context, "rbp", 24);
        expectOffset(Context, "r12", 32);
        expectOffset(Context, "r13", 40);
        expectOffset(Context, "r14", 48);
        expectOffset(Context, "r15", 56);
        expectOffset(Context, "mxcsr", 64);
        expectOffset(Context, "x87_cw", 68);
        expectOffset(Context, "_pad", 70);
        expectSize(Context, 72);
    }
}

extern fn zart_fiber_capture_x86_fp_control(out: *X86FpControl) callconv(.c) void;

extern fn zart_fiber_switch(prev: *Context, next: *const Context) callconv(.c) void;

comptime {
    if (supported) {
        verifyContextLayout();

        const prefix = if (builtin.object_format == .macho) "_" else "";
        const symbol = prefix ++ "zart_fiber_switch";

        if (is_aarch64_aapcs64_basic) {
            // C ABI arguments:
            //   x0 = prev context
            //   x1 = next context
            //
            // Save the current continuation into `prev`:
            //   sp is the current stack pointer.
            //   pc is the local resume label `1`.
            //   x19-x30 and d8-d15 are the AAPCS64 preserved register set.
            //
            // Restore `next` in the opposite order, install its stack pointer,
            // and branch to its saved pc. When this saved context is resumed in
            // the future, execution continues at label `1` and `ret` returns to
            // the original caller of `zart_fiber_switch`.
            asm (".text\n" ++
                    ".globl " ++ symbol ++ "\n" ++
                    symbol ++ ":\n" ++
                    "    mov x9, sp\n" ++
                    "    str x9, [x0, #0]\n" ++
                    "    adr x9, 1f\n" ++
                    "    str x9, [x0, #8]\n" ++
                    "    stp x19, x20, [x0, #16]\n" ++
                    "    stp x21, x22, [x0, #32]\n" ++
                    "    stp x23, x24, [x0, #48]\n" ++
                    "    stp x25, x26, [x0, #64]\n" ++
                    "    stp x27, x28, [x0, #80]\n" ++
                    "    stp x29, x30, [x0, #96]\n" ++
                    "    stp d8, d9, [x0, #112]\n" ++
                    "    stp d10, d11, [x0, #128]\n" ++
                    "    stp d12, d13, [x0, #144]\n" ++
                    "    stp d14, d15, [x0, #160]\n" ++
                    "    ldr x9, [x1, #0]\n" ++
                    "    ldr x10, [x1, #8]\n" ++
                    "    ldp x19, x20, [x1, #16]\n" ++
                    "    ldp x21, x22, [x1, #32]\n" ++
                    "    ldp x23, x24, [x1, #48]\n" ++
                    "    ldp x25, x26, [x1, #64]\n" ++
                    "    ldp x27, x28, [x1, #80]\n" ++
                    "    ldp x29, x30, [x1, #96]\n" ++
                    "    ldp d8, d9, [x1, #112]\n" ++
                    "    ldp d10, d11, [x1, #128]\n" ++
                    "    ldp d12, d13, [x1, #144]\n" ++
                    "    ldp d14, d15, [x1, #160]\n" ++
                    "    mov sp, x9\n" ++
                    "    br x10\n" ++
                    "1:\n" ++
                    "    ret\n");
        } else if (is_x86_64_sysv) {
            // C ABI arguments:
            //   rdi = prev context
            //   rsi = next context
            //
            // Save the current continuation into `prev`:
            //   rsp is the current stack pointer.
            //   rip is the local resume label `1`.
            //   rbx/rbp/r12-r15 are the SysV callee-saved GPRs.
            //
            // Restore `next`, switch stacks, and jump to its saved rip. When
            // this saved context is resumed later, label `1` executes `retq`,
            // returning to the original caller of `zart_fiber_switch`.
            asm (".text\n" ++
                    ".globl " ++ symbol ++ "\n" ++
                    symbol ++ ":\n" ++
                    "    leaq 1f(%rip), %rax\n" ++
                    "    movq %rsp, 0(%rdi)\n" ++
                    "    movq %rax, 8(%rdi)\n" ++
                    "    movq %rbx, 16(%rdi)\n" ++
                    "    movq %rbp, 24(%rdi)\n" ++
                    "    movq %r12, 32(%rdi)\n" ++
                    "    movq %r13, 40(%rdi)\n" ++
                    "    movq %r14, 48(%rdi)\n" ++
                    "    movq %r15, 56(%rdi)\n" ++
                    "    stmxcsr 64(%rdi)\n" ++
                    "    fnstcw 68(%rdi)\n" ++
                    "    movq 16(%rsi), %rbx\n" ++
                    "    movq 24(%rsi), %rbp\n" ++
                    "    movq 32(%rsi), %r12\n" ++
                    "    movq 40(%rsi), %r13\n" ++
                    "    movq 48(%rsi), %r14\n" ++
                    "    movq 56(%rsi), %r15\n" ++
                    "    ldmxcsr 64(%rsi)\n" ++
                    "    fldcw 68(%rsi)\n" ++
                    "    movq 0(%rsi), %rsp\n" ++
                    "    jmpq *8(%rsi)\n" ++
                    "1:\n" ++
                    "    retq\n");
        }
    }
}

comptime {
    if (is_x86_64_sysv) {
        const prefix = if (builtin.object_format == .macho) "_" else "";
        const symbol = prefix ++ "zart_fiber_capture_x86_fp_control";

        asm (".text\n" ++
                ".globl " ++ symbol ++ "\n" ++
                symbol ++ ":\n" ++
                "    stmxcsr 0(%rdi)\n" ++
                "    fnstcw 4(%rdi)\n" ++
                "    movw $0, 6(%rdi)\n" ++
                "    retq\n");
    }
}

fn captureX86FpControl() X86FpControl {
    if (comptime is_x86_64_sysv) {
        var control: X86FpControl = .{};
        zart_fiber_capture_x86_fp_control(&control);
        return control;
    } else {
        return .{};
    }
}

fn switchContext(prev: *Context, next: *const Context) void {
    if (comptime supported) {
        zart_fiber_switch(prev, next);
    } else {
        unreachable;
    }
}

const preserved_register_test_count = if (is_aarch64_aapcs64_basic) 18 else if (is_x86_64_sysv) 5 else 0;

const PreservedRegisterProbe = extern struct {
    // Test-only assembly treats this as two adjacent arrays. `observed` starts
    // immediately after `expected`, so the observed offset is:
    //   AArch64: 18 registers * 8 bytes = 144
    //   x86_64:  5 registers * 8 bytes = 40
    expected: [preserved_register_test_count]usize,
    observed: [preserved_register_test_count]usize = undefined,
};

fn verifyPreservedRegisterProbeLayout() void {
    if (comptime is_aarch64_aapcs64_basic) {
        expectOffset(PreservedRegisterProbe, "expected", 0);
        expectOffset(PreservedRegisterProbe, "observed", 144);
        expectSize(PreservedRegisterProbe, 288);
    } else if (comptime is_x86_64_sysv) {
        expectOffset(PreservedRegisterProbe, "expected", 0);
        expectOffset(PreservedRegisterProbe, "observed", 40);
        expectSize(PreservedRegisterProbe, 80);
    }
}

extern fn zart_fiber_probe_preserved_registers(
    probe: *PreservedRegisterProbe,
    yield_fn: *const fn () callconv(.c) void,
) callconv(.c) void;

comptime {
    if (supported and builtin.is_test) {
        verifyPreservedRegisterProbeLayout();

        const prefix = if (builtin.object_format == .macho) "_" else "";
        const symbol = prefix ++ "zart_fiber_probe_preserved_registers";

        if (is_aarch64_aapcs64_basic) {
            // Test-only register preservation probe.
            //
            // C ABI arguments:
            //   x0 = PreservedRegisterProbe *
            //   x1 = C-callable yield function
            //
            // Local stack frame:
            //   0..95    original x19-x30, restored before returning
            //   96       saved probe pointer, because x0 is caller-saved
            //   112..175 original d8-d15, restored before returning
            //   176..191 padding to keep `sp` 16-byte aligned
            //
            // The probe loads expected values into the preserved registers,
            // calls `Fiber.yield`, records the registers after resume, then
            // restores its caller's preserved registers before returning.
            asm (".text\n" ++
                    ".globl " ++ symbol ++ "\n" ++
                    symbol ++ ":\n" ++
                    "    sub sp, sp, #192\n" ++
                    "    stp x19, x20, [sp, #0]\n" ++
                    "    stp x21, x22, [sp, #16]\n" ++
                    "    stp x23, x24, [sp, #32]\n" ++
                    "    stp x25, x26, [sp, #48]\n" ++
                    "    stp x27, x28, [sp, #64]\n" ++
                    "    stp x29, x30, [sp, #80]\n" ++
                    "    str x0, [sp, #96]\n" ++
                    "    stp d8, d9, [sp, #112]\n" ++
                    "    stp d10, d11, [sp, #128]\n" ++
                    "    stp d12, d13, [sp, #144]\n" ++
                    "    stp d14, d15, [sp, #160]\n" ++
                    "    ldr x19, [x0, #0]\n" ++
                    "    ldr x20, [x0, #8]\n" ++
                    "    ldr x21, [x0, #16]\n" ++
                    "    ldr x22, [x0, #24]\n" ++
                    "    ldr x23, [x0, #32]\n" ++
                    "    ldr x24, [x0, #40]\n" ++
                    "    ldr x25, [x0, #48]\n" ++
                    "    ldr x26, [x0, #56]\n" ++
                    "    ldr x27, [x0, #64]\n" ++
                    "    ldr x28, [x0, #72]\n" ++
                    "    ldp d8, d9, [x0, #80]\n" ++
                    "    ldp d10, d11, [x0, #96]\n" ++
                    "    ldp d12, d13, [x0, #112]\n" ++
                    "    ldp d14, d15, [x0, #128]\n" ++
                    "    blr x1\n" ++
                    "    ldr x9, [sp, #96]\n" ++
                    "    str x19, [x9, #144]\n" ++
                    "    str x20, [x9, #152]\n" ++
                    "    str x21, [x9, #160]\n" ++
                    "    str x22, [x9, #168]\n" ++
                    "    str x23, [x9, #176]\n" ++
                    "    str x24, [x9, #184]\n" ++
                    "    str x25, [x9, #192]\n" ++
                    "    str x26, [x9, #200]\n" ++
                    "    str x27, [x9, #208]\n" ++
                    "    str x28, [x9, #216]\n" ++
                    "    stp d8, d9, [x9, #224]\n" ++
                    "    stp d10, d11, [x9, #240]\n" ++
                    "    stp d12, d13, [x9, #256]\n" ++
                    "    stp d14, d15, [x9, #272]\n" ++
                    "    ldp x19, x20, [sp, #0]\n" ++
                    "    ldp x21, x22, [sp, #16]\n" ++
                    "    ldp x23, x24, [sp, #32]\n" ++
                    "    ldp x25, x26, [sp, #48]\n" ++
                    "    ldp x27, x28, [sp, #64]\n" ++
                    "    ldp x29, x30, [sp, #80]\n" ++
                    "    ldp d8, d9, [sp, #112]\n" ++
                    "    ldp d10, d11, [sp, #128]\n" ++
                    "    ldp d12, d13, [sp, #144]\n" ++
                    "    ldp d14, d15, [sp, #160]\n" ++
                    "    add sp, sp, #192\n" ++
                    "    ret\n");
        } else if (is_x86_64_sysv) {
            // Test-only register preservation probe.
            //
            // C ABI arguments:
            //   rdi = PreservedRegisterProbe *
            //   rsi = C-callable yield function
            //
            // The five pushes preserve the caller's SysV callee-saved GPRs.
            // The extra 16-byte allocation keeps `%rsp` 16-byte aligned before
            // `callq *%rsi` and provides a spill slot for the probe pointer.
            asm (".text\n" ++
                    ".globl " ++ symbol ++ "\n" ++
                    symbol ++ ":\n" ++
                    "    pushq %rbx\n" ++
                    "    pushq %r12\n" ++
                    "    pushq %r13\n" ++
                    "    pushq %r14\n" ++
                    "    pushq %r15\n" ++
                    "    subq $16, %rsp\n" ++
                    "    movq %rdi, 0(%rsp)\n" ++
                    "    movq 0(%rdi), %rbx\n" ++
                    "    movq 8(%rdi), %r12\n" ++
                    "    movq 16(%rdi), %r13\n" ++
                    "    movq 24(%rdi), %r14\n" ++
                    "    movq 32(%rdi), %r15\n" ++
                    "    callq *%rsi\n" ++
                    "    movq 0(%rsp), %rax\n" ++
                    "    movq %rbx, 40(%rax)\n" ++
                    "    movq %r12, 48(%rax)\n" ++
                    "    movq %r13, 56(%rax)\n" ++
                    "    movq %r14, 64(%rax)\n" ++
                    "    movq %r15, 72(%rax)\n" ++
                    "    addq $16, %rsp\n" ++
                    "    popq %r15\n" ++
                    "    popq %r14\n" ++
                    "    popq %r13\n" ++
                    "    popq %r12\n" ++
                    "    popq %rbx\n" ++
                    "    retq\n");
        }
    }
}

fn yieldForRegisterProbe() callconv(.c) void {
    yield();
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
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, ContextState.run, &state);
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
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, null);
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
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, &value);
    defer fiber.deinit();

    try testing.expectEqual(Status.suspended, try fiber.run());
    try testing.expectEqual(@as(u32, 1), value);
    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expectEqual(@as(u32, 3), value);
}

test "multiple yields preserve control flow" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const S = struct {
        fn run(arg: ?*anyopaque) anyerror!void {
            const value: *usize = @ptrCast(@alignCast(arg.?));
            for (0..5) |index| {
                value.* = index + 1;
                yield();
            }
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var value: usize = 0;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, &value);
    defer fiber.deinit();

    for (0..5) |index| {
        try testing.expectEqual(Status.suspended, try fiber.run());
        try testing.expectEqual(index + 1, value);
        try testing.expect(fiber.savedStackDepth() > 0);
    }
    try testing.expectEqual(Status.completed, try fiber.run());
}

test "nested fibers resume their direct resumer" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const Child = struct {
        fn run(arg: ?*anyopaque) anyerror!void {
            const log: *std.ArrayListUnmanaged(u8) = @ptrCast(@alignCast(arg.?));
            try log.append(testing.allocator, 'b');
            yield();
            try log.append(testing.allocator, 'd');
        }
    };
    const ParentState = struct {
        child: *Fiber,
        log: *std.ArrayListUnmanaged(u8),

        fn run(arg: ?*anyopaque) anyerror!void {
            const state: *@This() = @ptrCast(@alignCast(arg.?));
            try state.log.append(testing.allocator, 'a');
            try testing.expectEqual(Status.suspended, try state.child.run());
            try state.log.append(testing.allocator, 'c');
            yield();
            try testing.expectEqual(Status.completed, try state.child.run());
            try state.log.append(testing.allocator, 'e');
        }
    };

    var log: std.ArrayListUnmanaged(u8) = .empty;
    defer log.deinit(testing.allocator);

    var child_stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var child: Fiber = undefined;
    try init(&child, &child_stack, Child.run, &log);
    defer child.deinit();

    var parent_stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var parent_state: ParentState = .{ .child = &child, .log = &log };
    var parent: Fiber = undefined;
    try init(&parent, &parent_stack, ParentState.run, &parent_state);
    defer parent.deinit();

    try testing.expectEqual(Status.suspended, try parent.run());
    try testing.expectEqualStrings("abc", log.items);
    try testing.expectEqual(Status.completed, try parent.run());
    try testing.expectEqualStrings("abcde", log.items);
}

test "entry error after yield is observable" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const Expected = error{Expected};
    const S = struct {
        fn run(_: ?*anyopaque) anyerror!void {
            yield();
            return Expected.Expected;
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, null);
    defer fiber.deinit();

    try testing.expectEqual(Status.suspended, try fiber.run());
    try testing.expectEqual(Status.failed, try fiber.run());
    try testing.expectEqual(Expected.Expected, fiber.failure().?);
    try testing.expectError(error.Failed, fiber.run());
}

test "suspended fiber abandonment is explicit" {
    if (!supported) return error.SkipZigTest;

    const S = struct {
        fn run(_: ?*anyopaque) anyerror!void {
            yield();
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, null);

    try std.testing.expectEqual(Status.suspended, try fiber.run());
    fiber.abandonWithoutUnwind();
}

test "reentrant run of current fiber is rejected" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const S = struct {
        fn run(arg: ?*anyopaque) anyerror!void {
            const observed: *bool = @ptrCast(@alignCast(arg.?));
            const self = current().?;
            try testing.expectError(error.AlreadyRunning, self.run());
            observed.* = true;
        }
    };

    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var observed = false;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, &observed);
    defer fiber.deinit();

    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expect(observed);
}

test "stack size boundaries are enforced" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;

    var too_small: [minimum_stack_size - stack_alignment]u8 align(stack_alignment) = undefined;
    var rejected: Fiber = undefined;
    try testing.expectError(error.StackTooSmall, init(&rejected, &too_small, struct {
        fn run(_: ?*anyopaque) anyerror!void {}
    }.run, null));

    var boundary: [minimum_stack_size]u8 align(stack_alignment) = undefined;
    var fiber: Fiber = undefined;
    try init(&fiber, &boundary, struct {
        fn run(_: ?*anyopaque) anyerror!void {}
    }.run, null);
    defer fiber.deinit();

    try testing.expectEqual(@as(usize, 0), fiber.savedStackDepth());
    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expect(fiber.savedStackDepth() > 0);
}

test "fiber records its pinned address" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;

    var stack: [minimum_stack_size]u8 align(stack_alignment) = undefined;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, struct {
        fn run(_: ?*anyopaque) anyerror!void {}
    }.run, null);
    defer fiber.deinit();

    try testing.expectEqual(@intFromPtr(&fiber), @intFromPtr(fiber.self));

    const copied = fiber;
    try testing.expect(@intFromPtr(&copied) != @intFromPtr(copied.self));
}

test "callee-saved general registers survive yield" {
    if (!supported) return error.SkipZigTest;

    const testing = std.testing;
    const S = struct {
        probe: PreservedRegisterProbe,

        fn run(arg: ?*anyopaque) anyerror!void {
            const state: *@This() = @ptrCast(@alignCast(arg.?));
            zart_fiber_probe_preserved_registers(&state.probe, yieldForRegisterProbe);
        }
    };

    var state: S = .{
        .probe = .{
            .expected = if (comptime is_aarch64_aapcs64_basic)
                .{
                    0x1919_1919_1919_1919,
                    0x2020_2020_2020_2020,
                    0x2121_2121_2121_2121,
                    0x2222_2222_2222_2222,
                    0x2323_2323_2323_2323,
                    0x2424_2424_2424_2424,
                    0x2525_2525_2525_2525,
                    0x2626_2626_2626_2626,
                    0x2727_2727_2727_2727,
                    0x2828_2828_2828_2828,
                    0xd8d8_d8d8_d8d8_d8d8,
                    0xd9d9_d9d9_d9d9_d9d9,
                    0xdada_dada_dada_dada,
                    0xdbdb_dbdb_dbdb_dbdb,
                    0xdcdc_dcdc_dcdc_dcdc,
                    0xdddd_dddd_dddd_dddd,
                    0xdede_dede_dede_dede,
                    0xdfdf_dfdf_dfdf_dfdf,
                }
            else
                .{
                    0x0b0b_0b0b_0b0b_0b0b,
                    0x1212_1212_1212_1212,
                    0x1313_1313_1313_1313,
                    0x1414_1414_1414_1414,
                    0x1515_1515_1515_1515,
                },
        },
    };
    var stack: [64 * 1024]u8 align(stack_alignment) = undefined;
    var fiber: Fiber = undefined;
    try init(&fiber, &stack, S.run, &state);
    defer fiber.deinit();

    try testing.expectEqual(Status.suspended, try fiber.run());
    try testing.expectEqual(Status.completed, try fiber.run());
    try testing.expectEqualSlices(usize, &state.probe.expected, &state.probe.observed);
}
