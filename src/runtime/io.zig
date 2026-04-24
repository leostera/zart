//! Runtime-owned wrappers for Zig's standard I/O interface.

const std = @import("std");

pub const RuntimeIo = struct {
    base: std.Io,

    pub fn init(base: ?std.Io) RuntimeIo {
        return .{
            .base = base orelse std.Io.failing,
        };
    }

    pub fn deinit(runtime_io: *RuntimeIo) void {
        runtime_io.* = undefined;
    }
};

pub const ActorIoContext = struct {
    base: *const std.Io,
    charge_runtime: *anyopaque,
    charge_actor: *anyopaque,
    charge_fn: *const fn (*anyopaque, *anyopaque) void,

    pub fn init(
        base: *const std.Io,
        charge_runtime: *anyopaque,
        charge_actor: *anyopaque,
        charge_fn: *const fn (*anyopaque, *anyopaque) void,
    ) ActorIoContext {
        return .{
            .base = base,
            .charge_runtime = charge_runtime,
            .charge_actor = charge_actor,
            .charge_fn = charge_fn,
        };
    }

    pub fn interface(context: *ActorIoContext) std.Io {
        return .{
            .userdata = context,
            .vtable = &actor_io_vtable,
        };
    }

    fn charge(context: *ActorIoContext) void {
        context.charge_fn(context.charge_runtime, context.charge_actor);
    }
};

const actor_io_vtable: std.Io.VTable = blk: {
    var vtable = std.Io.failing.vtable.*;
    vtable.async = actorIoAsync;
    vtable.concurrent = actorIoConcurrent;
    vtable.await = actorIoAwait;
    vtable.cancel = actorIoCancel;
    vtable.groupAsync = actorIoGroupAsync;
    vtable.groupConcurrent = actorIoGroupConcurrent;
    vtable.groupAwait = actorIoGroupAwait;
    vtable.groupCancel = actorIoGroupCancel;
    vtable.operate = actorIoOperate;
    vtable.batchAwaitAsync = actorIoBatchAwaitAsync;
    vtable.batchAwaitConcurrent = actorIoBatchAwaitConcurrent;
    vtable.batchCancel = actorIoBatchCancel;
    vtable.sleep = actorIoSleep;
    vtable.random = actorIoRandom;
    vtable.randomSecure = actorIoRandomSecure;
    vtable.now = actorIoNow;
    vtable.clockResolution = actorIoClockResolution;
    break :blk vtable;
};

fn actorIoContext(userdata: ?*anyopaque) *ActorIoContext {
    return @ptrCast(@alignCast(userdata.?));
}

fn actorIoAsync(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: std.mem.Alignment,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*std.Io.AnyFuture {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.async(
        base.userdata,
        result,
        result_alignment,
        context_bytes,
        context_alignment,
        start,
    );
}

fn actorIoConcurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: std.mem.Alignment,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) std.Io.ConcurrentError!*std.Io.AnyFuture {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.concurrent(
        base.userdata,
        result_len,
        result_alignment,
        context_bytes,
        context_alignment,
        start,
    );
}

fn actorIoAwait(
    userdata: ?*anyopaque,
    any_future: *std.Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.await(base.userdata, any_future, result, result_alignment);
}

fn actorIoCancel(
    userdata: ?*anyopaque,
    any_future: *std.Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.cancel(base.userdata, any_future, result, result_alignment);
}

fn actorIoGroupAsync(
    userdata: ?*anyopaque,
    group: *std.Io.Group,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.groupAsync(base.userdata, group, context_bytes, context_alignment, start);
}

fn actorIoGroupConcurrent(
    userdata: ?*anyopaque,
    group: *std.Io.Group,
    context_bytes: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) std.Io.ConcurrentError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.groupConcurrent(base.userdata, group, context_bytes, context_alignment, start);
}

fn actorIoGroupAwait(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.groupAwait(base.userdata, group, token);
}

fn actorIoGroupCancel(userdata: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.groupCancel(base.userdata, group, token);
}

fn actorIoOperate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.operate(base.userdata, operation);
}

fn actorIoBatchAwaitAsync(userdata: ?*anyopaque, batch: *std.Io.Batch) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.batchAwaitAsync(base.userdata, batch);
}

fn actorIoBatchAwaitConcurrent(
    userdata: ?*anyopaque,
    batch: *std.Io.Batch,
    timeout: std.Io.Timeout,
) std.Io.Batch.AwaitConcurrentError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.batchAwaitConcurrent(base.userdata, batch, timeout);
}

fn actorIoBatchCancel(userdata: ?*anyopaque, batch: *std.Io.Batch) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.batchCancel(base.userdata, batch);
}

fn actorIoSleep(userdata: ?*anyopaque, timeout: std.Io.Timeout) std.Io.Cancelable!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.sleep(base.userdata, timeout);
}

fn actorIoRandom(userdata: ?*anyopaque, buffer: []u8) void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.random(base.userdata, buffer);
}

fn actorIoRandomSecure(userdata: ?*anyopaque, buffer: []u8) std.Io.RandomSecureError!void {
    const context = actorIoContext(userdata);
    defer context.charge();
    const base = context.base.*;
    return base.vtable.randomSecure(base.userdata, buffer);
}

fn actorIoNow(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
    const context = actorIoContext(userdata);
    const base = context.base.*;
    return base.vtable.now(base.userdata, clock);
}

fn actorIoClockResolution(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Clock.ResolutionError!std.Io.Duration {
    const context = actorIoContext(userdata);
    const base = context.base.*;
    return base.vtable.clockResolution(base.userdata, clock);
}
