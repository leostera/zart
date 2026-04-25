# zart

`zart` is a small typed actor runtime for Zig.

The core idea is simple: spawning an actor returns an `Actor(Msg)` handle, and that handle is the typed capability used to send messages. Messages are structurally copied into the target actor's inbox; pointer fields remain references and keep their normal Zig ownership/lifetime rules.

## Status

`zart` is pre-release and under active development.

Implemented today:

- Typed `Actor(Msg)` handles.
- Function actors and struct actors.
- Message sends through `actor.send(msg)`.
- Cooperative scheduling on stackful fibers.
- `ctx.recv()` parking and `ctx.yield()` CPU checkpoints.
- Configurable execution and I/O budgets.
- Non-blocking actor I/O facade through runtime-provided `std.Io` drivers.
- Default multicore scheduler with worker parking and work stealing.
- POSIX file and stream socket I/O backend through `zart.io.Default`.
- Runtime tracing hooks.
- Stack slab pooling for actor fibers.
- Runnable examples under `examples/`.
- Runtime tests and actor/fiber benchmarks.

Planned:

- Monitors and links.
- Lock-free SMP-ready mailboxes.
- `spawn_blocking`.
- Poller wake fd/event integration for lower-latency cross-worker I/O submits.

## Example

```zig
const std = @import("std");
const zart = @import("zart");

const Reply = union(enum) {
    value: u64,
};

const CounterMsg = union(enum) {
    inc: u64,
    get: zart.Actor(Reply),
    stop,
};

const Counter = struct {
    pub const Msg = CounterMsg;

    initial: u64 = 0,

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
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

    pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void {
        switch (try ctx.recv()) {
            .value => |n| self.slot.* = n,
        }
    }
};

test "counter actor" {
    var rt = try zart.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var observed: u64 = 0;
    const collector = try rt.spawn(Collector{ .slot = &observed });
    const counter = try rt.spawn(Counter{ .initial = 10 });

    try counter.send(.{ .inc = 32 });
    try counter.send(.{ .get = collector });
    try counter.send(.stop);

    try rt.run();

    try std.testing.expectEqual(@as(u64, 42), observed);
}
```

Function actors are also supported when the parameter is typed as `*zart.Ctx(Msg)`:

```zig
fn worker(ctx: *zart.Ctx(CounterMsg)) !void {
    while (true) {
        switch (try ctx.recv()) {
            .inc => {},
            .get => {},
            .stop => return,
        }
    }
}
```

## Runtime Configuration

`Runtime.Options` controls runtime policy and testability:

```zig
var rt = try zart.Runtime.init(allocator, .{
    .stack_size = 16 * 1024,
    .stack_slab_size = 4 * 1024 * 1024,
    .preallocate_stack_slab = true,
    .preallocate_registry_slab = true,
    .execution_budget = 64,
    .io_budget = 64,
    .worker_count = 0,
    .internal_allocator = std.heap.smp_allocator,
    .tracer = null,
    .io = null,
});
```

Actors receive `ctx.allocator()` from the allocator passed to `Runtime.init`. Runtime internals use `internal_allocator`, defaulting to `std.heap.smp_allocator`.

`worker_count = 0` uses the host logical CPU count. Set `worker_count = 1` for deterministic single-worker tests; the public execution API is still `rt.run()`.

The detailed runtime contract is documented in `docs/runtime-semantics.md`.

## I/O

Actor I/O is non-blocking by runtime contract. `ctx.io()` returns a `std.Io` facade that preserves standard I/O call shapes while routing operations through a user-provided `zart.IoDriver`.

The driver must not block the scheduler. It can complete requests immediately or retain them and complete them later from a poller/event callback.

The default backend is selected at comptime through `zart.io.Default`:

```zig
var actor_io = try zart.io.Default.init();
defer actor_io.deinit();

var rt = try zart.Runtime.init(allocator, .{
    .io = actor_io.driver(),
});
```

On POSIX targets, `zart.io.Default` retries non-blocking file and stream socket reads/writes internally. If an operation returns `WouldBlock`, the actor is parked until readiness is reported and then resumed with the completed result.

## Tracing

Pass `Runtime.Options.tracer` to observe runtime events without changing actor code. When no tracer is configured, trace event construction is skipped at the call sites.

Events include actor spawned/resumed/waiting/yielded/completed/failed, message sent/received, and I/O submitted/completed.

## Examples

Examples live under `examples/` and are wired into the build:

```sh
zig build examples
zig build example-counter
zig build example-ping_pong
zig build example-fan_out
zig build example-cooperative_yield
zig build example-tracing
zig build example-file_io
zig build example-http_server
```

They cover typed request/reply actors, ping-pong messaging, fan-out aggregation, explicit `ctx.yield()` checkpoints, runtime tracing, `ctx.io()` file reads, and one-actor-per-request HTTP socket handling.

The HTTP server runs until interrupted by default:

```sh
zig build example-http_server -- --port 8080 --acceptors 4
curl http://127.0.0.1:8080/hello
```

For smoke tests, limit the accept loop:

```sh
zig build example-http_server -- --port 8080 --max-requests 1
```

## Build

```sh
zig build
zig build test
zig build examples
zig build bench -- --quick
zig build bench -- --quick --warmup 5 --iterations 1000
zig build bench -Doptimize=ReleaseFast -- --quick
```

The benchmark runner reports min/max/avg/mean/median/stddev for each case using adaptive time units.

## Package

Import the module as `zart` from `src/root.zig`.

```zig
const zart = @import("zart");
```
