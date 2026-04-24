# Agent Notes

This repository is `zart`, a small typed actor runtime for Zig.

## Working Rules

Use `rg`/`rg --files` for searches. Keep edits focused and avoid touching unrelated untracked files. The working tree may be dirty, so do not revert user changes unless explicitly asked.

Run Zig commands with a timeout when possible:

```sh
timeout 120 zig build test
timeout 120 zig build examples
timeout 120 zig build bench -- --quick
```

Prefer conventional commits for completed slices:

```text
feat(runtime): add typed actor handles
fix(fiber): preserve optimized stack switching
test(runtime): cover scheduler interleavings
```

## Runtime Vocabulary

Use `Actor(Msg)` for the public typed handle. Do not call user-facing actors "processes", and do not use `Pid` terminology.

Use `ActorId` for internal/raw identities. `ActorId` is appropriate in tracing, registries, and low-level runtime code.

Messages are structurally copied into actor inboxes. Pointer fields remain references owned by user code.

## Current Runtime Shape

The runtime is cooperative inside actors and SMP by default across scheduler workers. Actors run on stackful fibers. `ctx.recv()` parks until a message arrives, `ctx.yield()` cooperates at CPU checkpoints, and `ctx.io()` exposes a non-blocking `std.Io` facade backed by a runtime-provided driver.

Use `Runtime.run()` for execution. `Runtime.Options.worker_count = 0` uses the host logical CPU count; tests that need deterministic single-worker semantics should set `.worker_count = 1`.

The runtime is intentionally split across modules under `src/runtime/`. Avoid growing `src/Runtime.zig` into a god module.

Planned work includes monitors/links, stronger lock-free mailbox internals for SMP, poller wake integration, and `spawn_blocking`.

## Tests And Benchmarks

Runtime tests live under `tests/runtime/`, grouped by behavior. Shared actor testing utilities belong under `src/testing.zig` and are exported as `zart.testing`.

Benchmarks live in `bench/actors.zig`. The benchmark CLI supports:

```sh
zig build bench -- --quick --warmup 5 --iterations 1000
```

Benchmark output reports min/max/avg/mean/median/stddev with adaptive time units.

Examples live under `examples/` and should compile with `zig build examples`. Individual examples run as `zig build example-counter`, `zig build example-file_io`, and so on.
