# Contributing

`zart` is early-stage runtime work. Keep changes small, tested, and explicit about semantics.

## Requirements

- Zig `0.16.0`.
- A supported fiber target: currently `aarch64` or `x86_64`, excluding `x86_64-windows`.

## Common Commands

Use timeouts for long-running commands when working locally or through an agent:

```sh
timeout 120 zig build test
timeout 120 zig build examples
timeout 120 zig build bench -- --quick
timeout 120 zig build bench -- --quick --warmup 5 --iterations 1000
```

Useful build commands:

```sh
zig build
zig build test
zig build examples
zig build bench -- --quick
zig build bench -Doptimize=ReleaseFast -- --quick
```

## Design Principles

The public handle is `Actor(Msg)`. Avoid PID/process terminology in user-facing APIs.

Messages are copied structurally into actor inboxes. Do not add implicit deep-copy behavior; pointer fields are user-managed references.

Actors should be cheap to spawn. Runtime internals should keep allocation paths visible and benchmarked.

Scheduling is cooperative inside actors and SMP by default across runtime workers. `ctx.recv()` parks, `ctx.yield()` cooperates at CPU checkpoints, and actor I/O must be non-blocking.

Runtime policy belongs in `Runtime.Options` so tests can force small budgets, small stacks, custom allocators, custom tracers, and custom I/O drivers.

Keep `src/Runtime.zig` as orchestration glue. Put focused internals under `src/runtime/`.

Use `rt.run()` for runtime execution. Deterministic scheduler tests should set `.worker_count = 1`; SMP behavior belongs in focused tests such as `tests/runtime/smp.zig`.

## Tests

Runtime tests live under `tests/runtime/` and should be grouped by behavior:

```text
tests/runtime/spawn.zig
tests/runtime/scheduler.zig
tests/runtime/io.zig
tests/runtime/tracing.zig
tests/runtime/properties.zig
tests/runtime/stress.zig
```

Shared actor testing tools belong in `src/testing.zig` and are exported as `zart.testing`.

For scheduling behavior, prefer deterministic traces over timing sleeps.

For randomized behavior, use deterministic seeds and keep iteration counts bounded enough for `zig build test`.

## Benchmarks

Benchmarks live in `bench/actors.zig`.

Use `--quick` for local smoke checks and larger counts for performance investigations:

```sh
zig build bench -- --quick --warmup 5 --iterations 1000
zig build bench -Doptimize=ReleaseFast -- --quick --warmup 5 --iterations 1000
```

When optimizing a path, add or refine a benchmark that isolates the cost first. Useful slices include fiber init, fiber run, fiber yield/resume, stack acquire/release, actor cell allocation, actor spawn, one-to-one sends, N-to-N sends, and synthetic I/O.

## Examples

Examples live under `examples/` and should compile with:

```sh
zig build examples
```

Add a small runnable example when introducing a user-facing runtime capability. Prefer examples that use the public `Actor(Msg)`, `Ctx(Msg)`, and `Runtime.run()` APIs without reaching into runtime internals.

## Style

Prefer clear module names:

```zig
const actor = @import("runtime/actor.zig");
```

Avoid `_mod` suffixes for imports.

Comments are welcome.

Default to ASCII in source and docs unless a file already uses non-ASCII or the term needs it.

## Commits

Use conventional commits:

```text
feat(runtime): add stack pool preallocation
fix(fiber): preserve optimized context switching
test(runtime): cover parked actor wakeups
bench(runtime): add actor spawn statistics
docs: document actor runtime basics
```

Commit coherent slices after tests pass.
