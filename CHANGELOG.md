# Changelog

All notable changes to `zart` are tracked here.

This project is still pre-release. Entries are grouped under `Unreleased` until a versioning scheme is established.

## Unreleased

### Added

- Added a small typed actor runtime with public `Actor(Msg)` handles and internal `ActorId` identities.
- Added function actors with message inference from `*zart.Ctx(Msg)`.
- Added struct actors with `pub const Msg` and `run(self, ctx)` shape.
- Added structurally copied message sends through typed actor handles.
- Added cooperative scheduling with `ctx.recv()` parking and `ctx.yield()` CPU checkpoints.
- Added configurable runtime policy through `Runtime.Options`, including execution budget, I/O budget, stack size, stack slab size, internal allocator, tracer, and I/O driver.
- Added stackful `Fiber` support for `aarch64` and `x86_64` non-Windows targets.
- Added pooled actor stacks with optional first-slab preallocation at runtime construction.
- Added non-blocking actor I/O facade over Zig `std.Io` operations through runtime-provided drivers.
- Added `zart.io.Default` comptime backend selection and a POSIX readiness/retry backend for file and stream socket reads/writes.
- Added default SMP runtime execution through `Runtime.run()`.
- Added worker parking, owner wakeups, and work stealing for multicore scheduling.
- Added runtime tracing events for actor lifecycle, scheduling, message sends/receives, failures, and I/O submission/completion.
- Added `zart.testing` utilities for deterministic actor interleaving tests.
- Added external runtime test modules under `tests/runtime/`.
- Added runnable examples under `examples/` and build steps for each example.
- Added actor and fiber benchmarks under `zig build bench`.

### Changed

- Split runtime internals into focused modules under `src/runtime/`.
- Moved runtime behavior tests out of the main runtime module and into grouped test files.
- Renamed public actor handles away from mailbox/PID terminology to `Actor(Msg)`.
- Expanded benchmarks to cover fiber init/run/yield, actor spawn, 1-to-1 sends, N-to-N sends, and synthetic I/O.
- Benchmark output now reports min/max/avg/mean/median/stddev with adaptive `ns`/`us`/`ms`/`s` units.
- Benchmark CLI now uses `--warmup N` and `--iterations N`.
- `Runtime.Options.worker_count` now defaults to `0`, which selects the host logical CPU count.
- Deterministic scheduler tests now opt into `.worker_count = 1`.

### Fixed

- Fixed optimized fiber stack switching by preventing the low-level context switch from being inlined.
- Fixed benchmark N-to-N setup so each case sends the intended number of messages.
- Fixed long-running randomized runtime tests that could appear hung during normal `zig build test` runs.
- Fixed SMP I/O completion races around actor parking, completion draining, and runtime quiescence.
- Fixed POSIX I/O pending-list synchronization under multicore actor submits.

### Known Gaps

- Monitors and links are not implemented yet.
- `spawn_blocking` is not implemented yet.
- Mailboxes and some scheduler paths still use conservative synchronization before the planned lock-free SMP internals.
- The POSIX poller uses a bounded wait until a dedicated poll wake fd/event mechanism lands.
