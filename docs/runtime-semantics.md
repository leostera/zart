# Runtime Semantics

This document specifies the behavior `zart` should preserve while the runtime
is hardened and optimized.

## Vocabulary

- `Actor(Msg)` is the public typed capability for sending `Msg` values.
- `ActorId` is the internal untyped identity used by tracing, registries, and
  low-level runtime internals.
- A message send structurally copies the message value into the recipient
  inbox. Pointer fields remain references owned by user code.
- An actor runs on a stackful fiber and owns its own inbox.

## Actor States

Actors move through these states:

- `runnable`: the actor is eligible to be scheduled.
- `running`: exactly one worker is currently executing the actor fiber.
- `waiting`: the actor is parked on a wait reason.
- `completed`: the actor body returned successfully and the actor handle is no
  longer sendable.
- `failed`: the actor body returned an error and the actor handle is no longer
  sendable.

`waiting` actors have one wait reason:

- `recv`: the actor is waiting for an inbox message.
- `io`: the actor is waiting for a runtime I/O request to complete.

`none` is used when an actor is not waiting.

## Spawning

`Runtime.spawn(entry)` creates a new actor and returns an `Actor(Msg)` handle.
The message type is inferred from a function actor parameter of
`*zart.Ctx(Msg)` or from a struct actor declaration of `pub const Msg`.

Spawning allocates an actor cell, a fiber stack, an `ActorId` registry slot, and
then enqueues the actor as `runnable`. External spawns are distributed across
workers. Spawns from an actor prefer the current actor's owner worker.

## Sending

`Actor(Msg).send(msg)` must:

- Reject sends after runtime shutdown with `error.RuntimeStopped`.
- Reject stale or unknown identities with `error.InvalidActor`.
- Reject mismatched message types with `error.WrongMessageType`.
- Reject completed or failed actors with `error.ActorDead`.
- Structurally copy `msg` into the recipient inbox.
- Wake the recipient when it is `waiting` on `recv`.

Message order is FIFO for a single producer sending to one actor. Concurrent
multi-producer ordering is intentionally unspecified across producers, but each
message must be delivered at most once while the target remains alive.

## Receiving

`ctx.recv()` returns the next inbox message. If no message is available, the
actor transitions to `waiting/recv` and yields to the scheduler.

`ctx.recv()` must re-check the inbox after publishing `waiting/recv` so a racing
send cannot be lost between the empty check and the park.

## Yielding

`ctx.yield()` is a cooperative CPU checkpoint. An actor has an execution budget
for each scheduler turn. Calling `ctx.yield()` decrements the budget until it is
exhausted; then the actor becomes `runnable` and yields its fiber.

Long CPU loops should call `ctx.yield()` explicitly.

## I/O

`ctx.io()` returns a `std.Io` facade backed by the runtime I/O driver. The driver
contract is non-blocking: submitting a request must return without blocking a
scheduler worker.

When an actor starts an I/O request:

- The request is submitted to the runtime driver.
- The actor parks as `waiting/io`.
- The actor resumes only after the completion is drained by the scheduler.
- Stack-backed request storage must remain live until the scheduler has drained
  the completion.

If a driver has a poller, `Runtime.run()` remains alive while it has pending
I/O. If a driver has no poller, `Runtime.run()` may return with pending I/O so
external code can complete it and call `run()` again.

## Running

`Runtime.run()` is the only public execution API. It starts the configured
worker set and returns when:

- The runtime is stopped.
- An actor fails and the failure is reported from `run()`.
- There are no runnable actors, no active actors, no queued I/O completions, and
  no evented pending I/O.

`Runtime.Options.worker_count = 0` uses the host logical CPU count.
Deterministic tests should set `.worker_count = 1`.

## Failure

If an actor returns an error from its body:

- The actor transitions to `failed`.
- The actor is removed from the registry.
- The actor is retired for later destruction.
- `Runtime.run()` returns that error.
- The runtime returns to `idle`; unrelated live actors may continue on a later
  `run()`.
- Sends through existing handles fail with `error.InvalidActor` or
  `error.ActorDead`, depending on whether the registry slot has already been
  removed.

Failures are not monitors or links. Supervision semantics are planned but not
implemented.

## Shutdown

`ctx.stop()` requests runtime shutdown after the current actor turn. The runtime
wakes parked workers and transitions to `stopped` when `run()` leaves.

After shutdown, sends return `error.RuntimeStopped`.

## Reclamation

Completed and failed actors are removed from the registry before their actor
cell is destroyed. Registry slots increment their generation before reuse so
stale handles cannot resolve to new actors.

Retired actors may be destroyed after a scheduler boundary. Runtime teardown
must drain I/O completions before destroying actor stacks because stack-backed
I/O request nodes can still be in the completion queue.

`Runtime.deinit()` releases live actors that have not run and actors parked on
`recv`. It is a runtime error to deinitialize while unresolved actor I/O is
still pending, because the driver can still hold stack-backed request pointers.
Call `rt.hasPendingIo()` when tearing down no-poller test drivers that may
complete requests after `run()` returns.

## Tracing

When configured, a tracer records actor lifecycle, scheduling, message, failure,
and I/O events. Trace callbacks are serialized by the runtime.

When no tracer is configured, trace call sites should be cheap and avoid trace
event allocation.
