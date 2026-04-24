//! zart: a small typed actor runtime.

const runtime = @import("Runtime.zig");

pub const Fiber = @import("Fiber.zig");
pub const Runtime = runtime.Runtime;
pub const AnyActorId = runtime.AnyActorId;
pub const ActorId = runtime.ActorId;
pub const Tracer = runtime.Tracer;
pub const TraceEvent = runtime.TraceEvent;
pub const MessageTrace = runtime.MessageTrace;
pub const FailureTrace = runtime.FailureTrace;
pub const Ctx = runtime.Ctx;
pub const MessageOf = runtime.MessageOf;

test {
    _ = runtime;
    _ = Fiber;
}
