//! zart: a small typed actor runtime.

const runtime = @import("Runtime.zig");

pub const io = @import("io.zig");
pub const Fiber = @import("Fiber.zig");
pub const Runtime = runtime.Runtime;
pub const ActorId = runtime.ActorId;
pub const Actor = runtime.Actor;
pub const Tracer = runtime.Tracer;
pub const TraceEvent = runtime.TraceEvent;
pub const MessageTrace = runtime.MessageTrace;
pub const FailureTrace = runtime.FailureTrace;
pub const Io = runtime.Io;
pub const IoDriver = runtime.IoDriver;
pub const IoRequest = runtime.IoRequest;
pub const DefaultIo = runtime.DefaultIo;
pub const PosixIo = runtime.PosixIo;
pub const Ctx = runtime.Ctx;
pub const MessageOf = runtime.MessageOf;
pub const testing = @import("testing.zig");

test {
    _ = runtime;
    _ = io;
    _ = Fiber;
    _ = testing;
    _ = @import("runtime/injection_queue.zig");
    _ = @import("runtime/parker.zig");
    _ = @import("runtime/reclamation.zig");
    _ = @import("runtime/scheduler.zig");
    _ = @import("runtime/worker.zig");
}
