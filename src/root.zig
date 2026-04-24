//! zart: a small typed actor runtime.

const runtime = @import("Runtime.zig");

pub const Fiber = @import("Fiber.zig");
pub const Runtime = runtime.Runtime;
pub const ActorId = runtime.ActorId;
pub const Mailbox = runtime.Mailbox;
pub const Ctx = runtime.Ctx;
pub const MessageOf = runtime.MessageOf;

test {
    _ = runtime;
    _ = Fiber;
}
