//! Typed actor handle and message-type inference helpers.

/// Builds the typed actor handle for a message type.
pub fn Actor(comptime RuntimeType: type, comptime ActorId: type, comptime Msg: type) type {
    return struct {
        pub const Message = Msg;

        raw: ActorId,
        runtime: *RuntimeType,

        /// Structurally copies `msg` into the recipient actor's inbox.
        pub fn send(actor: @This(), msg: Msg) !void {
            try actor.runtime.send(Msg, actor.raw, msg);
        }

        /// Returns the untyped runtime identity for tracing or low-level APIs.
        pub fn any(actor: @This()) ActorId {
            return actor.raw;
        }
    };
}

/// Infers an actor's message type from its function context or struct `Msg`.
pub fn MessageOf(comptime ActorType: type) type {
    return switch (@typeInfo(ActorType)) {
        .@"fn" => messageOfFunction(ActorType),
        .@"struct" => {
            if (!@hasDecl(ActorType, "Msg")) {
                @compileError("struct actor must declare pub const Msg");
            }
            if (!@hasDecl(ActorType, "run")) {
                @compileError("struct actor must declare pub fn run(self: *@This(), ctx: *zart.Ctx(Msg)) !void");
            }
            return ActorType.Msg;
        },
        else => @compileError("actor must be a function or struct"),
    };
}

fn messageOfFunction(comptime Fn: type) type {
    const fn_info = @typeInfo(Fn).@"fn";
    if (fn_info.params.len != 1) {
        @compileError("function actor must have the shape fn (*zart.Ctx(Msg)) !void");
    }

    const param_type = fn_info.params[0].type orelse {
        @compileError("function actor context parameter must be typed");
    };
    const ptr_info = switch (@typeInfo(param_type)) {
        .pointer => |pointer| pointer,
        else => @compileError("function actor first parameter must be *zart.Ctx(Msg)"),
    };

    const ctx_type = ptr_info.child;
    if (!@hasDecl(ctx_type, "Message")) {
        @compileError("function actor first parameter must be *zart.Ctx(Msg)");
    }
    return ctx_type.Message;
}
