//! Runtime tracing types.

/// Opaque runtime identity used by tracing and typed actor handles.
pub const ActorId = struct {
    index: usize,
    generation: u64,
};

/// Message trace payload. `from` is null for sends made outside an actor.
pub const MessageTrace = struct {
    from: ?ActorId,
    to: ActorId,
};

/// Actor failure trace payload.
pub const FailureTrace = struct {
    actor: ActorId,
    err: anyerror,
};

/// Runtime event stream emitted when `Runtime.Options.tracer` is set.
pub const TraceEvent = union(enum) {
    actor_spawned: ActorId,
    actor_resumed: ActorId,
    actor_waiting: ActorId,
    actor_yielded: ActorId,
    actor_completed: ActorId,
    actor_failed: FailureTrace,
    message_sent: MessageTrace,
    message_received: ActorId,
};

/// User-provided trace sink.
pub const Tracer = struct {
    context: ?*anyopaque,
    event_fn: *const fn (?*anyopaque, TraceEvent) void,

    pub fn record(tracer: Tracer, event: TraceEvent) void {
        tracer.event_fn(tracer.context, event);
    }
};
