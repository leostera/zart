//! Concrete actor I/O backends.

const builtin = @import("builtin");

pub const posix = @import("io/posix.zig");
pub const kqueue = @import("io/kqueue.zig");
pub const uring = @import("io/uring.zig");

pub const Kqueue = kqueue.Kqueue;
pub const Posix = posix.Posix;
pub const PosixPoll = Posix;
pub const Uring = uring.Uring;

pub const Default = switch (builtin.os.tag) {
    .linux => Uring,
    .dragonfly,
    .freebsd,
    .netbsd,
    .openbsd,
    .driverkit,
    .ios,
    .maccatalyst,
    .macos,
    .tvos,
    .visionos,
    .watchos,
    => Kqueue,
    else => Unsupported,
};

pub const default_supported = Default != Unsupported;

pub const Unsupported = struct {
    pub fn init() Unsupported {
        @compileError("zart.io.Default has no backend for this target");
    }
};
