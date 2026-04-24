//! Concrete actor I/O backends.

const builtin = @import("builtin");

pub const posix = @import("io/posix.zig");

pub const Posix = posix.Posix;
pub const PosixIo = Posix;

pub const Default = switch (builtin.os.tag) {
    .linux,
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
    => Posix,
    else => Unsupported,
};

pub const default_supported = Default != Unsupported;

pub const Unsupported = struct {
    pub fn init() Unsupported {
        @compileError("zart.io.Default has no backend for this target");
    }
};
