//! Linux io_uring actor I/O backend.
//!
//! This is reserved as the Linux production backend. `poll(2)` is not selected
//! as the default Linux backend.

pub const Uring = struct {
    pub fn init() Uring {
        @compileError("zart.io.Uring is not implemented yet");
    }
};
