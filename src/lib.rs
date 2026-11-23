//! Replay-controlled Target waits.

/// Target environment variable for its per-run Replay timer socket.
pub const TIMER_SOCKET_ENV: &str = "BRZ_REPLAY_TIMER_SOCKET";

pub mod tokio {
    pub mod time;
}

#[doc(hidden)]
pub mod wire;
