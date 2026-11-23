//! Replay-controlled Target waits.

/// Environment variable for the build-time socket and its optional runtime override.
#[cfg(brz_replay_timer)]
pub const TIMER_SOCKET_ENV: &str = "BRZ_REPLAY_TIMER_SOCKET";

pub mod tokio {
    pub mod time;
}

#[doc(hidden)]
#[cfg(brz_replay_timer)]
pub mod wire;
