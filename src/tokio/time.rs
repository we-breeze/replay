//! Tokio waits that can be controlled by Replay.

use ::tokio::time::Instant;
#[cfg(brz_replay_timer)]
use std::ffi::OsStr;
use std::io;
#[cfg(brz_replay_timer)]
use std::path::Path;

#[cfg(brz_replay_timer)]
const BUILD_SOCKET: &str = env!("BRZ_REPLAY_TIMER_SOCKET");

/// Identifies one logical wait and the input the Target has processed.
#[derive(Debug, Clone, Copy)]
pub struct TimerKey<'a> {
    pub case_id: &'a str,
    pub site: &'a str,
    pub stream: &'a str,
    /// One-based occurrence of this wait site in the stream.
    pub occurrence: u64,
    /// Last application input ordinal processed by Target. Zero denotes no input.
    pub consumed_through: u64,
}

/// Wait until the Tokio deadline. Builds with `BRZ_REPLAY_TIMER_SOCKET` set
/// follow that controller's decision. A nonempty setting at runtime overrides
/// the build-time socket path in those builds.
///
/// Create and pin one Future per logical wait if it participates in a
/// `select!` loop. Dropping the Future cancels the wait.
pub async fn sleep_until(key: TimerKey<'_>, deadline: Instant) -> io::Result<()> {
    #[cfg(brz_replay_timer)]
    {
        let override_path = std::env::var_os(crate::TIMER_SOCKET_ENV);
        sleep_until_on(timer_socket_path(override_path.as_deref()), key, deadline).await
    }
    #[cfg(not(brz_replay_timer))]
    {
        let _ = key;
        ::tokio::time::sleep_until(deadline).await;
        Ok(())
    }
}

#[cfg(brz_replay_timer)]
fn timer_socket_path(override_path: Option<&OsStr>) -> &Path {
    Path::new(
        override_path
            .filter(|path| !path.is_empty())
            .unwrap_or(OsStr::new(BUILD_SOCKET)),
    )
}

/// Wait using the given Replay timer socket.
#[cfg(all(brz_replay_timer, unix))]
pub async fn sleep_until_on(socket: &Path, key: TimerKey<'_>, deadline: Instant) -> io::Result<()> {
    use crate::wire::{self, End, Outcome, Registration, VERSION};
    use ::tokio::{io::BufReader, net::UnixStream};

    let registration = Registration {
        version: VERSION,
        case_id: key.case_id.into(),
        site: key.site.into(),
        stream: key.stream.into(),
        occurrence: key.occurrence,
        consumed_through: key.consumed_through,
    };
    registration.validate()?;
    let mut connection = BufReader::new(UnixStream::connect(socket).await?);
    wire::write_frame(connection.get_mut(), &registration).await?;
    match wire::read_frame(&mut connection).await? {
        Outcome::Wake => wire::write_frame(connection.get_mut(), &End::End).await,
        Outcome::WallClock => {
            drop(connection);
            ::tokio::time::sleep_until(deadline).await;
            Ok(())
        }
        Outcome::Error { message } => Err(io::Error::other(message)),
    }
}

/// Wait using the given Replay timer socket.
#[cfg(all(brz_replay_timer, not(unix)))]
pub async fn sleep_until_on(_: &Path, _: TimerKey<'_>, _: Instant) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Replay timer sockets require Unix",
    ))
}

#[cfg(test)]
mod tests;
