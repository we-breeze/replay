//! Tokio waits that can be controlled by Replay.

use ::tokio::time::Instant;
use std::io;
use std::path::Path;

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

/// Wait until the Tokio deadline or until the configured Replay engine releases
/// this wait. A configured controller's failures are returned to the caller.
///
/// Create and pin one Future per logical wait if it participates in a
/// `select!` loop. Dropping the Future cancels the wait.
pub async fn sleep_until(key: TimerKey<'_>, deadline: Instant) -> io::Result<()> {
    if let Some(path) = std::env::var_os(crate::TIMER_SOCKET_ENV) {
        return sleep_until_on(Path::new(&path), key, deadline).await;
    }
    ::tokio::time::sleep_until(deadline).await;
    Ok(())
}

/// Wait using the given Replay timer socket.
#[cfg(unix)]
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
#[cfg(not(unix))]
pub async fn sleep_until_on(_: &Path, _: TimerKey<'_>, _: Instant) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Replay timer sockets require Unix",
    ))
}

#[cfg(all(test, unix))]
mod tests;
