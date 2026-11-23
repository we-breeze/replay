use super::*;
#[cfg(all(brz_replay_timer, unix))]
use crate::wire::{self, End, Outcome, Registration};
#[cfg(all(brz_replay_timer, unix))]
use ::tokio::{io::BufReader, net::UnixListener, time::timeout};
#[cfg(all(brz_replay_timer, unix))]
use std::path::PathBuf;
#[cfg(any(not(brz_replay_timer), unix))]
use std::time::Duration;

#[cfg(all(brz_replay_timer, unix))]
fn socket() -> (tempfile::TempDir, PathBuf, UnixListener) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timer.sock");
    let listener = UnixListener::bind(&path).unwrap();
    (directory, path, listener)
}

#[cfg(any(not(brz_replay_timer), unix))]
fn key() -> TimerKey<'static> {
    TimerKey {
        case_id: "case",
        site: "text.flush",
        stream: "output",
        occurrence: 2,
        consumed_through: 3,
    }
}

#[cfg(not(brz_replay_timer))]
#[::tokio::test(start_paused = true)]
async fn ordinary_build_waits_until_the_tokio_deadline() {
    let deadline = Instant::now() + Duration::from_secs(60);
    sleep_until(key(), deadline).await.unwrap();
    assert!(Instant::now() >= deadline);
}

#[cfg(brz_replay_timer)]
#[test]
fn replay_build_uses_its_socket_unless_overridden_at_runtime() {
    let default_path = Path::new(BUILD_SOCKET);
    assert_eq!(timer_socket_path(None), default_path);
    assert_eq!(timer_socket_path(Some(OsStr::new(""))), default_path);
    let override_path = Path::new("/tmp/replay-timer-test.sock");
    assert_eq!(
        timer_socket_path(Some(override_path.as_os_str())),
        override_path
    );
}

#[cfg(all(brz_replay_timer, unix))]
#[::tokio::test]
async fn releases_one_wait_without_waiting_for_its_wall_clock_deadline() {
    let (_directory, path, listener) = socket();
    let target = sleep_until_on(&path, key(), Instant::now() + Duration::from_secs(60));
    let engine = async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let registration: Registration = wire::read_frame(&mut stream).await.unwrap();
        registration.validate().unwrap();
        assert_eq!(registration.case_id, "case");
        assert_eq!(registration.site, "text.flush");
        assert_eq!(registration.stream, "output");
        assert_eq!(registration.occurrence, 2);
        assert_eq!(registration.consumed_through, 3);
        wire::write_frame(stream.get_mut(), &Outcome::Wake)
            .await
            .unwrap();
        assert_eq!(
            wire::read_frame::<End>(&mut stream).await.unwrap(),
            End::End
        );
    };
    let (result, ()) = timeout(Duration::from_secs(5), async {
        ::tokio::join!(target, engine)
    })
    .await
    .unwrap();
    result.unwrap();
}

#[cfg(all(brz_replay_timer, unix))]
#[::tokio::test(start_paused = true)]
async fn wall_clock_response_preserves_the_original_deadline() {
    let (_directory, path, listener) = socket();
    let deadline = Instant::now() + Duration::from_secs(1);
    let (result, ()) = ::tokio::join!(sleep_until_on(&path, key(), deadline), async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        wire::read_frame::<Registration>(&mut stream).await.unwrap();
        wire::write_frame(stream.get_mut(), &Outcome::WallClock)
            .await
            .unwrap();
        let closed = wire::read_frame::<End>(&mut stream).await.unwrap_err();
        assert_eq!(closed.kind(), io::ErrorKind::UnexpectedEof);
    });
    result.unwrap();
    assert!(Instant::now() >= deadline);
}

#[cfg(all(brz_replay_timer, unix))]
#[::tokio::test]
async fn dropping_the_wait_closes_the_connection_without_an_end_receipt() {
    let (_directory, path, listener) = socket();
    let mut wait = Box::pin(sleep_until_on(&path, key(), Instant::now()));
    let peer = async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        wire::read_frame::<Registration>(&mut stream).await.unwrap();
        stream
    };
    let mut stream = ::tokio::select! {
        stream = timeout(Duration::from_secs(5), peer) => stream.unwrap(),
        result = &mut wait => panic!("timer returned before release: {result:?}"),
    };
    drop(wait);
    let closed = wire::read_frame::<End>(&mut stream).await.unwrap_err();
    assert_eq!(closed.kind(), io::ErrorKind::UnexpectedEof);
}

#[cfg(all(brz_replay_timer, unix))]
#[::tokio::test]
async fn controller_errors_reach_the_target() {
    let (_directory, path, listener) = socket();
    let (result, ()) = ::tokio::join!(sleep_until_on(&path, key(), Instant::now()), async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        wire::read_frame::<Registration>(&mut stream).await.unwrap();
        wire::write_frame(
            stream.get_mut(),
            &Outcome::Error {
                message: "case ended".into(),
            },
        )
        .await
        .unwrap();
    });
    assert_eq!(result.unwrap_err().to_string(), "case ended");
}
