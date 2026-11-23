use super::*;
use crate::wire::{self, End, Outcome, Registration};
use ::tokio::{io::BufReader, net::UnixListener, time::timeout};
use std::{path::PathBuf, time::Duration};

fn socket() -> (tempfile::TempDir, PathBuf, UnixListener) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timer.sock");
    let listener = UnixListener::bind(&path).unwrap();
    (directory, path, listener)
}

fn key() -> TimerKey<'static> {
    TimerKey {
        case_id: "case",
        site: "text.flush",
        stream: "output",
        occurrence: 2,
        consumed_through: 3,
    }
}

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
