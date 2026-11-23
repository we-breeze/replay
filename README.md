# brz-replay

Use a Replay-controlled wait at a timer site whose behavior depends on
ordering with external input or dependency requests. Add the SDK to the
Target's Cargo dependencies.

```toml
[dependencies]
brz-replay = "0.1"
```

Build the ordinary binary with `cargo build --release` and leave
`BRZ_REPLAY_TIMER_SOCKET` unset in the build environment. Its waits use
the original Tokio deadline directly.

Use the managed socket path when building for Replay.

```sh
BRZ_REPLAY_TIMER_SOCKET=/traffic-e2e/replay/timer.sock cargo build --release
```

`BRZ_REPLAY_TIMER_SOCKET` selects the build mode. An empty value fails the
build. A Replay binary uses its compiled-in path unless a nonempty runtime
setting overrides it. The engine can release a registered wait or direct it to
use its original Tokio deadline.

```rust
use brz_replay::tokio::time::{TimerKey, sleep_until};
use std::time::Duration;
use tokio::time::Instant;

async fn wait_for_flush(
    case_id: &str,
    stream: &str,
    batch: u64,
    consumed_through: u64,
    first_buffered_at: Instant,
) -> std::io::Result<()> {
    let key = TimerKey {
        case_id,
        site: "stream.flush",
        stream,
        occurrence: batch,
        consumed_through,
    };
    sleep_until(key, first_buffered_at + Duration::from_secs(1)).await
}
```

Register the wait after handling the input identified by `consumed_through`.
Count application events after parsing and handling them. Use occurrence
numbers starting at one for each site and stream. When polling a wait in a
`tokio::select!` loop, keep the pinned Future for the whole logical wait.
Dropping it cancels the wait.

The release affects this Future. The application still executes its original
code after it returns. In a Replay build, a socket or controller failure
returns an I/O error.
