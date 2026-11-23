# brz-replay

Add `brz-replay = "0.1"` to your Target's Cargo dependencies.

Use a Replay-controlled wait at a timer site whose behavior depends on
ordering with external input or dependency requests. Ordinary runs use the
original Tokio deadline. During Replay, the engine can release that specific
wait through the Unix socket named by `BRZ_REPLAY_TIMER_SOCKET`.

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
code after it returns. If the configured socket or control protocol fails, the
Future returns an I/O error.
