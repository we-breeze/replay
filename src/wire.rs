//! Versioned Replay timer socket frames shared with the engine.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 4096;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub version: u16,
    pub case_id: String,
    pub site: String,
    pub stream: String,
    pub occurrence: u64,
    pub consumed_through: u64,
}

impl Registration {
    pub fn validate(&self) -> io::Result<()> {
        if self.version != VERSION {
            return Err(invalid_data("unsupported Replay timer protocol version"));
        }
        if self.case_id.is_empty()
            || self.site.is_empty()
            || self.stream.is_empty()
            || self.occurrence == 0
        {
            return Err(invalid_data(
                "Replay timer requires a case, site, stream and positive occurrence",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Wake,
    WallClock,
    Error { message: String },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum End {
    End,
}

pub async fn read_frame<T: DeserializeOwned>(
    reader: &mut (impl AsyncBufRead + Unpin),
) -> io::Result<T> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(invalid_data("Replay timer frame is too large"));
    }
    if bytes.pop() != Some(b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Replay timer connection closed mid-frame",
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    if bytes.len() >= MAX_FRAME_BYTES {
        return Err(invalid_data("Replay timer frame is too large"));
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await
}

fn invalid_data(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
