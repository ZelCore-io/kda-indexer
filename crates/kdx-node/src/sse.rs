//! Minimal server-sent-events parser for `/header/updates`.

use std::pin::Pin;

use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt};
use serde_json::Value;

use crate::types::{NodeError, Result};

/// Incremental SSE decoder: feed bytes, get complete (event, data) pairs.
#[derive(Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    pub fn push(&mut self, chunk: &[u8]) -> Vec<(String, String)> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some((end, sep_len)) = find_event_end(&self.buf) {
            let block: Vec<u8> = self.buf.drain(..end + sep_len).take(end).collect();
            let text = String::from_utf8_lossy(&block);
            let mut event = String::new();
            let mut data = String::new();
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = v.trim().to_string();
                } else if let Some(v) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(v.strip_prefix(' ').unwrap_or(v));
                }
            }
            if !data.is_empty() || !event.is_empty() {
                out.push((event, data));
            }
        }
        out
    }
}

fn find_event_end(buf: &[u8]) -> Option<(usize, usize)> {
    for i in 0..buf.len() {
        if buf[i..].starts_with(b"\n\n") {
            return Some((i, 2));
        }
        if buf[i..].starts_with(b"\r\n\r\n") {
            return Some((i, 4));
        }
    }
    None
}

type ByteStream = Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>>;

/// Turn the response body into a stream of `BlockHeader` event payloads (parsed JSON).
pub fn header_events(body: ByteStream) -> Pin<Box<dyn Stream<Item = Result<Value>> + Send>> {
    struct State {
        body: ByteStream,
        parser: SseParser,
        pending: std::collections::VecDeque<Result<Value>>,
        done: bool,
    }
    let st = State { body, parser: SseParser::default(), pending: Default::default(), done: false };
    Box::pin(stream::unfold(st, |mut st| async move {
        loop {
            if let Some(item) = st.pending.pop_front() {
                return Some((item, st));
            }
            if st.done {
                return None;
            }
            match st.body.next().await {
                None => return None,
                Some(Err(e)) => {
                    st.done = true;
                    return Some((Err(NodeError::Transport(e.to_string())), st));
                }
                Some(Ok(chunk)) => {
                    for (event, data) in st.parser.push(&chunk) {
                        if event == "BlockHeader" {
                            st.pending.push_back(serde_json::from_str(&data).map_err(|e| NodeError::Decode(e.to_string())));
                        }
                    }
                }
            }
        }
    }))
}
