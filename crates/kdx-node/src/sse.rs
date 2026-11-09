//! Minimal server-sent-events parser for `/header/updates`.

use std::pin::Pin;

use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt};
use serde_json::Value;

use crate::types::{NodeError, Result};

/// Incremental SSE decoder: feed bytes, get complete (event, data) pairs.
pub struct SseParser {
    buf: Vec<u8>,
    /// Bytes of `buf` already scanned without finding a separator.
    scanned: usize,
    max_event: usize,
}

/// An event grew past the configured cap without a separator.
#[derive(Debug, PartialEq, Eq)]
pub struct EventTooLarge;

impl SseParser {
    pub fn new(max_event: usize) -> SseParser {
        SseParser {
            buf: Vec::new(),
            scanned: 0,
            max_event,
        }
    }

    pub fn push(
        &mut self,
        chunk: &[u8],
    ) -> std::result::Result<Vec<(String, String)>, EventTooLarge> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            // Resume a few bytes back so a separator split across chunks is still found.
            let from = self.scanned.saturating_sub(3);
            let Some((end, sep_len)) = find_event_end(&self.buf, from) else {
                self.scanned = self.buf.len();
                if self.buf.len() > self.max_event {
                    return Err(EventTooLarge);
                }
                return Ok(out);
            };
            self.scanned = 0;
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
    }
}

fn find_event_end(buf: &[u8], from: usize) -> Option<(usize, usize)> {
    for i in from..buf.len() {
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
pub fn header_events(
    body: ByteStream,
    max_event: usize,
) -> Pin<Box<dyn Stream<Item = Result<Value>> + Send>> {
    struct State {
        body: ByteStream,
        parser: SseParser,
        pending: std::collections::VecDeque<Result<Value>>,
        done: bool,
    }
    let st = State {
        body,
        parser: SseParser::new(max_event),
        pending: Default::default(),
        done: false,
    };
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
                    let Ok(events) = st.parser.push(&chunk) else {
                        st.done = true;
                        return Some((
                            Err(NodeError::TooLarge {
                                limit: st.parser.max_event,
                            }),
                            st,
                        ));
                    };
                    for (event, data) in events {
                        if event == "BlockHeader" {
                            st.pending.push_back(
                                serde_json::from_str(&data)
                                    .map_err(|e| NodeError::Decode(e.to_string())),
                            );
                        }
                    }
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_separator_and_cap() {
        let mut p = SseParser::new(64);
        assert!(p
            .push(b"event: BlockHeader\ndata: {}\n")
            .unwrap()
            .is_empty());
        let ev = p.push(b"\nevent: x\r\n\r").unwrap();
        assert_eq!(ev, vec![("BlockHeader".to_string(), "{}".to_string())]);
        assert_eq!(
            p.push(b"\n").unwrap(),
            vec![("x".to_string(), String::new())]
        );
        assert_eq!(p.push(&[b'a'; 65]), Err(EventTooLarge));
    }
}
