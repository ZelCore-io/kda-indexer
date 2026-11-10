//! Watchdog heartbeats (defined in `kdx-types` so `kdx-segment` can beat too).

pub use kdx_types::heartbeat::{
    age, as_source, beat, beat_source, init, retire, Component, KeepAlive, Source,
};
