//! Liveness beats for the systemd watchdog (spec §6.4). Workers call [`beat`] as they make
//! progress; the watchdog thread of `run`/`serve` reads [`age`] and stops pinging systemd when
//! a component has been silent too long. Plain atomics: no allocation, no lock, no runtime.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// A part of the process that must keep making progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Component {
    /// Backfill, tail writer and freezer.
    Ingest,
    /// The API runtime (a tokio task beats every few seconds).
    Api,
}

impl Component {
    pub const ALL: [Component; 2] = [Component::Ingest, Component::Api];

    pub fn name(self) -> &'static str {
        match self {
            Component::Ingest => "ingest",
            Component::Api => "api",
        }
    }
}

/// Which part of ingest a beat comes from. `Ingest` is stale when any *active* source is
/// stale, so a busy tail cannot hide a hung freezer. A source is active from its first beat
/// until [`retire`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Startup, backfill and everything not marked otherwise.
    Run = 0,
    Tail = 1,
    Freezer = 2,
}

const SOURCES: usize = 3;
const INACTIVE: u64 = u64::MAX;

static BASE: OnceLock<Instant> = OnceLock::new();
/// Milliseconds since `BASE` of the last beat per ingest source (INACTIVE = not running).
static INGEST: [AtomicU64; SOURCES] = [
    AtomicU64::new(INACTIVE),
    AtomicU64::new(INACTIVE),
    AtomicU64::new(INACTIVE),
];
static API: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static SOURCE: Cell<Source> = const { Cell::new(Source::Run) };
}

fn base() -> Instant {
    *BASE.get_or_init(Instant::now)
}

fn now_ms() -> u64 {
    base().elapsed().as_millis() as u64
}

/// Start the clock (so a component that never beats is stale from now on, not from never).
pub fn init() {
    base();
    INGEST[Source::Run as usize].store(now_ms(), Ordering::Relaxed);
}

/// The source this thread's `beat(Ingest)` calls count for.
pub fn current_source() -> Source {
    SOURCE.with(|s| s.get())
}

/// Run `f` with this thread's ingest beats counted for `src` (restored afterwards).
pub fn as_source<R>(src: Source, f: impl FnOnce() -> R) -> R {
    let prev = SOURCE.with(|s| s.replace(src));
    let r = f();
    SOURCE.with(|s| s.set(prev));
    r
}

/// Record progress of an ingest source explicitly.
pub fn beat_source(src: Source) {
    INGEST[src as usize].store(now_ms(), Ordering::Relaxed);
}

/// The source is done (backfill finished): it no longer counts toward `age(Ingest)`.
pub fn retire(src: Source) {
    INGEST[src as usize].store(INACTIVE, Ordering::Relaxed);
}

/// Record progress of `c` (`Ingest` counts for the calling thread's source).
pub fn beat(c: Component) {
    match c {
        Component::Ingest => beat_source(current_source()),
        Component::Api => API.store(now_ms(), Ordering::Relaxed),
    }
}

/// Time since the last beat of `c`; for `Ingest` the oldest of the active sources.
pub fn age(c: Component) -> Duration {
    let now = now_ms();
    let last = match c {
        Component::Api => API.load(Ordering::Relaxed),
        Component::Ingest => INGEST
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .filter(|&v| v != INACTIVE)
            .min()
            .unwrap_or(now),
    };
    Duration::from_millis(now.saturating_sub(last))
}

/// Beats `c` every 5 s from a helper thread until dropped. Wrap a single long computation
/// that cannot beat itself (an index build, a sort); the work stays bounded, so a real hang
/// there is rare, but it is the one place a hang is masked. Counts for the creating
/// thread's ingest source.
pub struct KeepAlive {
    comp: Component,
    src: Source,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl KeepAlive {
    pub fn new(c: Component) -> KeepAlive {
        let src = current_source();
        beat(c);
        let stop = Arc::new(AtomicBool::new(false));
        let s = stop.clone();
        let thread = std::thread::Builder::new()
            .name("kdx-keepalive".into())
            .spawn(move || {
                as_source(src, || {
                    let mut slept = 0u32;
                    while !s.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(100));
                        slept += 1;
                        if slept >= 50 {
                            slept = 0;
                            beat(c);
                        }
                    }
                })
            })
            .ok();
        KeepAlive {
            comp: c,
            src,
            stop,
            thread,
        }
    }
}

impl Drop for KeepAlive {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        as_source(self.src, || beat(self.comp));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_resets_on_beat() {
        init();
        std::thread::sleep(Duration::from_millis(30));
        assert!(age(Component::Api) >= Duration::from_millis(30));
        beat(Component::Api);
        assert!(age(Component::Api) < Duration::from_millis(30));
    }

    #[test]
    fn busy_source_does_not_hide_a_silent_one() {
        init();
        beat_source(Source::Tail);
        beat_source(Source::Freezer);
        std::thread::sleep(Duration::from_millis(40));
        beat_source(Source::Tail);
        assert!(age(Component::Ingest) >= Duration::from_millis(40));
        retire(Source::Freezer);
        retire(Source::Run);
        assert!(age(Component::Ingest) < Duration::from_millis(40));
        retire(Source::Tail);
    }
}
