use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

/// Shared token bucket with burst 1: at most `rps` request starts per second.
pub struct RateLimiter {
    interval: Option<Duration>,
    next: Mutex<Instant>,
}

impl RateLimiter {
    /// `rps == 0` disables limiting.
    pub fn new(rps: u32) -> Self {
        let interval = (rps > 0).then(|| Duration::from_secs_f64(1.0 / f64::from(rps)));
        Self { interval, next: Mutex::new(Instant::now()) }
    }

    pub async fn acquire(&self) {
        let Some(interval) = self.interval else { return };
        let mut next = self.next.lock().await;
        let now = Instant::now();
        if *next > now {
            tokio::time::sleep_until(*next).await;
            *next += interval;
        } else {
            *next = now + interval;
        }
    }
}
