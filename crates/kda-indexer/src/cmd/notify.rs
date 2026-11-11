//! systemd integration (spec §6.4): `sd_notify` over `$NOTIFY_SOCKET` and the watchdog
//! thread. Both are no-ops outside systemd (no socket, no `WATCHDOG_USEC`), so dev runs and
//! tests behave as before. No dependency: a datagram socket and a thread.

use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::sync::OnceLock;
use std::time::Duration;

use kdx_types::heartbeat::{self, Component};
use tokio_util::sync::CancellationToken;

/// The datagram socket and the target address, resolved once (None outside systemd).
fn target() -> Option<&'static (UnixDatagram, SocketAddr)> {
    static T: OnceLock<Option<(UnixDatagram, SocketAddr)>> = OnceLock::new();
    T.get_or_init(|| {
        let path = std::env::var_os("NOTIFY_SOCKET").filter(|p| !p.is_empty())?;
        let addr = parse_addr(&path)?;
        let sock = UnixDatagram::unbound().ok()?;
        Some((sock, addr))
    })
    .as_ref()
}

fn parse_addr(path: &std::ffi::OsStr) -> Option<SocketAddr> {
    use std::os::unix::ffi::OsStrExt;
    let b = path.as_bytes();
    if let Some(name) = b.strip_prefix(b"@") {
        abstract_addr(name)
    } else {
        SocketAddr::from_pathname(path).ok()
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn abstract_addr(name: &[u8]) -> Option<SocketAddr> {
    use std::os::linux::net::SocketAddrExt;
    SocketAddr::from_abstract_name(name).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn abstract_addr(_: &[u8]) -> Option<SocketAddr> {
    None
}

/// Send one notification datagram; failures are ignored (the service keeps running).
fn send(msg: &str) {
    if let Some((sock, addr)) = target() {
        let _ = sock.send_to_addr(msg.as_bytes(), addr);
    }
}

/// `READY=1`: the store is open and the API is bound.
pub fn ready(status: &str) {
    send(&format!("READY=1\nSTATUS={}", one_line(status)));
}

/// `STATUS=` text shown by `systemctl status`.
pub fn status(text: &str) {
    send(&format!("STATUS={}", one_line(text)));
}

/// `STOPPING=1` on SIGTERM.
pub fn stopping() {
    send("STOPPING=1");
}

/// `WATCHDOG=1`.
pub fn watchdog_ping() {
    send("WATCHDOG=1");
}

fn one_line(s: &str) -> String {
    s.replace('\n', " ")
}

/// Watchdog period from `WATCHDOG_USEC` (ignored when `WATCHDOG_PID` names another process).
pub fn watchdog_usec() -> Option<u64> {
    if let Ok(pid) = std::env::var("WATCHDOG_PID") {
        if pid.parse::<u32>().ok() != Some(std::process::id()) {
            return None;
        }
    }
    std::env::var("WATCHDOG_USEC")
        .ok()?
        .parse::<u64>()
        .ok()
        .filter(|&u| u > 0)
}

/// Start the watchdog thread when `WATCHDOG_USEC` is set: it pings every `usec / 3` while every
/// component in `watch` has beaten within `usec / 2`; a stale component stops the pings (and
/// is logged once per stall), so systemd restarts a hung process. It is an OS thread, not a
/// tokio or rayon worker, so a saturated pool cannot starve it.
pub fn spawn_watchdog(watch: &'static [Component]) {
    heartbeat::init();
    let Some(usec) = watchdog_usec() else { return };
    let ping_every = Duration::from_micros(usec / 3);
    let stale_after = Duration::from_micros(usec / 2);
    let tick = Duration::from_millis(500).min(ping_every);
    let spawned = std::thread::Builder::new()
        .name("kdx-watchdog".into())
        .spawn(move || {
            tracing::info!(
                watchdog_secs = usec / 1_000_000,
                "watchdog: pinging systemd while {:?} beat",
                watch.iter().map(|c| c.name()).collect::<Vec<_>>()
            );
            let mut last_ping: Option<std::time::Instant> = None;
            let mut stalled = false;
            loop {
                let stale = watch.iter().find(|c| heartbeat::age(**c) > stale_after);
                match stale {
                    Some(c) if !stalled => {
                        stalled = true;
                        tracing::error!(
                            component = c.name(),
                            silent_secs = heartbeat::age(*c).as_secs(),
                            "watchdog: component silent, not pinging (systemd restarts the unit)"
                        );
                    }
                    None if stalled => {
                        stalled = false;
                        tracing::warn!("watchdog: components beat again, pinging resumes");
                    }
                    _ => {}
                }
                if stale.is_none() && last_ping.is_none_or(|t| t.elapsed() >= ping_every) {
                    watchdog_ping();
                    last_ping = Some(std::time::Instant::now());
                }
                std::thread::sleep(tick);
            }
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "watchdog: cannot start the thread");
    }
}

/// Beat `Api` every 5 s from a tokio task until `stop`: proves the runtime still schedules.
pub fn spawn_api_heartbeat(stop: CancellationToken) {
    heartbeat::beat(Component::Api);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(5)) => heartbeat::beat(Component::Api),
            }
        }
    });
}

/// Update `STATUS=` every 5 s from a small closure until `stop`.
pub fn spawn_status<F>(stop: CancellationToken, text: F)
where
    F: Fn() -> String + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            status(&text());
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    });
}
