//! Per-request DataLoaders (GQ-5, Tasks 3–5). Each batch is one `ApiState::read` (a fresh
//! `View` on the blocking pool, one read permit) of at most [`MAX_BATCH_KEYS`] keys; no
//! `View` lives across an `.await`, and every batch of a request shares its deadline
//! ([`crate::deadline`]).

/// Keys per DataLoader batch.
pub const MAX_BATCH_KEYS: usize = 256;

/// Keys between two `View::check_deadline` calls in a list walk.
pub const DEADLINE_CHECK_EVERY: usize = 64;
