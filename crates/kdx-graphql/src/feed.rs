//! `BlockFeed` (GQ-6, Task 6): new canonical blocks per chain for subscriptions, polled from
//! the committed tail checkpoints. Task 2 only defines the handle `build_schema` takes.

/// Handle of the block feed (cheap to clone). Filled in by Task 6.
#[derive(Clone, Default)]
pub struct BlockFeed {}
