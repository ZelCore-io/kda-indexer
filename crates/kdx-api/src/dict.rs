//! `ViewDict`: names for one `View`. The shared `MemDict` snapshot answers most lookups; ids
//! and names past it (interned since the snapshot) are read from the view's own txn, so a
//! record the view can see always resolves.

use std::sync::{Arc, OnceLock};

use kdx_hot::dict::fwd_key;
use kdx_hot::{RoTxn, Table, Txn};
use kdx_types::{Dict, DictKind, MemDict};

/// Most event-name ids an event query may expand to (`api.max_event_lists`).
pub const MAX_EVENT_LISTS: usize = 256;

pub struct ViewDict<'a> {
    txn: &'a RoTxn,
    snap: Arc<MemDict>,
    /// Per kind: one lazily read slot per id past the snapshot (sized on first miss).
    extra: [OnceLock<Vec<OnceLock<Option<String>>>>; 5],
}

impl<'a> ViewDict<'a> {
    pub fn new(txn: &'a RoTxn, snap: Arc<MemDict>) -> ViewDict<'a> {
        ViewDict {
            txn,
            snap,
            extra: Default::default(),
        }
    }

    fn txn_len(&self, k: DictKind) -> usize {
        self.txn.count(Table::DictRev(k)).unwrap_or(0)
    }

    pub fn account_id(&self, name: &str) -> Option<u32> {
        self.find(DictKind::Account, name)
    }

    pub fn module_id(&self, name: &str) -> Option<u32> {
        self.find(DictKind::Module, name)
    }

    /// Ids of the event names (unqualified, as interned) containing `needle`, at most
    /// `cap`; the bool is true when more matched. The module filter is the caller's
    /// (`evt` module entity), names carry no module.
    pub fn event_names(&self, needle: &str, cap: usize) -> (Vec<u32>, bool) {
        let k = DictKind::EventName;
        let mut out = Vec::new();
        let n = self.txn_len(k).max(self.snap.len(k));
        for id in 0..n as u32 {
            if let Some(name) = self.name(k, id) {
                if name.contains(needle) {
                    if out.len() == cap {
                        return (out, true);
                    }
                    out.push(id);
                }
            }
        }
        (out, false)
    }
}

impl Dict for ViewDict<'_> {
    fn find(&self, k: DictKind, s: &str) -> Option<u32> {
        if let Some(id) = self.snap.find(k, s) {
            return Some(id);
        }
        let v = self
            .txn
            .get(Table::DictFwd(k), &fwd_key(s.as_bytes()))
            .ok()??;
        let id = u32::from_be_bytes(v.as_slice().try_into().ok()?);
        // A hashed (over-long) key could in principle collide: confirm the name.
        (self.name(k, id) == Some(s)).then_some(id)
    }

    fn name(&self, k: DictKind, id: u32) -> Option<&str> {
        let base = self.snap.len(k);
        if (id as usize) < base {
            return self.snap.name(k, id);
        }
        let slots = self.extra[k as usize].get_or_init(|| {
            let n = self.txn_len(k).saturating_sub(base);
            (0..n).map(|_| OnceLock::new()).collect()
        });
        let slot = slots.get(id as usize - base)?;
        slot.get_or_init(|| {
            let v = self.txn.get(Table::DictRev(k), &id.to_be_bytes()).ok()??;
            String::from_utf8(v).ok()
        })
        .as_deref()
    }
}
