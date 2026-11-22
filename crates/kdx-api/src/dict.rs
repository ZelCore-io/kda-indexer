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

    /// Ids of the event names (qualified, `module.NAME`, as interned) containing `needle`,
    /// within `module` when given, at most `cap`; the bool is true when more matched. The
    /// walk stops at the first match past `cap` (F-20).
    pub fn event_names(&self, needle: &str, module: Option<&str>, cap: usize) -> (Vec<u32>, bool) {
        let k = DictKind::EventName;
        let mut out = Vec::new();
        let n = self.txn_len(k).max(self.snap.len(k));
        // The module is everything before the last dot (`free.a.TRANSFER` is `free.a`).
        let in_module = |name: &str| {
            module.is_none_or(|m| name.rsplit_once('.').is_some_and(|(nm, _)| nm == m))
        };
        for id in 0..n as u32 {
            if let Some(name) = self.name(k, id) {
                if in_module(name) && name.contains(needle) {
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use kdx_hot::HotDict;
    use kdx_types::{DictKind, DictMut};

    /// F-20: `event_names` filters by module inside the walk and stops at `cap + 1`
    /// matches (the bool says more matched).
    #[test]
    fn event_names_filter_by_module_and_stop_past_the_cap() {
        let (_d, store) = crate::testutil::store();
        let mut w = store.hot_for_tests().rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        for n in [
            "free.a.TRANSFER",
            "coin.TRANSFER",
            "free.b.TRANSFER",
            "coin.TRANSFER_XCHAIN",
            "free.c.TRANSFER",
            "coin.GAS",
        ] {
            dict.intern(DictKind::EventName, n);
        }
        dict.persist(&mut w).unwrap();
        w.commit().unwrap();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let d = view.dict();
        assert_eq!(
            d.event_names("TRANSFER", Some("coin"), 2),
            (vec![1, 3], false)
        );
        assert_eq!(d.event_names("TRANSFER", Some("coin"), 1), (vec![1], true));
        assert_eq!(d.event_names("TRANSFER", None, 2), (vec![0, 1], true));
        assert_eq!(
            d.event_names("TRANSFER", None, 5),
            (vec![0, 1, 2, 3, 4], false)
        );
        // A module is matched whole: `free.a` is not `free`.
        assert_eq!(d.event_names("TRANSFER", Some("free"), 5), (vec![], false));
        assert_eq!(d.event_names("GAS", Some("coin"), 0), (vec![], true));
    }
}
