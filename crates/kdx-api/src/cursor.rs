//! Page cursors (`Chainweb-Next`, native `next`): a TxKey (stable across a freeze, spec §3.5)
//! plus a sub-position and a fingerprint of the block it points into (spec §3.2), so a page
//! taken before a reorg is refused instead of silently skipping or repeating rows.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use kdx_types::TxKey;

use crate::store::View;
use crate::{ApiError, ApiResult};

const VERSION: u8 = 1;
const LEN: usize = 1 + 8 + 2 + 4;

/// Text of the 400 answered for a cursor whose block was reorged away.
pub const REORGED: &str = "cursor invalidated by reorg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub key: TxKey,
    /// Position inside the tx (event or transfer index), route-defined.
    pub sub: u16,
    /// First 4 bytes of the hash of block `(key.chain, key.height)`.
    pub fp: u32,
}

impl Cursor {
    /// A cursor at `key` with the fingerprint of its block in `view`.
    pub fn new(view: &View, key: TxKey, sub: u16) -> ApiResult<Cursor> {
        let fp = fingerprint(view, key)?.unwrap_or(0);
        Ok(Cursor { key, sub, fp })
    }

    /// A token for "every row at heights >= `done` consumed" (a bounded scan cut, `sub` is
    /// the route's height-done marker). It fingerprints the block of the first chain that
    /// has one at `done` and names that chain in the key, so a lagging or halted chain 0
    /// does not make the token dead on arrival (R6). The key's pos carries `skip`, the
    /// rows of `offset` still owed (R7). With no block at `done` on any chain, fp is 0 and
    /// `check` accepts it while that is still so.
    pub fn height_done(view: &View, done: u32, sub: u16, skip: u16) -> ApiResult<Cursor> {
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            let key = TxKey::new(done, c, skip);
            if let Some(fp) = fingerprint(view, key)? {
                return Ok(Cursor { key, sub, fp });
            }
        }
        Ok(Cursor {
            key: TxKey::new(done, 0, skip),
            sub,
            fp: 0,
        })
    }

    /// base64url of `version 1 | key | sub | fp` (big-endian).
    pub fn encode(&self) -> String {
        let mut b = [0u8; LEN];
        b[0] = VERSION;
        b[1..9].copy_from_slice(&self.key.to_be_bytes());
        b[9..11].copy_from_slice(&self.sub.to_be_bytes());
        b[11..15].copy_from_slice(&self.fp.to_be_bytes());
        URL_SAFE_NO_PAD.encode(b)
    }

    /// Parse a token; `400` on anything that is not one.
    pub fn decode(s: &str) -> ApiResult<Cursor> {
        let bad = || ApiError::BadRequest("invalid cursor".into());
        if s.len() > 64 {
            return Err(bad());
        }
        let b = URL_SAFE_NO_PAD.decode(s.trim_end_matches('=')).map_err(|_| bad())?;
        if b.len() != LEN || b[0] != VERSION {
            return Err(bad());
        }
        Ok(Cursor {
            key: TxKey::from_be_bytes(b[1..9].try_into().map_err(|_| bad())?),
            sub: u16::from_be_bytes([b[9], b[10]]),
            fp: u32::from_be_bytes(b[11..15].try_into().map_err(|_| bad())?),
        })
    }

    /// `400 cursor invalidated by reorg` when the block the cursor points into is no longer
    /// the stored one (or is gone).
    pub fn check(&self, view: &View) -> ApiResult<()> {
        match fingerprint(view, self.key)? {
            Some(fp) if fp == self.fp => Ok(()),
            // A height-done token minted where no chain had a block (see `height_done`).
            // Row cursors always point into a stored block, so they never mint fp 0.
            None if self.fp == 0 => Ok(()),
            _ => Err(ApiError::BadRequest(REORGED.into())),
        }
    }
}

fn fingerprint(view: &View, key: TxKey) -> ApiResult<Option<u32>> {
    Ok(view
        .block_hash(key.chain(), key.height())?
        .map(|h| u32::from_be_bytes([h.0[0], h.0[1], h.0[2], h.0[3]])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_garbage() {
        let c = Cursor {
            key: TxKey::new(4_000_000, 19, 7),
            sub: 3,
            fp: 0xdead_beef,
        };
        assert_eq!(Cursor::decode(&c.encode()).unwrap(), c);
        for bad in ["", "x", "AAAA", "!!!!", &"A".repeat(3000), &c.encode()[1..]] {
            assert!(matches!(Cursor::decode(bad), Err(ApiError::BadRequest(_))), "{bad}");
        }
    }

    /// R6/R7: a cut token fingerprints a chain that has a block at `done` (chain 0 lags
    /// here) and carries the owed offset.
    #[test]
    fn height_done_uses_a_present_chain() {
        use kdx_hot::tables::block_key;
        use kdx_types::record::{encode_block, BlockRecord};
        let (_d, store) = crate::testutil::store();
        let b = BlockRecord {
            hash: kdx_types::Hash32([0xab; 32]),
            chain: 3,
            height: 100,
            ..Default::default()
        };
        let mut v = Vec::new();
        encode_block(&b, &mut v);
        let mut w = store.hot_for_tests().rw().unwrap();
        w.put(kdx_hot::Table::Headers, &block_key(3, 100), &v).unwrap();
        w.commit().unwrap();
        let far = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let view = store.view(far).unwrap();
        let c = Cursor::height_done(&view, 100, u16::MAX, 7).unwrap();
        assert_eq!(c.key.chain(), 3);
        assert_eq!(c.key.pos(), 7);
        assert_eq!(c.fp, 0xabab_abab);
        let c = Cursor::decode(&c.encode()).unwrap();
        c.check(&view).unwrap();
        // No block at all at that height: fp 0, still accepted.
        let e = Cursor::height_done(&view, 200, u16::MAX, 0).unwrap();
        assert_eq!(e.fp, 0);
        e.check(&view).unwrap();
        // A row cursor whose block is gone stays refused.
        let gone = Cursor { key: TxKey::new(101, 3, 1), sub: 0, fp: 5 };
        assert!(gone.check(&view).is_err());
    }
}
