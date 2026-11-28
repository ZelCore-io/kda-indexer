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
        let b = URL_SAFE_NO_PAD
            .decode(s.trim_end_matches('='))
            .map_err(|_| bad())?;
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
            // Row cursors always point into a stored block, so they never mint fp 0: a row
            // cursor with fp 0 at an absent block is forged (F-19).
            None if self.fp == 0 && self.sub == crate::compat::SUB_HEIGHT_DONE => Ok(()),
            _ => Err(ApiError::BadRequest(REORGED.into())),
        }
    }
}

/// Longest request key text a stock token may carry (real ones are 43 characters).
const STOCK_RK_MAX: usize = 64;

/// A `next` token minted by stock chainweb-data, so a client paging across a switch from
/// stock to kda-indexer keeps its place. Stock's token is the standard base64 (no `=`) of
/// the Haskell `show` text of `(position, offset)`: `position` is `(height, request key,
/// idx)` on `/txs/account` and `/txs/events` (the request key is `cb` for a coinbase row)
/// and `(height, request key)` on `/txs/search`; `offset` is the rows still to skip after
/// it (0 when none). The page after it holds the rows that come strictly after
/// `position` in stock's order (height desc, request key desc bytewise, idx asc), with the
/// first `offset` of them skipped. Stock's cursor names no chain: rows that tie with it on
/// every field (coinbase rows of several chains at one height, both sides of a transfer to
/// oneself) are skipped with it, as stock skips them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StockCursor {
    pub height: i64,
    rk: [u8; STOCK_RK_MAX],
    rk_len: u8,
    /// `None` for the `(height, request key)` form of `/txs/search`.
    pub idx: Option<i64>,
    pub offset: u64,
}

impl StockCursor {
    /// The request key text the cursor compares with (`cb` for coinbase).
    pub fn rk(&self) -> &[u8] {
        &self.rk[..usize::from(self.rk_len)]
    }

    /// Build one (tests and callers that already hold the fields). `None` when `rk` is
    /// longer than stock ever mints.
    pub fn new(height: i64, rk: &[u8], idx: Option<i64>, offset: u64) -> Option<StockCursor> {
        if rk.len() > STOCK_RK_MAX {
            return None;
        }
        let mut b = [0u8; STOCK_RK_MAX];
        b[..rk.len()].copy_from_slice(rk);
        Some(StockCursor {
            height,
            rk: b,
            rk_len: rk.len() as u8,
            idx,
            offset,
        })
    }

    /// Parse a stock token; `None` for anything that is not one. Like stock, characters
    /// outside the base64 alphabet are ignored, and a space is read as the `+` an
    /// unescaped query string turns into one.
    pub fn decode(s: &str) -> Option<StockCursor> {
        use base64::alphabet::STANDARD;
        use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
        if s.is_empty() || s.len() > 512 {
            return None;
        }
        let mut t: Vec<u8> = s
            .bytes()
            .map(|b| if b == b' ' { b'+' } else { b })
            .filter(|b| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/')
            .collect();
        if t.len() % 4 == 1 {
            t.pop();
        }
        let cfg = GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true);
        let raw = GeneralPurpose::new(&STANDARD, cfg).decode(&t).ok()?;
        let text = std::str::from_utf8(&raw).ok()?;
        let mut p = HsRead {
            s: text.as_bytes(),
            i: 0,
        };
        p.sym(b'(')?;
        p.sym(b'(')?;
        let height = i64::try_from(p.int()?).ok()?;
        p.sym(b',')?;
        let rk = p.string()?;
        let idx = if p.peek() == Some(b',') {
            p.sym(b',')?;
            Some(i64::try_from(p.int()?).ok()?)
        } else {
            None
        };
        p.sym(b')')?;
        p.sym(b',')?;
        let off = p.int()?;
        p.sym(b')')?;
        p.ws();
        if p.i != p.s.len() {
            return None;
        }
        // Stock reads an offset of 0 or less as none; one past our offset cap is refused
        // (stock never mints one: its bounded scan owes at most the offset it was given).
        let offset = u64::try_from(off.max(0)).ok()?;
        if offset > crate::compat::MAX_OFFSET {
            return None;
        }
        StockCursor::new(height, rk.as_bytes(), idx, offset)
    }

    /// True when a row at `(height, rk, idx)` comes strictly after the cursor in stock's
    /// order.
    pub fn admits(&self, height: u32, rk: &[u8], idx: i64) -> bool {
        let h = i64::from(height);
        if h != self.height {
            return h < self.height;
        }
        match rk.cmp(self.rk()) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => self.idx.is_some_and(|c| idx > c),
            std::cmp::Ordering::Greater => false,
        }
    }
}

/// The part of Haskell's `Read` syntax `show` of a stock token uses: tuples, whole numbers
/// (optionally negative, optionally in parentheses) and string literals.
struct HsRead<'a> {
    s: &'a [u8],
    i: usize,
}

impl HsRead<'_> {
    fn ws(&mut self) {
        while self.s.get(self.i).is_some_and(u8::is_ascii_whitespace) {
            self.i += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.s.get(self.i).copied()
    }

    fn sym(&mut self, c: u8) -> Option<()> {
        (self.peek()? == c).then(|| self.i += 1)
    }

    /// A decimal whole number, `-` and parentheses allowed; at most 30 digits.
    fn int(&mut self) -> Option<i128> {
        if self.peek()? == b'(' {
            self.i += 1;
            let n = self.int()?;
            self.sym(b')')?;
            return Some(n);
        }
        let neg = self.peek()? == b'-';
        if neg {
            self.i += 1;
            self.ws();
        }
        let start = self.i;
        while self.s.get(self.i).is_some_and(u8::is_ascii_digit) {
            self.i += 1;
        }
        let digits = std::str::from_utf8(&self.s[start..self.i]).ok()?;
        if digits.is_empty() || digits.len() > 30 {
            return None;
        }
        let n: i128 = digits.parse().ok()?;
        Some(if neg { -n } else { n })
    }

    /// A string literal with Haskell's escapes.
    fn string(&mut self) -> Option<String> {
        self.sym(b'"')?;
        let rest = std::str::from_utf8(&self.s[self.i..]).ok()?;
        let mut out = String::new();
        let mut it = rest.char_indices().peekable();
        while let Some((at, c)) = it.next() {
            match c {
                '"' => {
                    self.i += at + 1;
                    return Some(out);
                }
                '\\' => {
                    let (_, e) = it.next()?;
                    let num = |it: &mut std::iter::Peekable<std::str::CharIndices>, radix: u32| {
                        let mut v: u32 = 0;
                        let mut any = false;
                        while let Some(d) = it.peek().and_then(|(_, c)| c.to_digit(radix)) {
                            v = v.checked_mul(radix)?.checked_add(d)?;
                            any = true;
                            it.next();
                        }
                        any.then_some(v).and_then(char::from_u32)
                    };
                    match e {
                        '\\' | '"' | '\'' => out.push(e),
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        'r' => out.push('\r'),
                        'a' => out.push('\u{7}'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'v' => out.push('\u{b}'),
                        '&' => {}
                        'x' => out.push(num(&mut it, 16)?),
                        'o' => out.push(num(&mut it, 8)?),
                        d if d.is_ascii_digit() => {
                            let mut v = d.to_digit(10)?;
                            while let Some(d) = it.peek().and_then(|(_, c)| c.to_digit(10)) {
                                v = v.checked_mul(10)?.checked_add(d)?;
                                it.next();
                            }
                            out.push(char::from_u32(v)?);
                        }
                        _ => return None,
                    }
                }
                c if c.is_control() => return None,
                c => out.push(c),
            }
            if out.len() > STOCK_RK_MAX {
                return None;
            }
        }
        None
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
            assert!(
                matches!(Cursor::decode(bad), Err(ApiError::BadRequest(_))),
                "{bad}"
            );
        }
    }

    /// Tokens stock chainweb-data minted on mainnet (2026-11-28): account transfers and
    /// events carry `((height, request key, idx), offset)`, search `((height, request
    /// key), offset)`.
    #[test]
    fn stock_tokens_decode() {
        let real: [(&str, i64, &str, Option<i64>); 5] = [
            // The brief's real case: `/txs/account/antpoolkda`.
            (
                "KCg3NDQwNjk4LCJmZWUrbzhFT2xKYS1PRzdHaTM5N0NWcjhmd2NMTUJYcHVBMGI0MkNCOHdWblVtbyIsMSksMCk",
                7_440_698,
                "fee+o8EOlJa-OG7Gi397CVr8fwcLMBXpuA0b42CB8wVnUmo",
                Some(1),
            ),
            (
                "KCg3NDQzOTI4LCJ4WG91T3VzVlZxa1ptYi1HZXJrUE9zRE04dXYxUU5val9aaEY1T0M3MThNIiwwKSwwKQ",
                7_443_928,
                "xXouOusVVqkZmb-GerkPOsDM8uv1QNoj_ZhF5OC718M",
                Some(0),
            ),
            (
                "KCg3NDM3ODk3LCI1ZTZfaWlzS09GQ3hBQnVNVVUxZ2QzeWlKRmJDcVRNVWdreXdBMzZDMVIwIiwxKSwwKQ",
                7_437_897,
                "5e6_iisKOFCxABuMUU1gd3yiJFbCqTMUgkywA36C1R0",
                Some(1),
            ),
            // `/txs/events`, a coinbase row.
            ("KCg3NDQzOTEwLCJjYiIsMCksMCk", 7_443_910, "cb", Some(0)),
            // `/txs/search`.
            (
                "KCg3NDQzOTAzLCItZThfOElaQU9ub2RNQUFkNzJON21WN2JfWEpLVTJQSXpXS05yOXA0bDZ3IiksMCk",
                7_443_903,
                "-e8_8IZAOnodMAAd72N7mV7b_XJKU2PIzWKNr9p4l6w",
                None,
            ),
        ];
        for (tok, h, rk, idx) in real {
            let c = StockCursor::decode(tok).unwrap_or_else(|| panic!("{tok}"));
            assert_eq!(
                (c.height, c.rk(), c.idx, c.offset),
                (h, rk.as_bytes(), idx, 0)
            );
            // Not one of ours, and ours are not stock's.
            assert!(Cursor::decode(tok).is_err());
            // Padded: still the same.
            assert_eq!(StockCursor::decode(&format!("{tok}==")), Some(c));
        }
        let ours = Cursor {
            key: TxKey::new(4_000_000, 19, 7),
            sub: 3,
            fp: 0xdead_beef,
        };
        assert_eq!(StockCursor::decode(&ours.encode()), None);
        // An offset, whitespace, parentheses, negatives and escapes, as Haskell reads them.
        let c = StockCursor::decode("KCg3NDQwNjk4LCJhIiwxKSwzKQ").unwrap();
        assert_eq!(
            (c.height, c.rk(), c.idx, c.offset),
            (7_440_698, &b"a"[..], Some(1), 3)
        );
        let c = StockCursor::decode("KCggNSAsICJhYiIgLCAoLTIpICkgLCAtNCAp").unwrap();
        assert_eq!(
            (c.height, c.rk(), c.idx, c.offset),
            (5, &b"ab"[..], Some(-2), 0)
        );
        let c = StockCursor::decode("KCgtMSwiXCJ4XFwiLDIpLDAp").unwrap();
        assert_eq!((c.height, c.rk(), c.idx), (-1, &b"\"x\\"[..], Some(2)));
        // Garbage: never a panic, always `None`.
        let enc = |t: &str| base64::engine::general_purpose::STANDARD_NO_PAD.encode(t);
        for bad in [
            String::new(),
            "x".into(),
            "!!!!".into(),
            "A".repeat(3000),
            enc("((1,\"a\",1))"),
            enc("((1,\"a\",1),0) junk"),
            enc("((1,\"a\",1,2),0)"),
            enc("((1,a,1),0)"),
            enc("((1,\"a,1),0)"),
            enc("((99999999999999999999,\"a\",1),0)"),
            enc("((1,\"a\",1),10001)"),
            enc("((1,\"a\\q\",1),0)"),
            enc(&format!("((1,\"{}\",1),0)", "a".repeat(65))),
            enc("((0x10,\"a\",1),0)"),
            enc("((1,\"\\1114112\",1),0)"),
        ] {
            assert_eq!(StockCursor::decode(&bad), None, "{bad}");
        }
    }

    /// Stock's order: height desc, request key desc (bytewise), idx asc; strictly after.
    #[test]
    fn stock_cursor_admits() {
        let c = StockCursor::new(10, b"fee+x", Some(1), 0).unwrap();
        assert!(c.admits(9, b"zzz", 0));
        assert!(!c.admits(11, b"aaa", 9));
        assert!(c.admits(10, b"bm86", 0));
        assert!(c.admits(10, b"Z", 0));
        assert!(!c.admits(10, b"fee_", 0));
        assert!(!c.admits(10, b"fee+x", 1));
        assert!(!c.admits(10, b"fee+x", 0));
        assert!(c.admits(10, b"fee+x", 2));
        // The (height, request key) form skips every row of that tx.
        let t = StockCursor::new(10, b"cb", None, 0).unwrap();
        assert!(!t.admits(10, b"cb", i64::MAX));
        assert!(t.admits(10, b"Zz", 0));
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
        w.put(kdx_hot::Table::Headers, &block_key(3, 100), &v)
            .unwrap();
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
        let gone = Cursor {
            key: TxKey::new(101, 3, 1),
            sub: 0,
            fp: 5,
        };
        assert!(gone.check(&view).is_err());
        // F-19: fp 0 at an absent block is a height-done token only; a row cursor with fp 0
        // is refused.
        let forged = Cursor {
            key: TxKey::new(200, 0, 0),
            sub: 0,
            fp: 0,
        };
        assert!(forged.check(&view).is_err());
    }
}
