//! Relay connections (GQ-9, GQ-10): page arguments with the clamp rule, cursors, `PageInfo`
//! from one extra probe item, and a lazily computed `totalCount`.

use std::sync::Arc;

use async_graphql::SimpleObject;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures::future::BoxFuture;
use kdx_api::Cursor;

use crate::error::{bad_input, not_supported};
use crate::GraphqlSettings;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// `first` / `after`: from the start of the list.
    Forward,
    /// `last` / `before`: from the end of the list.
    Backward,
}

/// A parsed page request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<C> {
    pub dir: Dir,
    /// Items to return (fetch `n + 1` to know if there are more).
    pub n: usize,
    pub cursor: Option<C>,
}

/// A connection cursor: a TxKey-based one (`kdx_api::Cursor`, portable across instances)
/// for tx, event and transfer lists, or a block position for block lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCursor {
    Tx(Cursor),
    Block(BlockCursor),
}

impl PageCursor {
    pub fn encode(&self) -> String {
        match self {
            PageCursor::Tx(c) => c.encode(),
            PageCursor::Block(b) => b.encode(),
        }
    }

    pub fn decode(s: &str) -> async_graphql::Result<PageCursor> {
        if let Ok(b) = BlockCursor::decode(s) {
            return Ok(PageCursor::Block(b));
        }
        Cursor::decode(s)
            .map(PageCursor::Tx)
            .map_err(|_| bad_input("invalid cursor"))
    }
}

/// A position in a block list: `(height, chain)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockCursor {
    pub height: u64,
    pub chain: u8,
}

const BLOCK_TAG: u8 = 0xB1;

impl BlockCursor {
    /// base64url of `0xB1 | height (u64 BE) | chain`.
    pub fn encode(&self) -> String {
        let mut b = [0u8; 10];
        b[0] = BLOCK_TAG;
        b[1..9].copy_from_slice(&self.height.to_be_bytes());
        b[9] = self.chain;
        URL_SAFE_NO_PAD.encode(b)
    }

    pub fn decode(s: &str) -> async_graphql::Result<BlockCursor> {
        let bad = || bad_input("invalid cursor");
        if s.len() > 32 {
            return Err(bad());
        }
        let b = URL_SAFE_NO_PAD.decode(s).map_err(|_| bad())?;
        if b.len() != 10 || b[0] != BLOCK_TAG {
            return Err(bad());
        }
        Ok(BlockCursor {
            height: u64::from_be_bytes(b[1..9].try_into().map_err(|_| bad())?),
            chain: b[9],
        })
    }
}

/// The page arguments every connection field takes.
#[derive(Debug, Clone, Default)]
pub struct PageArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl PageArgs {
    pub fn new(
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> PageArgs {
        PageArgs {
            first,
            last,
            before,
            after,
        }
    }

    /// The page, read as stock's `getPaginationParams` reads it (FIX-3 part D, P4): `first`
    /// and `last` by truthiness, so `0` is the default page (20, forward) without a cursor,
    /// while with a cursor the cursor's own count is taken as given (`first ?? 20`).
    /// `first` with `last`, negative counts, `after` with `last` or `before` with `first` are
    /// `BAD_USER_INPUT`. No count is clamped: stock's complexity gate (`stock_limits`, run
    /// before any resolver) bounds `first` and `last`, and a count above the safety cap
    /// [`crate::MAX_PAGE_ROWS`] is refused ([`crate::error::page_limit`]).
    pub fn parse(&self, s: &GraphqlSettings) -> async_graphql::Result<Page<PageCursor>> {
        if self.first.is_some_and(|n| n < 0) || self.last.is_some_and(|n| n < 0) {
            return Err(bad_input("first and last must not be negative"));
        }
        let first = self.first.filter(|n| *n != 0);
        let last = self.last.filter(|n| *n != 0);
        let (dir, n) = match (first, last) {
            (Some(_), Some(_)) => return Err(bad_input("pass first or last, not both")),
            _ if self.after.is_some() && last.is_none() => (
                Dir::Forward,
                self.first.map_or(s.default_page, |n| n as usize),
            ),
            _ if self.before.is_some() && self.after.is_none() && first.is_none() => (
                Dir::Backward,
                self.last.map_or(s.default_page, |n| n as usize),
            ),
            (Some(f), None) => (Dir::Forward, f as usize),
            (None, Some(l)) => (Dir::Backward, l as usize),
            (None, None) => (Dir::Forward, s.default_page),
        };
        if n > crate::MAX_PAGE_ROWS {
            return Err(crate::error::page_limit());
        }
        let cursor = match (dir, &self.after, &self.before) {
            (_, Some(_), Some(_)) => return Err(bad_input("pass after or before, not both")),
            (Dir::Forward, Some(a), None) => Some(PageCursor::decode(a)?),
            (Dir::Backward, None, Some(b)) => Some(PageCursor::decode(b)?),
            (_, None, None) => None,
            (Dir::Forward, None, Some(_)) => return Err(bad_input("before goes with last")),
            (Dir::Backward, Some(_), None) => return Err(bad_input("after goes with first")),
        };
        Ok(Page { dir, n, cursor })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, SimpleObject)]
pub struct PageInfo {
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
}

/// Turn the rows of a walk into edges and `PageInfo`. `rows` are in walk order and hold up
/// to `page.n + 1` items (the extra one is the probe). A backward page is returned in list
/// order (reversed), as Relay asks.
pub fn finish<T, C: Fn(&T) -> String>(
    mut rows: Vec<T>,
    page: &Page<PageCursor>,
    cursor_of: C,
) -> (Vec<(String, T)>, PageInfo) {
    let more = rows.len() > page.n;
    rows.truncate(page.n);
    if page.dir == Dir::Backward {
        rows.reverse();
    }
    let edges: Vec<(String, T)> = rows.into_iter().map(|r| (cursor_of(&r), r)).collect();
    let had_cursor = page.cursor.is_some();
    let (next, prev) = match page.dir {
        Dir::Forward => (more, had_cursor),
        Dir::Backward => (had_cursor, more),
    };
    let info = PageInfo {
        start_cursor: edges.first().map(|e| e.0.clone()),
        end_cursor: edges.last().map(|e| e.0.clone()),
        has_next_page: next,
        has_previous_page: prev,
    };
    (edges, info)
}

/// `totalCount` of a connection (GQ-10): known up front, computed only when selected, or
/// not served. A deferred count answers `(n, exact)`; an inexact one (its time share ran out)
/// is a lower bound, flagged in the response `extensions` (R-P5P9-14 COUNT_TIMEOUT).
/// A deferred count: `(items, exact)`.
pub type CountFn = dyn Fn() -> BoxFuture<'static, async_graphql::Result<(u64, bool)>> + Send + Sync;

#[derive(Clone, Default)]
pub enum Total {
    #[default]
    Unsupported,
    Exact(u64),
    Deferred(Arc<CountFn>),
}

impl Total {
    pub async fn resolve(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<i32> {
        let n = match self {
            Total::Unsupported => return Err(not_supported("totalCount")),
            Total::Exact(n) => *n,
            Total::Deferred(f) => {
                let (n, exact) = f().await?;
                if !exact {
                    crate::flags::mark(ctx, crate::flags::Flag::CountLowerBound);
                }
                n
            }
        };
        Ok(n.min(i32::MAX as u64) as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s() -> GraphqlSettings {
        GraphqlSettings::default()
    }

    /// FIX-3 part C: no clamp (stock has none; its complexity gate bounds the page), and
    /// stock's 20 when `first`/`last` are absent.
    #[test]
    fn page_sizes_are_served_as_asked() {
        let p = PageArgs::new(None, None, Some(1000), None)
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Forward, 1000));
        let p = PageArgs::new(None, None, None, Some(5))
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Backward, 5));
        let p = PageArgs::default().parse(&s()).unwrap();
        assert_eq!((p.dir, p.n), (Dir::Forward, 20));
    }

    /// FIX-3 part D (S3): a hard safety cap of [`crate::MAX_PAGE_ROWS`] rows per page, after
    /// stock's gate; above it a `LIMIT` refusal named `page` (stock answers such pages slowly
    /// or times out).
    #[test]
    fn pages_above_the_safety_cap_are_refused() {
        let p = PageArgs::new(None, None, Some(1000), None)
            .parse(&s())
            .unwrap();
        assert_eq!(p.n, crate::MAX_PAGE_ROWS);
        for a in [
            PageArgs::new(None, None, Some(1001), None),
            PageArgs::new(None, None, None, Some(1_000_000)),
            PageArgs::new(None, None, Some(i32::MAX), None),
        ] {
            let e = a.parse(&s()).unwrap_err();
            let x = e.extensions.as_ref().unwrap();
            assert_eq!(
                x.get("code"),
                Some(&async_graphql::Value::from("LIMIT")),
                "{a:?}"
            );
            assert_eq!(
                x.get("limit"),
                Some(&async_graphql::Value::from("page")),
                "{a:?}"
            );
            assert!(e.message.contains("1000"), "{}", e.message);
        }
    }

    /// FIX-3 part D (P4): stock's `getPaginationParams` reads `first`/`last` by truthiness, so
    /// `0` is the default page of 20 (forward) without a cursor; with a cursor stock takes
    /// `first ?? 20`, so 0 stays 0.
    #[test]
    fn zero_is_stock_s_default_page() {
        let p = PageArgs::new(None, None, Some(0), None)
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Forward, 20));
        let p = PageArgs::new(None, None, None, Some(0))
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Forward, 20));
        // `first: 0, last: 5`: stock's "not both" check reads truthiness too.
        let p = PageArgs::new(None, None, Some(0), Some(5))
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Backward, 5));
        let c = BlockCursor {
            height: 1,
            chain: 0,
        }
        .encode();
        let p = PageArgs::new(Some(c.clone()), None, Some(0), None)
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Forward, 0));
        let p = PageArgs::new(None, Some(c), None, None)
            .parse(&s())
            .unwrap();
        assert_eq!((p.dir, p.n), (Dir::Backward, 20));
    }

    #[test]
    fn bad_page_arguments() {
        let bc = BlockCursor {
            height: 1,
            chain: 0,
        }
        .encode();
        for a in [
            PageArgs::new(None, None, Some(1), Some(1)),
            PageArgs::new(None, None, Some(-1), None),
            PageArgs::new(Some(bc.clone()), Some(bc.clone()), None, None),
            PageArgs::new(Some(bc.clone()), None, None, Some(2)),
            PageArgs::new(None, Some(bc), Some(2), None),
            PageArgs::new(Some("garbage!".into()), None, Some(2), None),
        ] {
            assert!(a.parse(&s()).is_err(), "{a:?}");
        }
    }

    #[test]
    fn block_cursor_round_trip() {
        let c = BlockCursor {
            height: 6_951_337,
            chain: 19,
        };
        assert_eq!(
            PageCursor::decode(&c.encode()).unwrap(),
            PageCursor::Block(c)
        );
    }

    #[test]
    fn page_info_from_the_probe() {
        let page = Page {
            dir: Dir::Forward,
            n: 2,
            cursor: None,
        };
        let (e, i) = finish(vec![1, 2, 3], &page, |x| x.to_string());
        assert_eq!(e.len(), 2);
        assert!(i.has_next_page && !i.has_previous_page);
        assert_eq!(i.start_cursor.as_deref(), Some("1"));
        let page = Page {
            dir: Dir::Backward,
            n: 2,
            cursor: Some(PageCursor::Block(BlockCursor {
                height: 9,
                chain: 0,
            })),
        };
        let (e, i) = finish(vec![9, 8], &page, |x| x.to_string());
        assert_eq!(e.iter().map(|x| x.1).collect::<Vec<_>>(), vec![8, 9]);
        assert!(i.has_next_page && !i.has_previous_page);
    }
}
