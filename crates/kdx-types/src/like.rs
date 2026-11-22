//! Postgres `LIKE` matching for the compat routes and the code search verifier (spec §3.8,
//! S-4). Pure: no I/O.

/// Postgres `LIKE '%' || needle || '%'`: `%` any run, `_` one character, `\` escapes.
pub struct Like {
    plain: Option<String>,
    pat: Vec<Tok>,
}

#[derive(Clone, Copy, PartialEq)]
enum Tok {
    Any,
    One,
    Ch(char),
}

impl Like {
    pub fn new(needle: &str) -> Like {
        if !needle.contains(['%', '_', '\\']) {
            return Like {
                plain: Some(needle.to_owned()),
                pat: Vec::new(),
            };
        }
        let mut pat = vec![Tok::Any];
        let mut it = needle.chars();
        while let Some(c) = it.next() {
            pat.push(match c {
                '%' => Tok::Any,
                '_' => Tok::One,
                '\\' => Tok::Ch(it.next().unwrap_or('\\')),
                c => Tok::Ch(c),
            });
        }
        pat.push(Tok::Any);
        pat.dedup_by(|a, b| *a == Tok::Any && *b == Tok::Any);
        Like { plain: None, pat }
    }

    pub fn matches(&self, s: &str) -> bool {
        if let Some(p) = &self.plain {
            return s.contains(p.as_str());
        }
        // The pattern is `%`-separated pieces with `%` at both ends (`new` adds them), so
        // matching each piece at its leftmost place after the previous one is exact: a later
        // place never leaves more room for the rest. A piece of literals only is found with a
        // substring search; one with `_` is tried at each character (F-53).
        let mut at = 0;
        for piece in self.pat.split(|t| *t == Tok::Any) {
            if piece.is_empty() {
                continue;
            }
            match find_piece(&s[at..], piece) {
                Some(end) => at += end,
                None => return false,
            }
        }
        true
    }

    /// Maximal runs of literal characters, in pattern order (wildcards split runs; an escaped
    /// wildcard is a literal). Every string the pattern matches contains each run, so the
    /// n-grams of the runs are a superset prefilter for an n-gram index (S-4).
    pub fn literal_runs(&self) -> Vec<String> {
        if let Some(p) = &self.plain {
            return if p.is_empty() {
                Vec::new()
            } else {
                vec![p.clone()]
            };
        }
        let mut out = Vec::new();
        let mut cur = String::new();
        for t in &self.pat {
            match t {
                Tok::Ch(c) => cur.push(*c),
                _ => {
                    if !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                }
            }
        }
        if !cur.is_empty() {
            out.push(cur);
        }
        out
    }
}

/// End (byte offset in `s`) of the leftmost match of `piece` (no `Tok::Any`) in `s`.
fn find_piece(s: &str, piece: &[Tok]) -> Option<usize> {
    if piece.iter().all(|t| matches!(t, Tok::Ch(_))) {
        let lit: String = piece
            .iter()
            .map(|t| match t {
                Tok::Ch(c) => *c,
                _ => unreachable!(),
            })
            .collect();
        return s.find(lit.as_str()).map(|i| i + lit.len());
    }
    let mut start = 0;
    loop {
        let mut it = s[start..].chars();
        let mut len = 0;
        let ok = piece.iter().all(|t| match (t, it.next()) {
            (Tok::One, Some(c)) => {
                len += c.len_utf8();
                true
            }
            (Tok::Ch(p), Some(c)) if *p == c => {
                len += c.len_utf8();
                true
            }
            _ => false,
        });
        if ok {
            return Some(start + len);
        }
        start += s[start..].chars().next()?.len_utf8();
    }
}

#[cfg(test)]
mod tests {
    use super::Like;

    /// F-53: a long literal run before a `%` is found with a substring search, not by
    /// retrying the run at every character (O(N×M)): 4 MiB of `a` against `a{200}b%c` would
    /// be ~800M character steps that way.
    #[test]
    fn long_literal_runs_match_in_linear_time() {
        let s = "a".repeat(4 << 20);
        let like = Like::new(&format!("{}b%c", "a".repeat(200)));
        let t0 = std::time::Instant::now();
        assert!(!like.matches(&s));
        let took = t0.elapsed();
        assert!(took < std::time::Duration::from_millis(250), "{took:?}");
    }

    #[test]
    fn literal_runs_split_at_wildcards() {
        assert_eq!(
            Like::new("coin.transfer").literal_runs(),
            vec!["coin.transfer"]
        );
        assert_eq!(
            Like::new("ab%cde_fg").literal_runs(),
            vec!["ab", "cde", "fg"]
        );
        assert_eq!(Like::new("a\\%bcd").literal_runs(), vec!["a%bcd"]);
        assert!(Like::new("%_%").literal_runs().is_empty());
        assert!(Like::new("").literal_runs().is_empty());
    }
}
