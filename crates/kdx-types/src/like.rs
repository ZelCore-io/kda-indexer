//! Postgres `LIKE` matching for the compat routes and the code search verifier (spec §3.8,
//! S-4). Pure: no I/O.

/// Postgres `LIKE '%' || needle || '%'`: `%` any run, `_` one character, `\` escapes.
pub struct Like {
    plain: Option<String>,
    pat: Vec<Tok>,
    /// The `%`-separated pieces of `pat`, compiled once (F-53).
    pieces: Vec<Piece>,
}

/// One `%`-free piece of the pattern.
enum Piece {
    /// Literals only: a substring search.
    Lit(String),
    /// With `_`: Shift-And over `words` 64-bit words, one bit per pattern position.
    Wild(Bitap),
}

/// Shift-And masks of a piece: bit k of a char's mask is set when position k accepts it.
struct Bitap {
    words: usize,
    /// Bit of the last position (in word `words - 1`).
    last: u64,
    /// Positions that accept any char (`_`).
    any: Vec<u64>,
    /// `any` plus the positions of each ASCII char.
    ascii: Vec<Vec<u64>>,
    /// `any` plus the positions of each other char.
    other: Vec<(char, Vec<u64>)>,
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
                pieces: Vec::new(),
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
        let pieces = pat
            .split(|t| *t == Tok::Any)
            .filter(|p| !p.is_empty())
            .map(Piece::new)
            .collect();
        Like {
            plain: None,
            pat,
            pieces,
        }
    }

    pub fn matches(&self, s: &str) -> bool {
        if let Some(p) = &self.plain {
            return s.contains(p.as_str());
        }
        // The pattern is `%`-separated pieces with `%` at both ends (`new` adds them), so
        // matching each piece at its leftmost place after the previous one is exact: a later
        // place never leaves more room for the rest. Every piece is found in one pass over
        // the rest of `s`, so a match is O(N × pieces) (F-53).
        let mut at = 0;
        for piece in &self.pieces {
            match piece.find(&s[at..]) {
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

impl Piece {
    fn new(toks: &[Tok]) -> Piece {
        let lit: Option<String> = toks
            .iter()
            .map(|t| match t {
                Tok::Ch(c) => Some(*c),
                _ => None,
            })
            .collect();
        if let Some(lit) = lit {
            return Piece::Lit(lit);
        }
        let words = toks.len().div_ceil(64);
        let mut any = vec![0u64; words];
        for (k, t) in toks.iter().enumerate() {
            if *t == Tok::One {
                any[k / 64] |= 1 << (k % 64);
            }
        }
        let mut ascii = vec![any.clone(); 128];
        let mut other: Vec<(char, Vec<u64>)> = Vec::new();
        for (k, t) in toks.iter().enumerate() {
            if let Tok::Ch(c) = *t {
                let m = if c.is_ascii() {
                    &mut ascii[c as usize]
                } else {
                    match other.iter().position(|(o, _)| *o == c) {
                        Some(i) => &mut other[i].1,
                        None => {
                            other.push((c, any.clone()));
                            &mut other.last_mut().expect("pushed").1
                        }
                    }
                };
                m[k / 64] |= 1 << (k % 64);
            }
        }
        Piece::Wild(Bitap {
            words,
            last: 1 << ((toks.len() - 1) % 64),
            any,
            ascii,
            other,
        })
    }

    /// End (byte offset in `s`) of the leftmost match in `s`. A piece has a fixed length in
    /// chars, so the leftmost end is the leftmost start too.
    fn find(&self, s: &str) -> Option<usize> {
        let b = match self {
            Piece::Lit(l) => return s.find(l.as_str()).map(|i| i + l.len()),
            Piece::Wild(b) => b,
        };
        // Bit k of `d`: the last k + 1 chars match the first k + 1 positions.
        let mut d = vec![0u64; b.words];
        for (i, c) in s.char_indices() {
            let m = if c.is_ascii() {
                &b.ascii[c as usize]
            } else {
                b.other
                    .iter()
                    .find(|(o, _)| *o == c)
                    .map_or(&b.any, |(_, m)| m)
            };
            let mut carry = 1u64;
            for (w, mw) in d.iter_mut().zip(m) {
                let next = *w >> 63;
                *w = ((*w << 1) | carry) & mw;
                carry = next;
            }
            if d[b.words - 1] & b.last != 0 {
                return Some(i + c.len_utf8());
            }
        }
        None
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

    /// F-53 review: a piece mixing `_` with literals is matched by Shift-And in one pass,
    /// not re-checked from every start: `a{200}_b%c` over 4 MiB of `a` would be ~800M steps.
    #[test]
    fn long_pieces_with_underscore_match_in_linear_time() {
        let s = "a".repeat(4 << 20);
        for pat in [
            format!("{}_b%c", "a".repeat(200)),
            format!("_{}b", "a".repeat(250)),
        ] {
            let like = Like::new(&pat);
            let t0 = std::time::Instant::now();
            assert!(!like.matches(&s));
            let took = t0.elapsed();
            assert!(took < std::time::Duration::from_millis(250), "{took:?}");
        }
    }

    /// The matcher before F-53 (greedy with backtracking to the last `%`), as the reference.
    fn reference(needle: &str, s: &str) -> bool {
        #[derive(Clone, Copy, PartialEq)]
        enum T {
            Any,
            One,
            Ch(char),
        }
        let mut pat = vec![T::Any];
        let mut it = needle.chars();
        while let Some(c) = it.next() {
            pat.push(match c {
                '%' => T::Any,
                '_' => T::One,
                '\\' => T::Ch(it.next().unwrap_or('\\')),
                c => T::Ch(c),
            });
        }
        pat.push(T::Any);
        let t: Vec<char> = s.chars().collect();
        let (mut i, mut j) = (0usize, 0usize);
        let mut star: Option<(usize, usize)> = None;
        while i < t.len() {
            match pat.get(j) {
                Some(T::Any) => {
                    star = Some((j, i));
                    j += 1;
                }
                Some(T::One) => {
                    i += 1;
                    j += 1;
                }
                Some(T::Ch(c)) if *c == t[i] => {
                    i += 1;
                    j += 1;
                }
                _ => match star {
                    Some((sj, si)) => {
                        star = Some((sj, si + 1));
                        j = sj + 1;
                        i = si + 1;
                    }
                    None => return false,
                },
            }
        }
        pat[j..].iter().all(|t| *t == T::Any)
    }

    /// The new matcher answers exactly as the old one on random needles and subjects
    /// (pieces shorter and longer than one 64-bit mask word, non-ASCII, escapes).
    #[test]
    fn matches_like_the_previous_matcher() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut rnd = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let alpha = ['a', 'b', 'é', '_', '%', '\\'];
        for round in 0..20_000 {
            let long = round % 10 == 0;
            let nl = rnd(if long { 150 } else { 8 }) as usize;
            let needle: String = (0..nl)
                .map(|_| alpha[rnd(if long { 4 } else { 6 }) as usize])
                .collect();
            let sl = rnd(if long { 400 } else { 16 }) as usize;
            let hay: String = (0..sl).map(|_| alpha[rnd(3) as usize]).collect();
            assert_eq!(
                Like::new(&needle).matches(&hay),
                reference(&needle, &hay),
                "needle {needle:?} hay {hay:?}"
            );
        }
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
