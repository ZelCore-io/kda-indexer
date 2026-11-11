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
        // Greedy match with backtracking to the last `%` (linear in practice).
        let t: Vec<char> = s.chars().collect();
        let (mut i, mut j) = (0usize, 0usize);
        let mut star: Option<(usize, usize)> = None;
        while i < t.len() {
            match self.pat.get(j) {
                Some(Tok::Any) => {
                    star = Some((j, i));
                    j += 1;
                }
                Some(Tok::One) => {
                    i += 1;
                    j += 1;
                }
                Some(Tok::Ch(c)) if *c == t[i] => {
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
        self.pat[j..].iter().all(|t| *t == Tok::Any)
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

#[cfg(test)]
mod tests {
    use super::Like;

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
