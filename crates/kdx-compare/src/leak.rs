//! R-P5P9-86 (the leak line), FVH-E2.8: the FV memory criterion over jemalloc `allocated`
//! samples. The first hour after the process start is warm-up. From start + 60 min to the
//! close, the line is at most +100 MB, and FV fails on any step over 100 MB after the
//! warm-up: a sample more than 100 MB above the lowest sample before it (from the first
//! sample at or after start + 60 min). No sample after the warm-up fails too: the line was
//! not measured.

use anyhow::Context;

/// The warm-up after the process start, in seconds.
pub const WARM_UP_SECS: u64 = 3_600;
/// The largest rise allowed after the warm-up, in bytes (100 MB, decimal).
pub const MAX_RISE: u64 = 100_000_000;

/// One `allocated` sample: (unix seconds, bytes).
pub type Sample = (u64, u64);

/// Reads samples, one per line: `<YYYY-MM-DDTHH:MM:SSZ> <allocated bytes>`. Blank lines and
/// `#` comments are skipped; any other line that does not read is an error.
pub fn parse_samples(text: &str) -> anyhow::Result<Vec<Sample>> {
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split_whitespace();
        let (Some(t), Some(b), None) = (it.next(), it.next(), it.next()) else {
            anyhow::bail!("line {}: not `<time> <bytes>`: {line}", n + 1);
        };
        let t = crate::summary::parse_since(t).with_context(|| format!("line {}", n + 1))?;
        let b: u64 = b
            .parse()
            .with_context(|| format!("line {}: allocated bytes", n + 1))?;
        out.push((t, b));
    }
    out.sort();
    Ok(out)
}

/// The verdict: `Ok(text)` when the line holds, `Err(text)` when it fails; the text gives
/// the baseline, the close and the largest rise after the warm-up.
pub fn verdict(start: u64, samples: &[Sample]) -> Result<String, String> {
    let mb = |b: u64| format!("{:.1} MB", b as f64 / 1e6);
    let iso = crate::report::iso_secs;
    let after: Vec<Sample> = samples
        .iter()
        .copied()
        .filter(|(t, _)| *t >= start + WARM_UP_SECS)
        .collect();
    let Some(&(t0, base)) = after.first() else {
        return Err(format!(
            "leak line: FAIL, no sample at or after start + 60 min ({})",
            iso(start + WARM_UP_SECS)
        ));
    };
    let (mut low, mut worst) = (base, (0u64, t0, base));
    for &(t, b) in &after {
        low = low.min(b);
        let rise = b.saturating_sub(low);
        if rise > worst.0 {
            worst = (rise, t, low);
        }
    }
    let &(tc, close) = after.last().unwrap_or(&(t0, base));
    let text = format!(
        "leak line: baseline {} at {} (start + 60 min), close {} at {} ({:+.1} MB); largest \
         rise after the warm-up {} at {} over {} (limit {})",
        mb(base),
        iso(t0),
        mb(close),
        iso(tc),
        (close as f64 - base as f64) / 1e6,
        mb(worst.0),
        iso(worst.1),
        mb(worst.2),
        mb(MAX_RISE)
    );
    if worst.0 > MAX_RISE {
        Err(format!("{text}: FAIL"))
    } else {
        Ok(format!("{text}: PASS"))
    }
}

/// R-P5P9-95 m-11: the line's span. `starts`: the process start and each restart (each
/// begins its own warm-up and baseline); `end`: the close; `interval`: the sampler's
/// interval in seconds.
#[derive(Debug, Clone)]
pub struct Line {
    pub starts: Vec<u64>,
    pub end: u64,
    pub interval: u64,
}

/// The verdict over `line`: each run (from a start to the next start, the last to the
/// close) is judged by [`verdict`] on its own samples, after its own warm-up. The line is
/// measured only when, in each run, the samples after its warm-up begin within two
/// intervals of it, have no gap over two intervals, and (the last run) reach within two
/// intervals of the close; else it fails as not measured.
pub fn verdict_line(line: &Line, samples: &[Sample]) -> Result<String, String> {
    let iso = crate::report::iso_secs;
    let mut starts = line.starts.clone();
    starts.sort_unstable();
    starts.dedup();
    if starts.is_empty() {
        return Err("leak line: FAIL, no start".into());
    }
    let slack = 2 * line.interval.max(1);
    let mut texts = Vec::new();
    let mut failed = false;
    for (i, &s) in starts.iter().enumerate() {
        let until = starts.get(i + 1).copied().unwrap_or(line.end);
        let run: Vec<Sample> = samples
            .iter()
            .copied()
            .filter(|(t, _)| {
                *t >= s && *t < until.max(s + 1) || (i + 1 == starts.len() && *t == until)
            })
            .collect();
        let from = s + WARM_UP_SECS;
        if from >= until {
            texts.push(format!("run from {}: ends inside its warm-up", iso(s)));
            continue;
        }
        let after: Vec<u64> = run.iter().map(|x| x.0).filter(|t| *t >= from).collect();
        let mut gaps = Vec::new();
        match (after.first(), after.last()) {
            (Some(&f), Some(&l)) => {
                if f > from + slack {
                    gaps.push((from, f));
                }
                gaps.extend(
                    after
                        .windows(2)
                        .filter(|w| w[1] - w[0] > slack)
                        .map(|w| (w[0], w[1])),
                );
                if until > l + slack {
                    gaps.push((l, until));
                }
            }
            _ => gaps.push((from, until)),
        }
        if let Some((a, b)) = gaps.first() {
            failed = true;
            texts.push(format!(
                "run from {}: not measured from {} to {} (more than two intervals of {} s)",
                iso(s),
                iso(*a),
                iso(*b),
                line.interval
            ));
            continue;
        }
        match verdict(s, &run) {
            Ok(t) => texts.push(format!("run from {}: {t}", iso(s))),
            Err(t) => {
                failed = true;
                texts.push(format!("run from {}: {t}", iso(s)));
            }
        }
    }
    let text = texts.join("; ");
    if failed {
        Err(format!("{text}: FAIL"))
    } else {
        Ok(format!("{text}: PASS"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1_000_000;

    /// Minute samples from `start` for `mins` minutes: `base` plus each step `(minute, MB)`
    /// from that minute on.
    fn samples(start: u64, mins: u64, base: u64, steps: &[(u64, u64)]) -> Vec<Sample> {
        (0..=mins)
            .map(|m| {
                let up: u64 = steps.iter().filter(|(at, _)| m >= *at).map(|s| s.1).sum();
                (start + m * 60, base + up * MB)
            })
            .collect()
    }

    /// R-P5P9-86: CG-5's shape (a +241 MB step 47 min after the start, flat afterwards over
    /// a 3 h soak) and CG-4's (the step at +2 min) fall in the warm-up: PASS. A second step
    /// of +120 MB after the warm-up fails, so does a slow rise of 101 MB, and a run with no
    /// sample after start + 60 min fails as not measured. A rise of exactly 100 MB passes.
    #[test]
    fn the_leak_line_is_measured_after_the_warm_up() {
        let start = crate::summary::parse_since("2026-11-15T08:16:00Z").unwrap();
        let base = 908 * MB;
        assert!(verdict(start, &samples(start, 180, base, &[(47, 241)])).is_ok());
        assert!(verdict(start, &samples(start, 240, base, &[(2, 241)])).is_ok());
        let two = verdict(start, &samples(start, 180, base, &[(47, 241), (90, 120)]));
        let why = two.unwrap_err();
        assert!(
            why.contains("largest rise after the warm-up 120.0 MB"),
            "{why}"
        );
        let slow: Vec<Sample> = samples(start, 180, base, &[(47, 241)])
            .into_iter()
            .map(|(t, b)| {
                (
                    t,
                    b + ((t - start) / 60).saturating_sub(60) * (MB * 101 / 120),
                )
            })
            .collect();
        assert!(verdict(start, &slow).is_err());
        assert!(verdict(start, &samples(start, 59, base, &[])).is_err());
        assert!(verdict(start, &samples(start, 180, base, &[(100, 100)])).is_ok());
        // A dip and a later rise of 101 MB from the dip fails though the close is lower than
        // the baseline + 100 MB.
        let dip = samples(start, 180, base, &[(70, 50), (100, 0)]);
        let dip: Vec<Sample> = dip
            .into_iter()
            .map(|(t, b)| match (t - start) / 60 {
                80..=99 => (t, base - 60 * MB),
                m if m >= 100 => (t, base + 41 * MB + MB),
                _ => (t, b),
            })
            .collect();
        assert!(verdict(start, &dip).is_err());
    }

    #[test]
    fn samples_parse_and_refuse_bad_lines() {
        let s = parse_samples("# allocated\n2026-11-15T09:16:00Z 908000000\n\n").unwrap();
        assert_eq!(s.len(), 1);
        assert!(parse_samples("2026-11-15T09:16:00Z 9e8").is_err());
        assert!(parse_samples("908000000").is_err());
    }

    /// R-P5P9-95 m-11: a restart begins its own warm-up (its +241 MB step passes with the
    /// restart given, fails without); a sampler that stops before the close, or a gap of
    /// more than two intervals, fails as not measured.
    #[test]
    fn f_m11_restarts_and_coverage() {
        let start = crate::summary::parse_since("2026-11-15T08:16:00Z").unwrap();
        let base = 908 * MB;
        let restart = start + 240 * 60;
        let s = samples(start, 480, base, &[(240 + 47, 241)]);
        let end = start + 480 * 60;
        let line = |starts: Vec<u64>, end: u64| Line {
            starts,
            end,
            interval: 60,
        };
        assert!(verdict_line(&line(vec![start, restart], end), &s).is_ok());
        assert!(verdict_line(&line(vec![start], end), &s).is_err());
        let flat = samples(start, 180, base, &[]);
        let why = verdict_line(&line(vec![start], start + 24 * 3_600), &flat).unwrap_err();
        assert!(why.contains("not measured"), "{why}");
        let gap: Vec<Sample> = samples(start, 480, base, &[])
            .into_iter()
            .filter(|(t, _)| !(start + 120 * 60..start + 180 * 60).contains(t))
            .collect();
        let why = verdict_line(&line(vec![start], end), &gap).unwrap_err();
        assert!(why.contains("not measured"), "{why}");
        assert!(verdict_line(&line(vec![start], end), &samples(start, 480, base, &[])).is_ok());
    }
}
