#![allow(unused_imports)]
use super::*;
use statrs::distribution::ContinuousCDF;

/// The CDF of the underlying LogNormal must increase with `elapsed`.
#[test]
fn probability_monotonic() {
    let shaper = ProbShaper::new(1_000, 500);
    assert!(shaper.dist.cdf(500.0) < shaper.dist.cdf(1_000.0));
    assert!(shaper.dist.cdf(1_000.0) < shaper.dist.cdf(2_000.0));
}

#[test]
fn id_is_deterministic_for_same_byte_sequence() {
    let mut a = ProbShaper::new(1_000, 200);
    let mut b = ProbShaper::new(1_000, 200);
    let _ = a.is_complete(b"hello");
    let _ = a.is_complete(b"world");
    let _ = b.is_complete(b"hello");
    let _ = b.is_complete(b"world");
    assert_eq!(a.id(), b.id());
}

#[test]
fn id_changes_when_bytes_differ() {
    let mut a = ProbShaper::new(1_000, 200);
    let mut b = ProbShaper::new(1_000, 200);
    let _ = a.is_complete(b"hello");
    let _ = b.is_complete(b"hellp");
    assert_ne!(a.id(), b.id());
}

#[test]
fn id_does_not_consume_shaper() {
    let mut s = ProbShaper::new(1_000, 200);
    let _ = s.is_complete(b"abc");
    let id1 = s.id();
    let id2 = s.id();
    assert_eq!(id1, id2);
    let _ = s.is_complete(b"def");
    let id3 = s.id();
    assert_ne!(id1, id3);
}

#[test]
fn aggregate_span_length_is_near_target() {
    let target: usize = 200;
    let stddev: usize = 80;
    let n_streams = 2_000;
    let mut total: u64 = 0;
    let mut count: u64 = 0;
    for stream_idx in 0..n_streams {
        let mut s = ProbShaper::new(target, stddev);
        // Seed the shaper with stream-unique bytes so different runs diverge.
        let seed = (stream_idx as u32).to_le_bytes();
        let _ = s.is_complete(&seed);
        let mut len: usize = 0;
        // Feed up to 50x target before giving up — guarantees termination.
        for _ in 0..(target * 50) {
            len += 1;
            if s.is_complete(&[(len & 0xff) as u8]) {
                break;
            }
        }
        total += len as u64;
        count += 1;
    }
    let mean = total as f64 / count as f64;
    let lower = target as f64 * 0.85;
    let upper = target as f64 * 1.15;
    assert!(
        mean > lower && mean < upper,
        "mean span {} not within 15% of target {}",
        mean,
        target
    );
}

#[test]
fn is_complete_never_fires_at_zero_elapsed() {
    let mut s = ProbShaper::new(1_000, 200);
    // Call repeatedly with no bytes — elapsed stays at 0, hazard stays at 0.
    for _ in 0..100 {
        assert!(!s.is_complete(&[]));
    }
}

#[test]
fn is_complete_saturates_at_huge_elapsed() {
    let target = 100usize;
    // Feed enough bytes that the CDF effectively reaches 1.0.
    let huge = vec![0xABu8; target * 1000];
    // Try across many independent shapers — saturation must fire on every one.
    for stream_idx in 0..32u32 {
        let mut s = ProbShaper::new(target, 50);
        let _ = s.is_complete(&stream_idx.to_le_bytes());
        assert!(s.is_complete(&huge), "shaper {} failed to saturate", stream_idx);
    }
}

#[test]
fn tiny_stddev_tightens_distribution() {
    fn collect_lengths(stddev: usize, n: usize) -> Vec<usize> {
        let target = 400usize;
        let mut out = Vec::with_capacity(n);
        for stream_idx in 0..n {
            let mut s = ProbShaper::new(target, stddev);
            let _ = s.is_complete(&(stream_idx as u32).to_le_bytes());
            let mut len = 0usize;
            for _ in 0..(target * 50) {
                len += 1;
                if s.is_complete(&[(len & 0xff) as u8]) {
                    break;
                }
            }
            out.push(len);
        }
        out
    }
    fn variance(xs: &[usize]) -> f64 {
        let n = xs.len() as f64;
        let mean = xs.iter().map(|&x| x as f64).sum::<f64>() / n;
        xs.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n
    }
    let tight = collect_lengths(10, 500);
    let wide = collect_lengths(500, 500);
    let v_tight = variance(&tight);
    let v_wide = variance(&wide);
    assert!(
        v_tight < v_wide,
        "tight stddev variance {} should be smaller than wide stddev variance {}",
        v_tight,
        v_wide
    );
}

#[test]
#[should_panic]
fn panics_on_zero_target() {
    let _ = ProbShaper::new(0, 100);
}

#[test]
#[should_panic]
fn panics_on_zero_stddev() {
    let _ = ProbShaper::new(1_000, 0);
}
