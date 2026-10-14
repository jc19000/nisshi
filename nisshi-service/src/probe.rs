// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! kr: P10. Where a request spends its time between the socket and the
//! storage engine: lock-free histograms per stage of the connection loop, for
//! Produce, Fetch and everything else. Measurement only.

use std::{
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Sub-buckets per power of two: a quantile is known within 1/8 of its value.
const SUB_BITS: u32 = 3;
const BUCKETS: usize = 64 << SUB_BITS;

/// The stages of one request on its connection, in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    /// Previous response written to the first byte of this request: the
    /// connection had nothing to serve.
    Idle,
    /// First byte to the whole frame read.
    Read,
    /// Frame bytes to the decoded request (a `spawn_blocking` round trip).
    Decode,
    /// The route service: the handler and the storage engine.
    Handle,
    /// Response body to its bytes (a `spawn_blocking` round trip).
    Encode,
    /// Response bytes written and flushed.
    Write,
}

const STAGES: [(Stage, &str); 6] = [
    (Stage::Idle, "idle"),
    (Stage::Read, "read"),
    (Stage::Decode, "decode"),
    (Stage::Handle, "handle"),
    (Stage::Encode, "encode"),
    (Stage::Write, "write"),
];

const CLASSES: [&str; 3] = ["produce", "fetch", "other"];

struct Histogram {
    buckets: Vec<AtomicU64>,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn index(ns: u64) -> usize {
        if ns < (1 << SUB_BITS) {
            return ns as usize;
        }
        let exp = 63 - ns.leading_zeros();
        let sub = (ns >> (exp - SUB_BITS)) & ((1 << SUB_BITS) - 1);
        (((exp - SUB_BITS + 1) << SUB_BITS) as usize) + sub as usize
    }

    /// Inclusive upper bound of bucket `i`.
    fn upper(i: usize) -> u64 {
        let per = 1usize << SUB_BITS;
        if i < per {
            return i as u64;
        }
        let exp = (i / per) as u32 + SUB_BITS - 1;
        let sub = (i % per) as u64;
        let width = 1u64 << (exp - SUB_BITS);
        ((1u64 << exp) + sub * width).saturating_add(width - 1)
    }

    fn record(&self, ns: u64) {
        if let Some(bucket) = self.buckets.get(Self::index(ns)) {
            _ = bucket.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn count(&self) -> u64 {
        self.buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .sum()
    }

    fn quantile(&self, q: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let rank = ((total as f64 * q).ceil() as u64).clamp(1, total);
        let mut seen = 0;
        for (i, bucket) in self.buckets.iter().enumerate() {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= rank {
                return Self::upper(i);
            }
        }
        u64::MAX
    }

    fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

static PROBE: LazyLock<Vec<Vec<Histogram>>> = LazyLock::new(|| {
    CLASSES
        .iter()
        .map(|_| STAGES.iter().map(|_| Histogram::new()).collect())
        .collect()
});

fn class(api_key: i16) -> usize {
    match api_key {
        0 => 0,
        1 => 1,
        _ => 2,
    }
}

/// The api key of a request frame: the two bytes after its size prefix.
pub fn api_key_of(frame: &[u8]) -> i16 {
    frame
        .get(4..6)
        .and_then(|key| key.try_into().ok())
        .map_or(-1, i16::from_be_bytes)
}

pub fn now() -> Instant {
    Instant::now()
}

pub fn record(api_key: i16, stage: Stage, elapsed: Duration) {
    let at = STAGES
        .iter()
        .position(|(candidate, _)| *candidate == stage)
        .unwrap_or_default();
    if let Some(histogram) = PROBE.get(class(api_key)).and_then(|stages| stages.get(at)) {
        histogram.record(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }
}

/// One line per request class seen since the last reset, durations in
/// microseconds (`p50/p90/p99`).
pub fn report() -> Vec<String> {
    CLASSES
        .iter()
        .zip(PROBE.iter())
        .filter_map(|(name, stages)| {
            let n = stages.get(3).map_or(0, Histogram::count);
            (n > 0).then(|| {
                let stages = STAGES
                    .iter()
                    .zip(stages)
                    .map(|((_, stage), histogram)| {
                        let us = |q| histogram.quantile(q) / 1000;
                        format!("{stage}_us[{}/{}/{}]", us(0.5), us(0.9), us(0.99))
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{name} n={n} {stages}")
            })
        })
        .collect()
}

pub fn reset() {
    for histogram in PROBE.iter().flatten() {
        histogram.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_is_read_after_the_size_prefix() {
        assert_eq!(1, api_key_of(&[0, 0, 0, 9, 0, 1, 0, 17]));
        assert_eq!(-1, api_key_of(&[0, 0, 0, 9, 0]));
    }

    #[test]
    fn buckets_bound_their_values() {
        for ns in (0..4096u64).chain([1 << 20, u64::MAX / 2, u64::MAX]) {
            let i = Histogram::index(ns);
            assert!(i < BUCKETS);
            assert!(Histogram::upper(i) >= ns);
        }
    }

    #[test]
    fn quantiles() {
        let histogram = Histogram::new();
        assert_eq!(0, histogram.quantile(0.5));
        for us in 1..=1000u64 {
            histogram.record(us * 1000);
        }
        let p50 = histogram.quantile(0.5);
        assert!((500_000..=562_500).contains(&p50), "{p50}");
        histogram.reset();
        assert_eq!(0, histogram.count());
    }
}
