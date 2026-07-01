//! I2 (#19): re-rank p99 when the raw tier lives on SSD instead of RAM — the decisive integration-leg
//! measurement. The differentiation p99 (0.78ms) was measured with raw in RAM; here we move raw to a
//! file and re-rank by `pread`, in three conditions:
//!   1. raw in RAM        (baseline = IvfPq::search_rerank)
//!   2. raw on file, page-cache WARM
//!   3. raw on file, UNCACHED (fcntl F_NOCACHE — macOS O_DIRECT-equivalent, reads hit the device)
//!
//! Candidate generation is always the hot PQ path (IvfPq::search); only the re-rank read source varies.
//! Run: `cargo run --release --example ann_ssd_p99 -p stroma-core`

use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::time::Instant;
use stroma_core::ivf::IvfPq;
use stroma_core::vector::sqdist;

/// Bypass the OS buffer cache for this fd so reads hit the device (macOS `F_NOCACHE` = O_DIRECT-equiv).
/// Returns false on platforms without it (the uncached measurement is then skipped).
#[cfg(target_os = "macos")]
fn set_nocache(file: &File) -> bool {
    use std::os::unix::io::AsRawFd;
    // SAFETY: fcntl on a valid open fd with a defined command; the return value is checked.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    rc != -1
}
#[cfg(not(target_os = "macos"))]
fn set_nocache(_file: &File) -> bool {
    false
}

const N: usize = 100_000; // set higher (e.g. 500_000) for the A1 representative-scale run
const DIM: usize = 768;
const M: usize = 96;
const TRAIN: usize = 20_000;
const NC: usize = 3000;
const NOISE: f32 = 0.35;
const R: usize = 256; // re-rank candidate depth (matches IR operating point)
const K: usize = 10;
const NPROBE: usize = 8; // operating point
const ROW: usize = DIM * 4; // bytes per raw vector

fn splitmix(s: &mut u64) -> f32 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) as f32 / u64::MAX as f32) * 2.0 - 1.0
}

fn centers() -> Vec<Vec<f32>> {
    let mut s = 0xC0FF_EE00_1234_5678u64;
    (0..NC)
        .map(|_| (0..DIM).map(|_| splitmix(&mut s)).collect())
        .collect()
}

fn gen_vecs(n: usize, seed: u64, ctr: &[Vec<f32>]) -> Vec<Vec<f32>> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            let c = &ctr[(splitmix(&mut s).abs() * NC as f32) as usize % NC];
            (0..DIM).map(|i| c[i] + splitmix(&mut s) * NOISE).collect()
        })
        .collect()
}

fn read_row(file: &File, node: u64, buf: &mut [u8], out: &mut Vec<f32>) {
    file.read_at(buf, node * ROW as u64).unwrap();
    out.clear();
    for i in 0..DIM {
        out.push(f32::from_le_bytes(
            buf[i * 4..i * 4 + 4].try_into().unwrap(),
        ));
    }
}

fn percentiles(mut lat: Vec<f64>, label: &str) {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    let verdict = if p(0.99) < 2.0 { "PASS" } else { "FAIL" };
    println!(
        "  {label:<34} p50={:.3}ms p99={:.3}ms max={:.3}ms  [<2ms] → {verdict}",
        p(0.50),
        p(0.99),
        lat[lat.len() - 1]
    );
}

fn main() {
    let ctr = centers();
    let data = gen_vecs(N, 42, &ctr);
    let mut idx = IvfPq::new(DIM, IvfPq::suggested_nlist(N), M);
    idx.train(&data[..TRAIN]);
    idx.add_batch(
        data.iter()
            .enumerate()
            .map(|(i, v)| (i as u64, i as u64, v.clone(), (i % 2) as u32))
            .collect(),
    );
    println!("=== I2/#19 raw-tier p99: raw in RAM vs on SSD ({N} vec × {DIM}d, R={R}) ===");

    // write raw tier to a file (flat rows)
    let path = std::env::temp_dir().join("stroma_raw_tier.bin");
    {
        let mut f = File::create(&path).unwrap();
        let mut bytes = Vec::with_capacity(N * ROW);
        for v in &data {
            for x in v {
                bytes.extend_from_slice(&x.to_le_bytes());
            }
        }
        f.write_all(&bytes).unwrap();
        f.sync_all().unwrap();
    }
    println!(
        "raw file    : {:.0} MB at {}",
        (N * ROW) as f64 / 1e6,
        path.display()
    );

    let warm = gen_vecs(3000, 123, &ctr);
    let authz = |l: u32| l == 0;
    let keep = |n: u64| n.is_multiple_of(2);

    // condition 1: raw in RAM (baseline, module path)
    let mut lat = Vec::with_capacity(warm.len());
    for q in &warm {
        let t = Instant::now();
        let _ = idx.search_rerank(q, K, NPROBE, R, None, authz, keep);
        lat.push(t.elapsed().as_secs_f64() * 1e3);
    }
    percentiles(lat, "raw in RAM (baseline)");

    // conditions 2 & 3: raw on file — candidate gen via PQ, re-rank via pread
    let measure_file = |file: &File, label: &str| {
        let mut buf = vec![0u8; ROW];
        let mut row = Vec::with_capacity(DIM);
        let mut lat = Vec::with_capacity(warm.len());
        for q in &warm {
            let t = Instant::now();
            let cand = idx.search(q, R, NPROBE, None, authz, keep); // hot PQ candidates
            let mut scored: Vec<(f32, u64)> = Vec::with_capacity(cand.len());
            for (n, _) in &cand {
                read_row(file, *n, &mut buf, &mut row); // raw read from file
                scored.push((sqdist(q, &row), *n));
            }
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            scored.truncate(K);
            lat.push(t.elapsed().as_secs_f64() * 1e3);
        }
        percentiles(lat, label);
    };

    let cached = File::open(&path).unwrap();
    measure_file(&cached, "raw on file (page-cache warm)");

    let uncached = File::open(&path).unwrap();
    if set_nocache(&uncached) {
        measure_file(&uncached, "raw on file (UNCACHED / F_NOCACHE)");
    } else {
        println!(
            "  (uncached read measurement is macOS-only via F_NOCACHE; skipped on this platform)"
        );
    }

    let _ = std::fs::remove_file(&path);
    println!(
        "note: candidate gen is always the hot PQ path; only the re-rank read source varies (R={R} rows/query)."
    );
}
