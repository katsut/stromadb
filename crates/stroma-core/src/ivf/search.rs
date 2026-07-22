//! Query paths: the per-query ADC table, the (optionally 2-level) cell probe, and the three search
//! entry points — PQ-only, exact re-rank, and recall-complete.

use super::{COARSE_FANOUT, IvfPq};
use crate::fact::NodeId;
use crate::vector::sqdist;

impl IvfPq {
    /// Flat ADC table for query `q`: `m × pq_ksub` sub-distances, `score(code) = Σ_j table[j*ksub+code[j]]`.
    /// Cell-independent (non-residual PQ), so it is computed **once per query** and reused across every
    /// probed cell — candidate-gen cost no longer scales with `nprobe`.
    fn adc_table(&self, q: &[f32]) -> Vec<f32> {
        let mut table = vec![0f32; self.m * self.pq_ksub];
        for j in 0..self.m {
            let off = j * self.dsub;
            for c in 0..self.pq_ksub {
                let cb = self.sub_codebook(j, c);
                let mut d = 0f32;
                for i in 0..self.dsub {
                    let r = q[off + i] - cb[i];
                    d += r * r;
                }
                table[j * self.pq_ksub + c] = d;
            }
        }
        table
    }

    /// ADC score = Σ_j table[j·ksub + code_j]. The loop is gather-bound (a table lookup per code
    /// byte); NEON has no gather, so the win is instruction-level parallelism — four independent
    /// accumulators break the add dependency chain and let loads pipeline, and `chunks_exact`
    /// removes per-element bounds checks.
    #[inline]
    fn adc_score(&self, table: &[f32], code: &[u8]) -> f32 {
        let ksub = self.pq_ksub;
        let (mut s0, mut s1, mut s2, mut s3) = (0f32, 0f32, 0f32, 0f32);
        let mut off = 0usize;
        let mut it = code.chunks_exact(4);
        for ch in &mut it {
            s0 += table[off + ch[0] as usize];
            s1 += table[off + ksub + ch[1] as usize];
            s2 += table[off + 2 * ksub + ch[2] as usize];
            s3 += table[off + 3 * ksub + ch[3] as usize];
            off += 4 * ksub;
        }
        for &c in it.remainder() {
            s0 += table[off + c as usize];
            off += ksub;
        }
        (s0 + s1) + (s2 + s3)
    }

    /// The `nprobe` cells nearest to `q`, closest first. With the 2-level coarse quantizer (#32) this
    /// routes via the super-centroids — gather the coarse cells under the nearest supers until there
    /// are enough candidates, then pick the `nprobe` nearest among *those* — so the per-query coarse
    /// work is ~√nlist + a bounded candidate set instead of a full O(nlist) scan. Exact re-rank absorbs
    /// the small routing approximation. Falls back to a linear scan for small nlist.
    fn probe_cells(&self, q: &[f32], nprobe: usize) -> Vec<usize> {
        let np = nprobe.max(1);
        if self.super_coarse.is_empty() {
            let mut cd: Vec<(f32, usize)> = self
                .coarse
                .iter()
                .enumerate()
                .map(|(i, c)| (sqdist(q, c), i))
                .collect();
            cd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            return cd.into_iter().take(np).map(|(_, i)| i).collect();
        }
        // level 1: rank super-centroids by distance to q
        let mut sd: Vec<(f32, usize)> = self
            .super_coarse
            .iter()
            .enumerate()
            .map(|(i, c)| (sqdist(q, c), i))
            .collect();
        sd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        // level 2: gather coarse cells from the nearest supers until we have a generous candidate pool
        let target = (np * COARSE_FANOUT).min(self.nlist);
        let mut cand: Vec<usize> = Vec::with_capacity(target + 64);
        for (_, si) in sd {
            for &ci in &self.super_members[si] {
                cand.push(ci as usize);
            }
            if cand.len() >= target {
                break;
            }
        }
        // pick the np nearest coarse cells among the gathered candidates
        let mut cd: Vec<(f32, usize)> = cand
            .into_iter()
            .map(|ci| (sqdist(q, &self.coarse[ci]), ci))
            .collect();
        cd.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        cd.into_iter().take(np).map(|(_, i)| i).collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_cell(
        &self,
        table: &[f32],
        cell: usize,
        max_seqno: Option<u64>,
        allowed_label: &impl Fn(u32) -> bool,
        keep: &impl Fn(NodeId) -> bool,
        out: &mut Vec<(f32, NodeId)>,
        scored: &mut usize,
    ) {
        let c = &self.lists[cell];
        let rows = c
            .codes
            .chunks_exact(self.m)
            .zip(&c.nodes)
            .zip(&c.seqnos)
            .zip(&c.labels);
        for (((code, &node), &seqno), &label) in rows {
            if max_seqno.is_some_and(|w| seqno >= w) {
                continue; // strict watermark: indexed prefix only
            }
            if !allowed_label(label) {
                continue; // authz: never score unauthorized postings
            }
            if !keep(node) {
                continue; // graph type / arbitrary filter
            }
            out.push((self.adc_score(table, code), node));
            *scored += 1;
        }
    }

    fn topk(mut scored: Vec<(f32, NodeId)>, k: usize) -> Vec<(NodeId, f32)> {
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(d, n)| (n, d)).collect()
    }

    /// Approximate top-k by PQ (ADC) over the `nprobe` nearest cells — the "probed" view. Recall rises
    /// with `nprobe`. Filters: `max_seqno` (watermark), `allowed_label` (authz), `keep` (type). PQ-only
    /// ranking; for high recall use [`IvfPq::search_rerank`].
    pub fn search(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let table = self.adc_table(q); // once per query — reused across all probed cells
        let mut out = Vec::new();
        let mut scored = 0;
        for cell in self.probe_cells(q, nprobe) {
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut scored,
            );
        }
        Self::topk(out, k)
    }

    /// High-recall top-k: generate `rerank_r` PQ candidates (cheap, hot codes), then re-score just
    /// those by *exact* distance over the raw (cold) tier. Returns exact distances. This is the
    /// production read path — PQ error no longer bounds recall.
    #[allow(clippy::too_many_arguments)]
    pub fn search_rerank(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        rerank_r: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let cand = self.search(q, rerank_r.max(k), nprobe, max_seqno, allowed_label, keep);
        let exact: Vec<(f32, NodeId)> = cand
            .into_iter()
            .map(|(n, _)| (sqdist(q, self.raw_of(n)), n))
            .collect();
        Self::topk(exact, k)
    }

    /// Recall-complete top-k (H2): the probed result unioned with a bounded brute-force over matching
    /// postings in *unprobed* cells. `tail_budget` caps how many extra postings may be scored — if the
    /// matching tail fits the budget, coverage is complete; otherwise the tail is bounded (the knob).
    /// Returns `(results, tail_scored, tail_truncated)` so callers can see whether completeness held.
    /// (Ranking is still PQ; compose with re-rank for exact ordering of the union.)
    #[allow(clippy::too_many_arguments)]
    pub fn search_complete(
        &self,
        q: &[f32],
        k: usize,
        nprobe: usize,
        max_seqno: Option<u64>,
        allowed_label: impl Fn(u32) -> bool,
        keep: impl Fn(NodeId) -> bool,
        tail_budget: usize,
    ) -> (Vec<(NodeId, f32)>, usize, bool) {
        let table = self.adc_table(q); // once per query
        let probed: std::collections::HashSet<usize> =
            self.probe_cells(q, nprobe).into_iter().collect();
        let mut out = Vec::new();
        let mut scored = 0;
        for &cell in &probed {
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut scored,
            );
        }
        let mut tail_scored = 0;
        let mut truncated = false;
        for cell in 0..self.nlist {
            if probed.contains(&cell) {
                continue;
            }
            let before = out.len();
            let mut cell_scored = 0;
            self.scan_cell(
                &table,
                cell,
                max_seqno,
                &allowed_label,
                &keep,
                &mut out,
                &mut cell_scored,
            );
            tail_scored += cell_scored;
            if tail_scored > tail_budget {
                out.truncate(before); // drop the overflowing cell wholesale — bounded, not partial
                truncated = true;
                break;
            }
        }
        (Self::topk(out, k), tail_scored, truncated)
    }
}
