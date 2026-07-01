# Frozen contracts — v1 (2026-07-01)

The **stable seams** the rest of the system is built on: the Fact model, the fold semantics, the
changelog/read/write interfaces, the query operators, the version vector, and Live Query. Epic 6,
the real backends, and Vesicle all build against these shapes.

**Freeze policy.** Frozen = *build on it*; downstream may assume the shape and semantics below.
Changing a frozen contract is a **deliberate, breaking, versioned** change (bump to v2, update every
consumer) — not forbidden, but not casual. Backends *under* a contract (storage engine, ANN index,
IVM engine) change freely without touching the contract.

## Registry

| Contract | Spec | Frozen invariants (v1) |
|---|---|---|
| **Fact model** | `spec/data-model.md` | `Fact=⟨s,p,o,valid-time,tx-time,provenance,confidence⟩`; `Object=Node\|Value`; valid-time open (`to=None`); provenance `Asserted\|Derived`; `transaction_time` owned by the changelog |
| **Ontology catalog** | `spec/data-model.md` | Field-ID interning stable; predicate `{cardinality, rel-props, domain, range}`; minimal ingest validation is open-world (only known mismatches fail) |
| **Fold semantics** | `spec/fold.md` | per-(subject,predicate) join-semilattice; One→LWW-Register+history, Many→OR-Set, hard-delete=max-floor; `OrderKey=(tx,source,seq)` must be globally unique |
| **Changelog interface** | `spec/changelog.md` | `append`/`append_batch`/`replay(_range)`/watermark/backpressure; seqno = version authority; replay is a pure function of the log; **durable mode `open`/`sync`/`durable_head` — record durable iff `sync` returned Ok; recovery prefix-exact (torn tail dropped via `[len][crc]` frame)** |
| **Write contract (DB↔ETL)** | `spec/write-contract.md` | `WriteKind` vocabulary; `append_batch` atomic w.r.t. backpressure; `retract_edge` resolves OR-Set tags (ETL names the edge) |
| **Read / query ops** | `spec/read-path.md` | `point_one`/`point_many`/`expand`/`expand_set`/`two_hop`; read-merge = base ∪ tail, tail ≤ n_max; merged read ≡ post-materialize read |
| **Type-aware hybrid** | `spec/hybrid-search.md` | type filter over ANN candidates; **recall-completeness = `ANN(probed) ∪ brute-force(unprobed type-T)`, bounded tail (H2)**; **authz = scoped sub-index per authz-class, NOT shared index + post-filter (H4)** |
| **Version vector** | `spec/version-vector.md` | `(changelog_seqno, vector_watermark)` 2-tuple; partial order; **embeddings stamped with node seqno; `vector_watermark` = contiguous embedded prefix; fresh = prefix ∪ brute-force tail (H3)**; strict/fresh |
| **Live Query** | `spec/live-query.md` | `register`/`on_change` push diffs (added/removed); monotonic/bounded class; count bounded |

## NOT frozen (implementation — swappable under the contracts)
- changelog backend (in-memory / **framed file-WAL now** → LSM/RocksDB/Speedb + rkyv + O_DIRECT)
- vector index — **real IVF-PQ + exact re-rank landed** (`ivf.rs`; hot PQ codes 32× + cold raw re-rank
  tier). Measured SLO: filtered recall@10 ~1.0 @ type-sel 50% (rerank R=100), authz-on warm p99 0.78ms
  (`examples/ann_slo.rs`) — **p99 measured with raw in RAM; raw=SSD + p99<2ms not yet jointly validated**.
  Exact `vector::VectorIndex` retained as reference/oracle. **Wired into query-IR via the `AnnBackend`
  trait** (`ir::run` is generic over the backend; IVF-PQ path tested for equivalence vs the exact
  oracle). See `spec/vector-index.md`. Raw=SSD p99 re-measure (#19) and probe/rerank tuning (#23) pending.
- IVM engine (recompute-and-diff → differential-dataflow, validated in `poc-rkyv-ivm`)
- **durability failure model (H1)** — **file-WAL backend landed**: group-commit fsync, prefix-exact
  crash recovery, cold-start replay = RTO (0.81s @5M facts measured, 0 data loss;
  `examples/durability_slo.rs`). Device-level WAF/compaction/PITR defined when the LSM backend lands.

## Injection spikes that shaped v1
`../../ (platform) spikes/`: `poc-filtered-ann-recall` (H2), `poc-multiclock-vv` (H3),
`poc-authz-index-leak` (H4). See the platform review `docs/discuss/party-review-2026-07-01.md`.
