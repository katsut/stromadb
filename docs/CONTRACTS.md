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
| **Changelog interface** | `spec/changelog.md` | `append`/`append_batch`/`replay(_range)`/watermark/backpressure; seqno = version authority; replay is a pure function of the log |
| **Write contract (DB↔ETL)** | `spec/write-contract.md` | `WriteKind` vocabulary; `append_batch` atomic w.r.t. backpressure; `retract_edge` resolves OR-Set tags (ETL names the edge) |
| **Read / query ops** | `spec/read-path.md` | `point_one`/`point_many`/`expand`/`expand_set`/`two_hop`; read-merge = base ∪ tail, tail ≤ n_max; merged read ≡ post-materialize read |
| **Type-aware hybrid** | `spec/hybrid-search.md` | type filter over ANN candidates; **recall-completeness = `ANN(probed) ∪ brute-force(unprobed type-T)`, bounded tail (H2)**; **authz = scoped sub-index per authz-class, NOT shared index + post-filter (H4)** |
| **Version vector** | `spec/version-vector.md` | `(changelog_seqno, vector_watermark)` 2-tuple; partial order; **embeddings stamped with node seqno; `vector_watermark` = contiguous embedded prefix; fresh = prefix ∪ brute-force tail (H3)**; strict/fresh |
| **Live Query** | `spec/live-query.md` | `register`/`on_change` push diffs (added/removed); monotonic/bounded class; count bounded |

## NOT frozen (implementation — swappable under the contracts)
- changelog backend (in-memory → LSM/RocksDB/Speedb + rkyv + WAL + O_DIRECT)
- vector index (exact stand-in → quantized IVF-PQ/DiskANN)
- IVM engine (recompute-and-diff → differential-dataflow, validated in `poc-rkyv-ivm`)
- **durability failure model (H1)** — defined when the durable changelog lands (fsync/io_uring
  ordering, crash recovery, cold-start replay = RTO bound); NOT yet a frozen contract.

## Injection spikes that shaped v1
`../../ (platform) spikes/`: `poc-filtered-ann-recall` (H2), `poc-multiclock-vv` (H3),
`poc-authz-index-leak` (H4). See the platform review `docs/discuss/party-review-2026-07-01.md`.
