# Design Decisions & Measured Findings

> The *why* behind StromaDB's design, in the order it was decided, with the evidence that settled each
> call. This is the public rationale trail so a newcomer can follow how the engine got its shape.
> Component-level contracts live in each crate's module docs (rustdoc).
> Format per entry: **Context → Decision → Why → Evidence/Status**. Numbers come from the reproducible
> `crates/stroma-core/examples/*` probes.

## Method

### D0. Prove risky variables with throwaway probes before building
- **Context:** an ambitious core (durable versioned graph + vector hybrid + reactive queries) has several
  places it could be fundamentally infeasible.
- **Decision:** before the full build, prove each scary variable with a cheap, isolated probe; only then
  implement.
- **Why:** cheapest place to kill a bad idea is before the code exists.
- **Status:** the `examples/` probes (durability RTO, ANN recall/cost, SSD re-rank, integrated open-loop)
  are the descendants of those probes and stay in-tree as reproducible checks.

### D1. Measure only under representative, unfriendly conditions
- **Context:** early "green" numbers repeatedly turned out to be artifacts of easy conditions (nprobe=1,
  cheap filters, in-RAM raw, 100K scale).
- **Decision:** an SLO is only claimed when measured at the representative point (~0.5M vectors), on hard
  (overlapping-cluster) data, with the authz+type filter active, and with the cold tier on SSD.
- **Why:** each easy condition hid a real cost; stripping them surfaced the true drivers (nprobe, catalog
  lookups, coarse-quantizer scan, cold-SSD re-rank).
- **Status:** standing rule; the examples take scale/config knobs so results are reproducible.

## Data model & write path

### D2. One Fact tuple as the unit of everything
- **Decision:** `Fact = ⟨subject, predicate, object, valid-time, tx-time, provenance, confidence⟩`;
  `Object = Node | Value`. Types/predicates live in a typed catalog (Field-ID interning, cardinality,
  domain/range) with *minimal* ingest validation (open-world: only known mismatches fail). No reasoner in
  the DB.
- **Why:** every capability must compose on the same unit; a full OWL-style reasoner is a non-goal — the
  DB does not reason or call a model, that is the caller's job.

### D3. Fold = per-(subject,predicate) join-semilattice
- **Decision:** cardinality-One → LWW-Register + history; cardinality-Many → OR-Set; hard-delete → a
  max-register floor. Total order via `OrderKey = (tx, source, seq)`, which the engine keeps globally
  unique.
- **Why:** a join-semilattice makes replay **order-independent and deterministic** — the basis of audit,
  recovery, and multi-source merge.
- **Evidence:** `tests/fold_determinism.rs` (proptest: permutation-invariance, multi-source split/merge,
  idempotent re-delivery, GC invariance).

### D4. Changelog is the append-only version authority; backpressure, never silent stall
- **Decision:** every write is appended and assigned a monotonic `seqno` (the version authority);
  derived stores chase its watermark; replay is a pure function of the log. Under overload the changelog
  returns explicit `Backpressure`, it does not stall.
- **Why:** centralizing *version* in one monotonic seqno makes cross-store consistency tractable and lets
  any derived store be rebuilt; explicit backpressure keeps a slow consumer from melting the system.

### D5. Durability = framed file-WAL + group-commit fsync (LSM later, same contract)
- **Context:** durability must be crash-sound with a bounded recovery time, without (yet) a full LSM.
- **Decision:** append writes to a framed WAL (`[len][crc32][payload]`), `fsync` per chunk (group commit);
  `open` recovers the committed prefix and drops a torn tail via the frame checksum. `append` stays
  in-memory/infallible; `sync` is the explicit durability point. The eventual LSM backend slots in behind
  this same open/sync/replay/watermark contract.
- **Why:** group commit gives durability at chunk granularity without an fsync per write; the checksum *is*
  the commit marker, so recovery is prefix-exact with no separate journal.
- **Evidence (`examples/durability_slo.rs`, 5M facts):** write+fsync 0.71s; cold-start recovery (RTO)
  **0.81s** (< 10s target); **0 data loss** on torn-write.

### D21. Ending a one-value is an explicit `close` record, not a retract or a bounded rewrite
- **Context:** the ingest surface had no way to express cessation of a cardinality-one value (a value
  ending with no successor, e.g. an assignee removed). `retract` resolves OR-Set observed tags — a
  many-only mechanism — so on a one-predicate it was a silent no-op; re-writing the old value with a
  bounded `valid_to` also fails, because the original open-interval row still covers later instants and
  wins as-of among covering rows.
- **Decision:** a `close` ingest record maps to the changelog's `CloseOne` — a versioned row with no
  object. The head becomes absent and as-of reads at/after its `valid_from` return nothing, independent
  of arrival order (same fold semantics as any competing one-write). `retract` on a one-predicate is an
  explicit error naming `close`; a retract of an absent many-edge stays a no-op and is no longer counted.
- **Why:** cessation is a fact like any other, so it must be a first-class versioned write (replayable,
  as-of-correct, order-independent) rather than a mutation trick; one write kind per cardinality keeps
  the fold unambiguous.
- **Evidence:** ingest close tests in `crates/stroma-db/tests/db.rs` (head absent, as-of before/after
  the close boundary, reversed arrival order).

### D22. Ingest suppresses no-op re-assertions (append-on-change, not append-always)
- **Context:** a connector re-sync re-emits facts whose values are unchanged; appending every one made
  changelog growth (and cold-start replay time) proportional to observation frequency, not to real change.
- **Decision:** at the ingest boundary an incoming write identical to current state is skipped and
  reported in a `suppressed` ingest counter (`facts`/`closes` count appended writes only). A one-fact is
  suppressed iff the *current head* row matches on object, valid interval, and source — head-only, so a
  re-send equal to an older row still appends and legitimately moves the head under arrival order (the
  late-arrival guard depends on that); a many-fact iff the same `(object, source)` element is already
  live; an edge-prop set iff the value is unchanged (checked per prop — a suppressed fact body with a
  changed prop appends just the prop); a close iff the head is already a close at the same `valid_from`.
  A same-value fact from a *different* source always appends: distinct agreeing sources are per-row
  corroboration evidence. Cost: one head read per incoming fact against the materialized state (the same
  head the point read resolves), no new lookup structure.
- **Why:** the changelog is the version authority *for change*; observation frequency is not information
  the fold can use (the re-assertion folds to the identical state), so recording it only inflates the log,
  replay, and the read-merge history. Suppression at the boundary leaves fold/changelog semantics
  untouched — whatever is appended folds exactly as before.
- **Evidence:** suppression tests in `crates/stroma-db/tests/db.rs` (identical re-send suppressed with
  `durable_head` unchanged; different source / different `valid_from` / older-value re-send still append;
  duplicate close suppressed).

## Read path

### D6. Read-merge: materialized base ∪ bounded tail
- **Decision:** a read merges the materialized `base` fold with the un-materialized changelog tail (bounded
  by `n_max`), on demand. Merged read ≡ post-materialize read.
- **Why:** partial updates are never re-written; the un-merged backlog is bounded, tying write rate to read
  freshness.

### D7. Cross-store reads via a 2-tuple version vector (strict/fresh)
- **Decision:** `(changelog_seqno, vector_watermark)`. **Strict** reads the indexed prefix only; **Fresh**
  reads indexed ∪ a brute-forced tail, closing index/structure split-brain. The 2-tuple holds *iff*
  embeddings are stamped with their node's changelog seqno and `vector_watermark` is the contiguous
  embedded prefix.
- **Why (H3):** a naive scalar watermark left ~1/5000 dangling refs under async embedding; the contiguous
  prefix + fresh brute-force is always complete.
- **Evidence:** injection spike `poc-multiclock-vv` (results in the design history).

### D8. Type-aware hybrid with a recall-completeness clause (H2)
- **Decision:** a type-ANN operator returns `ANN(probed) ∪ brute-force(unprobed type-T)`, with the
  brute-force tail **bounded by a budget**. The recall tail (missed type-T members in unprobed cells) is a
  distinct axis from the watermark tail.
- **Why:** approximate ANN × a type filter collapses recall (pre/post-filter dilemma); the bounded
  completeness tail restores it without an unbounded scan.
- **Evidence:** `poc-filtered-ann-recall`; `IvfPq::search_complete`.

### D9. Authz is scoped, not shared-index + post-filter (H4)
- **Decision:** the authz+type predicate is applied **before** a candidate is scored; a principal never
  computes distance against data it can't see.
- **Why:** a shared index + post-authz filter leaks the unauthorized-near count through timing and top-k
  completeness; skipping unauthorized postings before scoring closes that channel.
- **Evidence:** `poc-authz-index-leak`.

## Vector backend

### D10. IVF-PQ + exact re-rank (hot codes / cold raw)
- **Context:** the A1 envelope has 0.5M–5M × 768-dim vectors; raw f32 is 1.5–15 GB — too big for the hot
  RAM budget.
- **Decision:** PQ-compress each vector to `m` bytes (hot, ~48 MB @ m=96, 32×) for candidate generation;
  re-rank the top-`rerank_r` candidates by **exact** distance over the raw vectors (cold tier). IVF routes
  a query to its `nprobe` nearest cells.
- **Why:** **pure-PQ recall@10 caps ~0.4 in 768-dim** (quantization error swamps fine ranking) — PQ alone
  can't meet the recall SLO. Re-rank restores it while touching raw for only `rerank_r` vectors/query, so
  raw can be a cold (SSD/mmap) tier and hot RAM stays the PQ codes.
- **Evidence (`examples/ann_slo.rs`):** filtered recall@10 pure-PQ ~0.38 → +rerank ~1.0; 32× compression.

### D11. Non-residual PQ + a once-per-query ADC table
- **Context:** classic IVFADC encodes the residual from the cell centroid, making the ADC table
  cell-dependent → rebuilt per probed cell → candidate-gen cost scales with `nprobe` (the p99 driver).
- **Decision:** because exact re-rank restores recall, PQ only has to *rank candidates*, so encode the
  **raw** sub-vectors (non-residual). The ADC table is then cell-independent and computed **once per
  query**. Codes are stored struct-of-arrays per cell for cache locality.
- **Why:** removes the `nprobe` dependence from candidate-gen; the cheap recall lever becomes `rerank_r`,
  not `nprobe`.
- **Evidence:** warm p99 dropped ~37% (4.0→2.5ms at nprobe=16, then to ~2.0ms with the SoA layout).

### D12. `nlist` scales with N + a 2-level coarse quantizer
- **Context:** with `nlist` too small, coarse cells are large/imbalanced → a query's probed postings blow
  up (the 100K p99 driver). With `nlist` large (~√N-scaled), `probe_cells` becomes a linear O(nlist·dim)
  scan (the 0.5M p99 driver).
- **Decision:** `suggested_nlist(n) ≈ 4·√n`; and for `nlist ≥ 512`, a **2-level coarse quantizer** —
  cluster the coarse centroids into ~√nlist super-centroids and route via them, so per-query coarse work
  is ~√nlist + a bounded candidate pool instead of O(nlist). Exact re-rank absorbs the small routing
  approximation.
- **Evidence (`examples/c2b_integrated.rs`):** 100K read p99 3.1→1.6ms (nlist 256→1024); 0.5M read p99
  3.2→**1.84ms** (2-level coarse); `two_level_coarse_preserves_recall` test (recall@10 ≥ 0.9).

### D13. FxHash for the node→type/label maps
- **Decision:** the catalog's `node_type`/`node_label` maps use FxHash (dependency-free), not the default
  SipHash.
- **Why:** the authz+type filter hits these once per candidate; SipHash was ~1ms of the read p99. FxHash is
  chosen over a dense-Vec index so it works for any node-id distribution (no dense-id assumption).

### D14. Operating point: nprobe=8, rerank_r=256 (raw must be warm for the p99 SLO)
- **Decision:** the query-IR read path defaults to nprobe=8, R=256.
- **Evidence:** on hard data, recall is bought with `rerank_r` (R=100→0.83, R=256→~1.0) at authz-on warm
  p99 <1ms. **Caveat:** re-rank p99 <2ms holds when the *active* raw working set is warm (RAM/page cache);
  fully-cold SSD re-rank at R=256 adds several ms (`examples/ann_ssd_p99.rs`) — mitigations (OPQ to shrink
  R, a warm re-rank buffer) are tracked as roadmap.

## Query IR, Live Query, build

### D15. Composable operator IR — authz at the head, bounded result, single algebra
- **Decision:** a pipeline is `Source → Transform*` evaluated one-shot server-side; authz is injected at
  the head and threaded into every source/expand; every result is bounded (`max_nodes`, a token budget) and
  stamped with the version vector (`as_of`). The same operators back Live Query (one algebra). The vector
  backend is abstracted behind `AnnBackend` so the exact index (oracle) and IVF-PQ are interchangeable.
- **Why:** callers issue cheap primitives in a loop; the DB is a fast, self-describing, authz-safe query
  layer, and the model stays on the caller side.

### D16. Live Query = recompute-and-diff (IVM stand-in)
- **Decision:** a live query is any Snapshot→node-set function; on change the registry re-evaluates and
  emits only the delta. The efficient differential-dataflow backend slots in behind the same
  register/diff contract later.

### D17. Parallel build via scoped threads (no dependency)
- **Decision:** k-means assignment and per-vector assign+encode fan out across CPUs with
  `std::thread::scope`; list insertion stays serial. `add_batch` is order-equivalent to serial `add`.
- **Evidence:** 200K×768 build 112s → ~10s (~11×), making the 0.5M representative build feasible.

## Concurrency & rule evaluation

### D19. Lock-free reads over a pinned snapshot
- **Context:** reads and writes shared one lock, so a long write batch stalled every read.
- **Decision:** split the database into a write authority (`Mutex<WriteState>`) and an immutable pinned
  read view (`RwLock<Arc<ReadState>>`). A read clones the `Arc<ReadState>` under a momentary lock and then
  runs entirely on that pinned state with no lock held; a write holds the write mutex for the ETL and, on
  completion, swaps in a fresh read view. Node attributes ride the snapshot as a flat `Arc<FxHashMap>`
  (O(1) clone on publish/pin; single-shot flat lookups on the read-path authz+type filter).
- **Why:** a long write must not block reads, and a read must be snapshot-isolated against writes that land
  after it pins.
- **Evidence:** integrated read p99 **1.32ms** under a concurrent writer (flat with the idle number); a
  multi-threaded concurrency + snapshot-isolation suite passes. A persistent `imbl` HAMT was measured for
  the node maps but dropped for the flat `Arc<FxHashMap>` — it reclaimed the read-path cost while keeping
  the O(1) publish.

### D20. Conformance = a declared rule → deterministic per-subject verdict
- **Context:** evaluating a multi-hop, as-of compliance rule (e.g. "was this approved by the manager of the
  assignee's department *as of the approval time*") is deterministic, but a caller that re-derives it each
  time is not: a measurement found a mid-tier agent, handed only the read primitives, scored 0–13% perfect on
  such an audit and dropped the timing-sensitive case even when guided.
- **Decision:** a `conformance` op evaluates a *declared* rule — a subject type, an optional scope, a
  required derived path (a chain of one-cardinality hops, the last optionally read *as-of* a valid-time
  anchor), an actual predicate, and an absence condition — into a per-subject verdict
  `OK | ABSENT | MISMATCH | NOT_APPLICABLE`, with `MISMATCH` sub-classified `stale | wrong` via a
  valid-time history probe. Composed purely from `point_one` / `point_one_asof` — no new inference, no
  reasoner. Post-authz, as-of-aware, deterministic (sorted output).
- **Why:** the deterministic part of a decision should be evaluated the same way every time; the engine
  owns the verdict, the caller orchestrates and acts on it.
- **Evidence:** reproduces a hand-checked 8-subject fixture exactly, including the as-of *stale* case the
  agent got wrong ~100% of the time. Follow-ups: stored/named rules and incremental (live) maintenance.

## DONE SLO (the "unchanging core" bar) — measured

| leg | target | measured |
|---|---|---|
| Durability | 0 data loss; cold-start replay < 10s @5M | 0 loss; RTO **0.81s** |
| Type-aware hybrid | filtered recall@10 ≥ 0.9 @ type-sel 50%; authz-on warm hybrid p99 < 2ms | recall ~1.0; p99 **<1ms** (hard data) |
| Integration | C2b open-loop, real ANN + real durability | 0.5M read p99 **1.84ms** (warm raw); 0 data loss; live diffs; version vector consistent |

## License

### D18. Elastic License 2.0 (source-available)
- **Decision:** the OSS core is under the Elastic License 2.0 (`LICENSE.txt`).
- **Why:** self-host, modify, embed are all allowed; only offering it as a hosted/managed service is
  restricted. A permissive license (Apache-2.0) was rejected to avoid a permissive history that a
  competing managed service could fork from.

## Known limitations / roadmap (what is *not* done yet)

These are deferred by decision, not oversights — the core is validated for a bounded, single-node,
pre-production workload:

- **Distribution / replication / HA:** the engine is single-node and scale-*up* by design — the bounded
  per-org envelope (millions–tens-of-millions of nodes, GB-class hot set) is what keeps the hot working
  set in memory, which is what buys the low-ms reads and the small footprint at once. There is no
  built-in replication or failover (a single node is a single point of failure); durability rests on the
  WAL + fsync and a backup/PITR of the authoritative input (changelog + type catalog + embeddings), from
  which every derived store rebuilds (cold-start RTO 0.81s). A read replica / hot standby fed from the
  changelog (the single version authority) is future work — a *different axis* from horizontal sharding,
  which is a non-goal: web-scale (billion-node), multi-region, and petabyte distributed processing are out
  of scope by design, not gaps.
- **LSM backend + compaction/checkpoint:** durability is a file-WAL today; without compaction the WAL grows
  and cold-start RTO scales with total history, not live state.
- **Concurrency:** reads are now lock-free over a pinned snapshot (D19), so reads run during a write; a
  generational MVCC for many concurrent long-lived readers is still pending, and writes are single-writer
  by design. (The 0.5M DONE-SLO numbers above were measured sequentially; D19's p99 is the concurrent
  read-under-write figure.)
- **Full MVCC snapshots:** `materialize` now maintains the observed snapshot incrementally (O(changed
  keys), shared via `Arc` — the per-epoch full-clone stall is gone); a generational MVCC for many
  concurrent long-lived readers is still pending.
- **Cold-SSD re-rank:** the raw re-rank tier must be warm for the p99 SLO; fully-cold SSD at R=256 is slow
  (OPQ / warm buffer are the mitigations).
- **OPQ, index drift re-training, async embedding pipeline, real-machine cost validation.**
