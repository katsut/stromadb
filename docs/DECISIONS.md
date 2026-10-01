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
  late-arrival guard depends on that); a many-fact iff the element is currently present AND a live add
  row matches `(object, source, valid interval)` exactly — so a corrected interval, and a re-grant
  after a close, still append; an edge-prop set iff the value is unchanged (checked per prop — a
  suppressed fact body with a changed prop appends just the prop); a close iff the (element's) winner
  is already a close at the same `valid_from`.
  A same-value fact from a *different* source always appends: distinct agreeing sources are per-row
  corroboration evidence. Cost: one head read per incoming fact against the materialized state (the same
  head the point read resolves), no new lookup structure. Node records follow the same rule: a re-send
  whose type and label both match the write-side mirrors is suppressed (no engine op, no jsonl line).
- **Why:** the changelog is the version authority *for change*; observation frequency is not information
  the fold can use (the re-assertion folds to the identical state), so recording it only inflates the log,
  replay, and the read-merge history. Suppression at the boundary leaves fold/changelog semantics
  untouched — whatever is appended folds exactly as before.
- **Evidence:** suppression tests in `crates/stroma-db/tests/db.rs` (identical re-send suppressed with
  `durable_head` unchanged; different source / different `valid_from` / older-value re-send still append;
  duplicate close suppressed).

### D23. Many-cardinality elements carry valid time; `close` with an object ends one element
- **Context:** Many keys were a plain add/remove OR-Set: `AddMany` dropped the fact's valid time and a
  `retract` tombstoned the observed adds outright, so a revocation *destroyed* the interval instead of
  ending it — "who could access this document as of T" was unanswerable even though every grant change
  had been ingested. One-cardinality had already solved cessation temporally (D21).
- **Decision:** each Many element keeps per-element version rows in the exact One-cardinality shape —
  an add row carries the element and its `[valid_from, valid_to)` interval, a `CloseMany` row (ingest:
  the `close` record with an `object` field) ends it. Rows are keyed by globally-unique order keys, so
  the state stays a join-semilattice (map union), and *presence* is "the element's greatest live row is
  an add" — with adds only that is exactly the old add-wins OR-Set, so pre-existing data and old WAL
  frames (which decode with an unreported interval) observe identically. As-of reads slice the rows per
  element with the same covering-row rule as `point_one_asof`; `point` and `expand` accept `valid_at`
  for Many and echo it back (capability detection — an older server would silently answer current).
  `retract` stays the history-destroying erase and never observes close rows.
- **Why:** revocation is a temporal fact, not an un-write; reusing the One-cardinality row shape per
  element buys the as-of semantics, arrival-order independence, and the determinism argument in one
  move instead of inventing a second temporal model.
- **Evidence:** fold determinism proptests extended with `CloseMany` + intervals (P1–P5, 2000 cases
  each); per-element as-of unit tests in `stroma-core/src/query.rs`; WAL old-frame decode test; ingest
  end-to-end (grant → close → re-grant across a reopen) in `crates/stroma-db/tests/db.rs`.

### D24. Compaction = fold snapshot + WAL truncate; explicit, history-preserving, crash-window-free
- **Context:** the changelog grows without bound and cold-start replay scales with total history —
  measured near-linear at ~0.29s RTO / ~44 MB WAL per million records (`examples/changelog_growth`),
  crossing the <10s SLO at ~35M records. fold+observe is ~80% of that RTO; frame decode ~20%.
- **Decision:** an explicit compaction (`Engine::compact` / `Db::compact` / serve `POST /compact` —
  an admin action like reset, no automatic trigger yet) persists the FOLD state verbatim as
  `wal.log.snap` — every version row with its original order key, tombstones, hard-delete floors —
  archives the covered WAL (`wal.log.archive-<S>`, uncompressed v1) and starts a fresh WAL whose
  first frame names `S`, so seqnos stay globally continuous. Superseded rows are retained by design:
  as-of reads are part of the read contract, so the snapshot bounds *replay work*, not history.
  Crash windows self-reconcile without multi-file atomicity: the snapshot commit is one atomic
  rename, and open replays only WAL records at/after the snapshot's seqno — a committed snapshot
  next to a not-yet-truncated WAL just skips the stale prefix.
- **Why:** the fold is the one structure that already holds everything the read contract needs;
  serializing it verbatim (rather than re-emitting synthetic writes) preserves LWW/as-of tie-breaks
  exactly, because order keys travel with the rows. `gc()` runs first — it provably preserves
  observation, so the snapshot never carries rows no read could see.
- **Limits:** with as-of retention the snapshot still grows with history — compaction shrinks the
  constant (sequential load vs replay-apply), not the asymptote; bounding by LIVE state needs a
  history-horizon/archive policy (future). Archive compression not yet.
- **Evidence:** `crates/stroma-core/tests/compaction.rs` — a compacted engine is asserted
  observationally identical to a never-compacted twin (current + as-of reads across the boundary,
  seqno continuity) across reopen, including the crash-window and double-compaction cases; P6
  proptest: the fold codec round-trips losslessly and canonically (2000 cases).

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
- **Why:** a naive scalar watermark left ~1/5000 dangling refs under async embedding; the contiguous
  prefix + fresh brute-force is always complete.
- **Evidence:** injection spike `poc-multiclock-vv` (results in the design history).

### D8. Type-aware hybrid with a recall-completeness clause
- **Decision:** a type-ANN operator returns `ANN(probed) ∪ brute-force(unprobed type-T)`, with the
  brute-force tail **bounded by a budget**. The recall tail (missed type-T members in unprobed cells) is a
  distinct axis from the watermark tail.
- **Why:** approximate ANN × a type filter collapses recall (pre/post-filter dilemma); the bounded
  completeness tail restores it without an unbounded scan.
- **Evidence:** `poc-filtered-ann-recall`; `IvfPq::search_complete`.

### D9. Authz is scoped, not shared-index + post-filter
- **Decision:** the authz+type predicate is applied **before** a candidate is scored; a principal never
  computes distance against data it can't see.
- **Why:** a shared index + post-authz filter leaks the unauthorized-near count through timing and top-k
  completeness; skipping unauthorized postings before scoring closes that channel.
- **Evidence:** `poc-authz-index-leak`.

## Vector backend

### D10. IVF-PQ + exact re-rank (hot codes / cold raw)
- **Context:** the capacity envelope has 0.5M–5M × 768-dim vectors; raw f32 is 1.5–15 GB — too big for the
  hot RAM budget.
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

### D25. Quantizer fit is tracked; rebuild reuses quantizers until drift or growth
- **Context:** the quantizers (coarse cells + PQ codebooks) are fitted once, to a sample. The store's
  rebuild used to retrain on the *oldest 20K prefix* every embed batch — quantizers locked to the earliest
  distribution — and the coarse-assignment distance computed on every add was thrown away, so recall decay
  from distribution shift was invisible.
- **Decision:** `train` records the mean coarse-assignment sqdist over its sample (the baseline); every
  `add` accumulates the same number for the live corpus; `fit()` reports baseline/live/ratio. The store's
  rebuild is **reuse-or-retrain**: reuse the trained quantizers (`fresh_like`, skips k-means — the dominant
  build cost) while the fit ratio stays ≤ 1.5 and `suggested_nlist(n)` has not outgrown the trained `nlist`
  ≥ 2×; otherwise retrain on a deterministic **stride sample spanning the whole corpus**. `/stats` exposes
  the fit numbers and whether the last build retrained.
- **Why:** makes silent recall decay observable and turns retraining into an explicit, threshold-gated
  event; steady-state rebuilds skip their dominant cost. Either rebuild path carries the identical posting
  set (nodes/seqnos/labels), so watermark/strict read semantics do not depend on which one ran.
- **Known limits:** fit measures coarse-assignment error only (PQ codebook misfit is correlated but not
  separately tracked); the ratio is a corpus mean, so a large well-fitting old corpus can dilute a small
  drifted stream below the threshold; when drift does trip, the reuse attempt pays one extra add pass
  before the retrain (k-means still dominates, so the retry is cheap by comparison).

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

### D26. Conformance verdicts are maintained incrementally via support-set tracking
- **Context:** `conformance` re-derives every subject's verdict per call. A watcher (a review queue, a
  live panel) polling a stored rule re-paid O(subjects) each time and had to diff client-side. The hard
  part is the derived paths: a verdict depends on reads at *intermediate* nodes (the assignee's
  department's manager), so "which subjects does this write affect" is not answerable from the write's
  subject alone.
- **Decision:** judging a subject records every `(node, predicate)` it read — its **support set** — and
  maintenance inverts that: a write to a key re-judges exactly the subjects whose last judgment read it;
  node-type touches guard the subject universe (`materialize_tracked_with_nodes`). Any write that could
  change a verdict must change a key the last judgment read (a branch not consulted cannot have
  influenced it, and what makes it consultable touches a recorded key first). Every write-path tail
  drain goes through one choke point that feeds every watched rule; changes land in a bounded per-rule
  journal behind a cursor (`conformance_watch` / `conformance_changes`), with `resync` on cursor
  fall-behind. Maintenance is unfiltered; the caller's label mask applies at read time, like the
  one-shot op.
- **Why:** the maintained map provably equals a full evaluation while the update cost tracks the blast
  radius, not the graph.
- **Evidence:** property test — 2,500 random events (supersessions, closes, late-arriving corrections,
  mid-stream subjects) with a full-evaluate oracle after every event. `examples/conformance_ivm.rs`:
  single-subject writes stay near-flat at 1.6→16.5µs from 1K→100K subjects while full re-evaluation
  grows 171µs→29.9ms (106×→1,812×); a manager transfer costs O(its department's issues), not O(all
  issues).
- **Known limits:** watches are in-memory (re-watch after reopen; re-declaring a rule invalidates its
  watch); the journal is bounded (fall too far behind → resync); maintenance cost is paid on the write
  path in proportion to watched rules × blast radius.

## Serving

### D27. Namespaces live in the serving layer; the engine stays one database per directory
- **Context:** a local `stroma up` is usually already owned by one app. Loading an unrelated dataset
  into it mixes that data's types, predicates and node ids into the app's graph, and the only
  isolation was a second process on another port — heavy for one developer machine.
- **Decision:** `stroma-serve` fronts several databases. The `--db` directory is the `default`
  namespace; a named namespace is an ordinary database directory at `<db>/ns/<name>/`, addressed by
  the path prefix `/ns/<name>/` (names `[a-z0-9_-]{1,64}`). A namespace is created by its first
  `/ingest`; anything else on a missing one is 404. Databases open lazily and stay cached for the
  process lifetime. Auth, sessions and token scopes stay server-wide.
- **Why not in the engine:** a `Db` owns one WAL, one catalog, one write mutex and one directory lock.
  Tenancy inside it would thread a namespace through every key, index and read path for no gain over
  a second directory, which already isolates everything and keeps every offline tool working
  unchanged (`stroma import --db <db>/ns/<name>`).
- **Why a path prefix, not a header:** an MCP client and the browser console can only be given a URL.
  A prefix makes a namespace addressable wherever a URL is, and unprefixed paths keep existing
  clients on `default`.
- **Why lazy creation on write:** a typo in a read URL must not leave an empty database behind, while
  the first ingest into a new name should just work, as `stroma up` does for a fresh directory.
- **Relation to #237:** namespaces share one process — one crash, one memory budget, one credential
  set. They are for one developer machine or one small server holding a few datasets. Many tenants,
  per-tenant credentials and crash isolation remain the job of process isolation and the planned
  fleet gateway (#237), which routes to one server per tenant. The two compose: a gateway child can
  itself serve namespaces.

### D28. External keys resolve by an exact-value scan, not a maintained value index
- **Context:** every read op and MCP tool takes a numeric node id, but an agent is usually handed an
  external identifier: an issue key stored as the one-cardinality text predicate `issue-key`. With no
  key → id step, an agent in an end-to-end run probed `point` with guessed ids, then gave up. `find`
  is a case-insensitive substring match over every text value, so it answers "which nodes mention X",
  not "which node has key X".
- **Decision:** a `lookup` op and MCP tool, `{predicate, value, type?, limit?, valid_at?}` →
  `{nodes:[{id, type, display}], truncated}`. It is an exact match on the current value of a
  one-cardinality predicate, or on the value in effect at `valid_at`. The label mask applies as on
  every read, so a masked node is absent. `limit` defaults to 10 and is capped at 100.
- **Why a scan:** the snapshot is keyed `(node, predicate)`, so a value index would be a second
  structure to keep consistent on every publish, compaction and as-of history change, plus a schema
  flag to opt predicates in. The scan walks the snapshot's one-cardinality map once, touching only
  entries of the given predicate for the comparison. An as-of lookup walks the history keys and does
  one `point_one_asof` probe per key of that predicate. Both are linear in stored keys, which is
  bounded by the per-org envelope. A lookup over a few thousand issues is dominated by the JSON
  round trip, not the walk.
- **Revisit when:** lookups become a hot path on large namespaces. The upgrade is a
  `(predicate, value) → nodes` map for predicates declared `key: true` on `pred_def`, maintained on
  the write side next to the node-label map. The op contract stays the same.

### D29. Conformance answers are scoped to subjects and bounded by default over MCP
- **Context:** `conformance` returned a verdict for every subject of the rule's type, out-of-scope
  ones included as `NOT_APPLICABLE`. On a namespace with about 5k issues that is one ~600 KB JSON
  line. An MCP client stores a tool result that large in a file the agent cannot read, so an agent
  deciding one issue never saw its own verdict.
- **Decision:** the op accepts `subject` or `subjects` and judges only those ids, in O(listed). It
  also accepts `only` (verdict names), `limit` and `offset`. The response adds `total` (rows kept by
  `only`), `returned`, `truncated` (rows remain after this page) and `counts` per verdict over every
  row before `only` and paging. Every distinct listed id gets exactly one row. An id the rule does not judge
  answers `NOT_APPLICABLE` with a `reason`: `not_subject_type` for a visible node of another type,
  `unknown_subject` for an id with no typed node or a masked node, so a masked node is not told
  apart from a missing one. These rows count as `NOT_APPLICABLE`.
- **Revision:** the first version dropped such ids from the answer. A client then saw
  `verdicts: []` and `total: 0` for an id of the wrong type, which looks the same as "not
  evaluated". An explicit row per requested id removes that ambiguity without revealing masked
  nodes.
- **Two defaults:** the HTTP op keeps its old behaviour, every verdict with no filter, because
  existing callers (the console's conformance panel, scripts) read the full list. The fields above
  are additive. For a full evaluation the MCP tool defaults to `limit: 50` and drops
  `NOT_APPLICABLE` rows while still counting them. A subject-scoped MCP call applies neither
  default and returns every requested row, so asking for one subject always yields its verdict.
- **Why not a scope-only fix:** a narrowing inline `scope` still reports the rest as
  `NOT_APPLICABLE`, and it makes the caller encode a key lookup as a rule. `lookup` (D28) plus
  `subjects` keeps rules declarative and the call order simple: key → id → verdict.

### D30. Conformance conditions take numeric ranges, and rules can be banded by first-match cases
- **Context:** approval-authority tables are usually banded by amount: up to one limit the direct
  manager approves, up to a higher one the next level, above that a fixed approver. Conditions
  could only test equality, so the amount-dependent part of such a table was not expressible, and a
  request raised past a band after approval could not be flagged.
- **Decision:** a condition is `{predicate, as_of?, test}` where the test is `equals` or a numeric
  range (`gt`/`gte` and/or `lt`/`lte`, or inclusive `between`). Two ints compare exactly; any other
  int/float pair compares as `f64`. A condition's `as_of` reads its value at the instant held by an
  anchor predicate on the subject, the same rule an as-of hop follows. A rule may replace `required`
  with ordered `cases: [{when?, required?}]`, evaluated first-match, so one stored rule encodes the
  whole table; verdicts carry the matched `case` index.
- **Unreadable values:** a missing value, a non-numeric value under a range, or a missing anchor
  leaves the condition unsatisfied, exactly as an equality test over a missing value already did.
  So an unreadable scope is `NOT_APPLICABLE`, an unreadable `absent_when` does not raise `ABSENT`,
  and an unreadable band falls through to the next case. Treating it as a violation instead would
  report a gap the rule never declared; a rule that wants one can say so with a final catch-all
  case.
- **Incremental maintenance:** condition reads, including the anchor and the as-of value, go
  through the same traced read path as hops, so they join the verdict's support set (D26). Case
  selection records only the conditions it consulted, up to the match. A revision of the amount
  re-judges exactly the subjects that read it. Property test: random amount revisions, closes,
  retroactive corrections and org changes against both a current-value and an as-of banded rule,
  with a full-evaluate oracle after every event.
- **Not done:** no arithmetic or cross-predicate comparisons (amount vs. another subject's limit).
  Bands are literals in the rule.

### D31. An unresolved required path is NOT_APPLICABLE with a reason, not a wrong MISMATCH
- **Context:** when the required path resolved to no value, such as a missing intermediate edge or
  a missing as-of anchor, a present actual was reported `MISMATCH` with `kind: wrong` and
  `required: null`. A correct approval then looked the same as a wrong one, and a stale one lost its
  `stale` kind. Separately, out-of-scope subjects came back `NOT_APPLICABLE` with no reason while
  requested non-subjects had one.
- **Decision:** every `NOT_APPLICABLE` row carries exactly one `reason`, and no other row does:
  `out_of_scope`, `no_matching_case`, `required_unresolved`, plus the two from D29. A present actual
  whose required path does not resolve is `NOT_APPLICABLE` with `required_unresolved`. The row keeps
  its `actual`, `distinct`, `as_of` and `case` values for diagnosis. Responses add `reasons`, a
  count per reason over every row before `only` and paging.
- **Precedence:** a missing actual is judged exactly as before, `ABSENT` when `absent_when` holds and
  `OK` otherwise, whether or not the required path resolves. Absence does not depend on the expected
  value, and the gap is real either way. With a present actual, a resolved `distinct_from` collision
  stays `MISMATCH` (`wrong`), since it is decidable without the expected value. Only what remains
  becomes `required_unresolved`.
- **Why not a fifth verdict:** an `UNRESOLVED` verdict would be equally precise, but existing
  callers rely on the four-value space. The console summary renders the four `counts` keys, scripts
  sum them, and `only` filters and MCP defaults are written against them. Under the chosen mapping
  a caller that acts on `MISMATCH` stops seeing false violations without any change, a caller that
  ignores `NOT_APPLICABLE` keeps working, and one that wants the data gaps reads `reason` or
  `reasons`. The MCP default still omits `NOT_APPLICABLE` rows from a full evaluation, so
  `reasons.required_unresolved` keeps their number visible.
- **Incremental maintenance:** unchanged in mechanism. A walk records the read that came up empty,
  `(last reached node, hop predicate)` or the anchor key, so the write that supplies the missing
  fact re-judges exactly the subjects stopped there (D26). Tests: a late edge moves a subject from
  `required_unresolved` to `OK` and another to `MISMATCH`, with no other subject re-judged. The
  banded random stream now adds and closes org edges and asserts that it crosses the unresolved
  boundary in both directions, with the full-evaluate oracle after every event.

### D32. A derived path may end in a literal; paths that can never resolve are rejected
- **Context:** the walk kept only node values, so a path whose last hop reads a literal, such as a
  manager's `name`, always resolved to nothing. Such a rule reported every present actual as
  `required_unresolved` (before D31, as a `wrong` MISMATCH), while SPEC.md showed exactly that rule
  shape with text values. The same walk silently gave nothing for a `many`-predicate hop and for a
  literal-valued hop in the middle of a path.
- **Decision:** every hop but the last must reach a node; the last hop's value is the path's value
  as read, node or literal, with the same `as_of` handling as before. The actual is compared with
  it by exact stored-value equality, the keying ingest already applies and the one `equals`
  conditions use: equal text matches, and an int never equals a float. `stale` versus `wrong` uses
  the same valid-time history probe on the last as-of hop, compared by value, so an approver named
  under a manager's old name is `stale`. A missing literal or a missing anchor is
  `required_unresolved` (D31). `distinct_from` follows the same rule.
- **Rejected, not resolved to nothing:** a hop on a `many` predicate (a set is never a single
  expected value) and a literal-valued hop before the last (a literal has no outgoing predicates)
  can never resolve. `conformance` and `conformance_watch` refuse such a rule with an error naming
  the path and hop, next to the unknown-name check. A `rule_def` line is still only parsed at
  ingest, because a rule may be declared before its predicates and a predicate may be re-declared
  later. So the check runs where names are resolved, and a stored rule never blocks replay of
  `rules.jsonl`. Embeddings are not predicates and cannot appear on a path, so they need no check.
- **Why support, not reject:** the literal case is the natural shape of "the recorded name must
  match the name in effect at review time", and it needs no new read primitive. `point_one` and
  `point_one_asof` already return literals, and the terminal read is the same traced read as
  before.
- **Incremental maintenance:** unchanged in mechanism. The last hop's `(node, predicate)` read is
  recorded whether or not a value or anchor is present, so a rename, a close, or the first name
  re-judges exactly the subjects whose path ends there (D26). Property test: a random stream of
  renames with valid time, name closes, reporting-line moves, anchor and actual changes over an
  as-of and a current-value literal rule, with a full-evaluate oracle after every event, asserting
  that it reaches `stale` mismatches and crosses `required_unresolved` in both directions.

### D33. No value in effect at the as-of anchor is `required_unresolved`, never `stale`
- **Context:** with `required.hops[-1].as_of`, an approval dated before the first recorded value
  of the last hop (the directory's `reports-to` or `manager-of` was first observed after the
  approval) has nothing in effect at the anchor. Before D31 such a row was `MISMATCH` with
  `kind: stale` and `required: null`, indistinguishable from "a different manager held the role
  at approval time", so every approval older than the first directory sync read as a lapsed
  authority. D31 moved every unresolved required path to `NOT_APPLICABLE`, which already covers
  this case mechanically; this decision pins the semantics and the exact definitions.
- **Decision:** an equality `MISMATCH` is only ever judged against a required value that was in
  effect at the anchor. Its `kind` is decided by the valid-time history of the last as-of hop
  alone:
  - `stale` — the required value in effect at the anchor differs from the actual, and the actual
    value appears in that hop's history at some other valid-time, earlier **or later** (the
    approver was the manager before a transfer, or became the manager afterwards). The probe is
    by value and ignores the instant, so a later-dated interval also counts.
  - `wrong` — the required value in effect at the anchor differs from the actual, and the actual
    value never appears in that hop's history. A mismatch on a timeless last hop, and every
    `distinct_from` collision, is `wrong`.
  - `required_unresolved` — no value was in effect at the anchor: the history starts after it, or
    a close covers it with no successor, as well as the D31 cases (a missing hop value or anchor).
    This holds even when the actual value held the hop at an earlier time: without a value in
    effect there is no comparison, so there is nothing stale about the row. The row keeps `as_of`
    and `actual`, so a caller can report "no history at `as_of`".
- **Why not a new kind or a coverage field:** a `kind: unknown_at_as_of` would put a non-violation
  under `MISMATCH`, which D31 rejected for the same reason callers act on `MISMATCH`. A
  `required_coverage` start on the row was considered and deferred: the `as_of` instant together
  with the `timeline` op already gives the first recorded interval, and the row shape stays as in
  D31.
- **Incremental maintenance:** exact, unchanged in mechanism. The as-of read that found nothing is
  recorded on `(last reached node, hop predicate)`, so a backfilled interval that starts before the
  anchor re-judges exactly the subjects anchored in it (D26). Tests: a backfill flips two
  unresolved approvals to `OK` and `wrong`, and a later correction of the same interval to another
  manager turns the `OK` into `stale`; the approval-shaped random stream asserts that it backfills
  an unresolved as-of read into a judged verdict, with the full-evaluate oracle after every event.
### D34. Float literals are keyed and compared as `f64` end to end; no on-disk format change
- **Context:** the JSON ingest path narrowed every `float` to `f32` before taking its bits as the
  fold key, while the fold key, the WAL and snapshot codec, and the range comparison already
  carried 64 bits. So an amount above 2^24 (16,777,216) or one with cents was rounded on the way
  in: `16777217` stored as `16777216`, `1234567.89` as `1234567.875`, and a banded rule over such
  amounts could pick the wrong band. Range bounds went through the same keying, so a bound rounded
  the same way and the error was invisible to an equality test but not to a threshold.
- **Decision:** a `float` is keyed by the bits of the `f64` the JSON number parses to, in every
  form (`{"float": x}`, a bare edge-property number, an `equals` value, a range bound). The JSON
  parser runs with exact float parsing (`serde_json/float_roundtrip`), since the default
  best-effort parser can land one ulp off for long mantissas, which would break the bit-exact
  read-back the property test asserts. Embeddings stay `f32`; ANN is unaffected.
- **Why no format bump:** the WAL and snapshot records already hold an 8-byte `f64` bit pattern
  for a float (tag `2`, `u64`). A record written before this change is a valid `f64` that happens to
  be `f32`-representable, so the existing reader reads it unchanged and no version or migration is
  needed. The stored history keeps whatever value was keyed at the time: a float ingested before
  the change stays at its rounded value until the fact is written again with the exact one. A
  deployment that needs the exact history re-ingests those facts from the authoritative input
  (D4), which the fold then treats as ordinary later writes.
- **Evidence:** round trip of `16777217.0` and `1234567.89` through a reopen (WAL replay) and a
  compaction snapshot, bit-exact; inclusive and exclusive range bounds at those values select
  exactly the right subjects, one cent apart in both directions; a property test over random
  finite `f64` (normal, subnormal, zero, and cent-valued amounts) reads back bit-exact after replay
  and is selected by a one-point `between` band.

## Core SLOs (the "unchanging core" bar) — measured

| leg | target | measured |
|---|---|---|
| Durability | 0 data loss; cold-start replay < 10s @5M | 0 loss; RTO **0.81s** |
| Type-aware hybrid | filtered recall@10 ≥ 0.9 @ type-sel 50%; authz-on warm hybrid p99 < 2ms | recall ~1.0; p99 **<1ms** (hard data) |
| Integration | integrated open-loop (`c2b_integrated`), real ANN + real durability | 0.5M read p99 **1.84ms** (warm raw); 0 data loss; live diffs; version vector consistent |

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
