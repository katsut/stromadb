# StromaDB — Architecture

Core design of the StromaDB engine. Companion to `../SPEC.md`. Technical scope only.

The load-bearing technical bets below were each validated up front with throwaway spikes
(single-algebra ownership, fold determinism, open-loop warm latency, cross-store snapshot
consistency, integrated tail, and type-aware-hybrid quality).

## 1. Data model — Fact-centric

- **Fact = ⟨subject, predicate, object, valid-time, transaction-time, provenance, confidence⟩.**
  Nodes and edges are *projections* of facts; every capability operates on this unit.
- **Predicate catalog** is registered and bounded (tens–hundreds), Field-ID interned. Each predicate
  carries cardinality, relationship properties (symmetric/transitive/inverse), and domain/range.
- **Edges are first-class** (edge-id); edge properties live in a separate store; multi-edges allowed;
  directed is primitive (undirected = symmetric sugar).
- **Provenance separates asserted (primary) from derived (LLM/hypothesis).** Queries default to
  primary; derived is returned only on explicit request (prevents hallucination self-reinforcement).

## 2. Write model — stream fold

- Diffs (insert/update/delete, **out-of-order, multi-source**) are shuffled by `(subject, predicate)`
  and folded. Fold behaviour is driven by predicate cardinality:
  cardinality-1 → supersede (LWW-Register); cardinality-many → accumulate (OR-Set).
- Each `(subject, predicate)` state is a **join-semilattice** (commutative + associative + idempotent
  merge), so the fold **converges under any arrival order / partition / redelivery** — the basis for
  deterministic replay and audit. LWW tie-break is a total order `(tx-time, source, write-seq)`.
- Two deletes: natural supersession (closes valid-time, keeps history) vs. compliance hard-delete
  (a max-register floor that purges ≤ floor; re-assertion above the floor survives).
- The ingest fold uses the **same differential-dataflow algebra** as read/IVM (§5).

## 3. Time model — bitemporal

- **valid-time** (true-in-the-world) + **transaction-time** (recorded; the as-of/MVCC basis).
- Supersession closes the old valid-time interval; history is queryable knowledge
  (`ever`, interval-overlap joins computed, not stored).
- History sinks to a cold tier (Lance); the hot working set stays bounded; retention ages out.

## 4. Storage & tiering

- **Append-only changelog** is the **version authority** (source of truth). Writes land on an LSM
  (RocksDB/Speedb, O_DIRECT). Facts are read **zero-copy** via rkyv.
- **Derived stores** — a quantized vector index (IVF-PQ/DiskANN) and Lance V2 columnar (cold) — each
  carry a **watermark**: how far they have caught up to the changelog.
- Multi-tier cache: RAM (hot working set + catalog) / SSD (warm) / Lance (cold). Admission/eviction
  governs the SLO. Co-location is logical (io_uring coalescing); physical is separated.

## 5. Query model — one composable algebra

- **Composable operators / IR**: `point / type-ANN / expand / neighbor-scan / temporal / filter /
  score-rank`. The traverser between steps = id-set + scores + optional path context, bounded by a
  token budget and stamped with the version vector.
- A composed pipeline is submitted once and runs **server-side one-shot** by default; a thin
  micro-planner does operator fusion / predicate pushdown / cardinality reorder.
- **The same algebra** runs as one-shot (streaming eval) or **Live Query (IVM, stateful arrangement)**
  — the immutable rkyv payload + owned dataflow ids let zero-copy facts and incremental state share
  one engine (validated).
- Macro planning (which questions, in what order) is the agent's; intelligence is caller-side.

## 6. Type-aware hybrid

- Vectors are pre-computed and received, stored in a separate quantized index + pointer.
- **Type-aware hybrid**: ANN candidates are filtered/reranked by graph type/constraints. On a
  constructed benchmark this returns dramatically more correct results than plain ANN with ~zero
  type-violations. The "typed" half is the two-part schema (§8).

## 7. Cross-store consistency (the deepest risk, validated)

- `as_of` is a **version vector** `(changelog seqno, Lance ver, ANN index ver, MVCC txid)`, sampled as
  one consistent cut (invariant: derived watermark ≤ changelog seqno ⇒ no dangling).
- **strict** read = all stores at `min(watermark)` (fully consistent, excludes newest tail).
- **fresh** read = each store at latest + bounded skew ≤ B + a **brute-force tail** over the
  un-indexed `[watermark, head)` range, which **closes index/structure split-brain** (a structurally-
  present match is never dropped because the index lagged). Agent default.
- An append-only log of immutable chunks gives **lock-free reads**, so concurrent ingest does not
  stall reads.

## 8. Typed graph — two parts

| Half | Where | Content |
|---|---|---|
| **Declarative** | DB | predicates, cardinality, properties, types, valid-time policy, embeddings — vocabulary + structural rules + *expectations* (completeness) |
| **Procedural** | Caller (LLM) | decision recipes (how predicates are composed into a judgment) |

The declarative half amounts to a lightweight ontology (types, domain/range, cardinality, relation
properties) — deliberately without axioms or a reasoner. The engine holds only minimal constraint
validation (domain/range, cardinality); full reasoning is relocated to the caller (no-internal-model
principle).

## 9. Access control

- Every request carries an **end-user principal**; agents query by delegation (on-behalf-of).
- Authz is injected at the **head** of every pipeline; downstream sees only authorized facts.
  Cardinality estimation is **post-authz** (count itself must not leak). ABAC label-based; tenant
  namespace isolation is the outermost boundary; derived/cached summaries inherit source labels.

## 10. Operational

- **DR**: continuously back up the authoritative input (changelog + type catalog + received
  embeddings) + WAL fsync + snapshot/PITR; rebuild derived stores on restore.
- **Migration/backfill**: snapshot(LSN) → map → bulk fold → CDC from that LSN (gap/dup-free cutover).
- **Observability**: per-primitive p50/p99, backpressure rate, cache hit-rate, un-merged N, IVM
  memory, embedding skew, type-violation rate; agent-pipeline traces; deterministic changelog replay.
