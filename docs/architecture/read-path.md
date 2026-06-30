# Architecture — Read path

> Design for `stroma-core::engine` (read-merge) + `stroma-core::query`. Companion WHAT:
> `../spec/read-path.md`. Overview: `../ARCHITECTURE.md` §4, §5. Status: implemented (Epic 2).

## Read-merge instead of read-modify-write (CAP-4)

A partial update should not rewrite the whole object. So writes are *appended* (to the changelog),
and a read *merges* the materialized base with the un-materialized tail on demand. The cost of a
read is therefore `base lookup + fold(tail)`, and the only thing that can make a read expensive is a
long tail — which is exactly what the `n_max` bound prevents.

## One bound, two guarantees

`n_max` is the changelog's backpressure limit *and* the read-merge tail bound — the same number.
Because `write` is rejected once the backlog hits `n_max` (CAP-1, no silent stall), the tail a read
must merge is always `<= n_max` (CAP-4, bounded). `materialize()` is the release valve: it folds the
tail into the base and advances the watermark, letting writes proceed. This ties write rate to read
freshness with a single knob.

## Correctness: merge ≡ materialize

Read-merge must not change answers, only defer work. Since the fold is a join-semilattice
(`fold.md`), `base ∪ fold(tail)` observed equals `fold([0, head))` observed — so a merged read and a
post-`materialize()` read are identical. This is asserted in the engine tests.

## point/expand are the symbolic-core operators (CAP-2)

`point` and 1–2 hop `expand` are the low-I/O local operators the workload is dominated by. They are
defined over the snapshot's `(subject, predicate)` maps, so they are correct independent of physical
layout. Physical co-location (a CSR adjacency for the symbolic core, degree-aware hub spill) is a
later optimization that speeds these up without changing their contracts.

## Data flow

```
write → changelog.append (authority, seqno)        ← backpressure at n_max
                       │
       materialize() → base fold (watermark)
                       │
snapshot() = base ∪ replay(tail [watermark, head)) → observe → Snapshot
                       │
query::{point_one, point_many, expand, two_hop}    ← reads
```

## Forward references
- Time scopes (`as-of/ever/overlap`) read `one_history` / valid-time → temporal operator (Epic 6).
- Live Query maintains these operators incrementally over the same fold (Epic 5, IVM).
- authz filter is injected ahead of these reads (Epic 6); type-aware hybrid adds the vector source
  (Epic 3). The CSR symbolic core is the physical form of `expand` (later).
