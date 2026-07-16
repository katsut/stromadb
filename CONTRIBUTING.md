# Contributing to StromaDB

Thanks for your interest. StromaDB is pre-1.0 and moving fast; issues and focused PRs are welcome.

## Build & test

```bash
cargo build                       # workspace (stromadb-core, stromadb-store, stromadb, stromadb-serve, stromadb-mcp)
cargo test                        # unit + integration tests
cargo fmt --all                   # format (rustfmt)
cargo clippy --all-targets -- -D warnings   # lints (must be clean)
```

Examples are runnable probes that double as reproducible benchmarks:

```bash
cargo run --release --example c2b_integrated -p stromadb-core   # integrated read p99
cargo run --release --example durability_slo -p stromadb-core   # RTO / data-loss / ingest
```

CI runs `fmt`, `clippy -D warnings`, and the test suite; a PR must be green before merge.

## Ground rules

- **Match the surrounding code.** Comment density, naming, and idiom should be indistinguishable from
  the file you are editing.
- **No unwrap in library code** except at documented init points; return typed errors.
- **New behavior needs a test.** Prefer property tests for invariants (see `fold` determinism,
  incremental-maintenance equivalence).
- **Docs are part of the change.** Component contracts live in each crate's module docs (rustdoc) —
  keep them accurate; if you change *why*, add a line to `docs/DECISIONS.md`. Capability claims in
  `README.md` must match implemented code — no target-state described as done.
- Keep dependencies minimal (`stromadb-core` has no runtime deps by design).

## License of contributions

StromaDB is source-available under the **Elastic License 2.0** (`LICENSE.txt`). By submitting a
contribution you agree that it is licensed under the same terms. A Contributor License Agreement (CLA)
may be required before a contribution can be merged; if so, a maintainer will point you to it on your
first PR.

## Reporting bugs / proposing changes

Open an issue describing the behavior (and a reproduction where possible) before a large PR, so the
approach can be agreed first. Security issues follow a different path — see [SECURITY.md](SECURITY.md).
