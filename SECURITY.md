# Security Policy

## Supported versions

StromaDB is pre-1.0. Security fixes are applied to the `main` branch; there are no long-term support
branches yet. Pin a commit if you need stability.

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Report privately via GitHub's **Private Vulnerability Reporting** on this repository
(*Security → Report a vulnerability*). Please include:

- a description of the issue and its impact,
- steps or a proof-of-concept to reproduce,
- affected commit/version and platform.

We aim to acknowledge a report within a few business days and to agree a disclosure timeline with the
reporter. Please give us reasonable time to release a fix before any public disclosure.

## Scope

StromaDB is a single-node, embeddable engine with no built-in authentication or network surface; it
runs inside a trusting process. Relevant classes of issue include: memory-safety defects, crash /
data-loss on crafted input (e.g. a malformed WAL frame), authz-scope bypass in the query path
(a principal observing nodes outside its allowed labels), and non-determinism in replay/recovery.
Denial-of-service from unbounded input on the ingest path is in scope where it violates the documented
backpressure contract.
