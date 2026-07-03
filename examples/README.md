# Examples

## `org-sample.ndjson`

A small, self-contained fictional-company knowledge graph for trying the console and query
surface — 46 nodes across five types (**Person**, **Team**, **Project**, **Document**, **Skill**)
linked by `member-of`, `reports-to`, `knows`, `works-on`, `has-skill`, `owns`, `depends-on`,
`authored`, and `about`. Names are generic (Alice, Bob, …) and authz labels vary from `0` (public)
to `3` (restricted), so the console's *view-as* roles filter the graph meaningfully.

Load it into a database directory:

```bash
stroma init   --db ./data
stroma ingest examples/org-sample.ndjson            --db ./data
stroma embed  examples/org-sample.embeddings.ndjson --db ./data   # optional: 8-d demo vectors
stroma serve  --db ./data        # then open http://127.0.0.1:7687
```

`org-sample.embeddings.ndjson` carries a deterministic 8-dimensional vector per node
(strong per-type signal plus team/id detail) so type-aware vector search returns sensible
neighbours and the console's embedding count is non-zero. Skip it if you only want the symbolic graph.

Or against a running `stroma-serve`, POST each line to `/ingest` (and `/embed`).

In the console: **Show all nodes** for the whole graph, or enter a focus node and a hop distance to
explore a neighbourhood; click a node for its details, drag a node to reposition it, and switch
*view as* to see how the graph changes under different label visibility.
