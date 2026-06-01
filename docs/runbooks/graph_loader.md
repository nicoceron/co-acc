# Graph Loader Runbook

Neo4j is an optional derived cache for interactive graph exploration. The lake
and curated parquet remain the source of truth for signal detection, case
evidence, anomaly scores, and narrator inputs.

## Current Local Path

The default local runtime does not require a graph projection. Use:

```bash
make backend-ready
```

That gate validates compose configuration, curated contracts, lake reality, and
the lake-backed API smoke path with `NEO4J_REQUIRED=false`. It proves that the
signal, case, and citizen-agent endpoints work without Neo4j.

## Compose Neo4j

The dev compose Neo4j service remains available for exploration and for legacy
graph-backed routes:

```bash
docker compose up -d neo4j api
```

API correctness must not depend on that graph having projected signal data. If
Neo4j is connected but empty, case list/detail reads still prefer the latest
lake-backed signal run.

## Loader Status

`coacc-etl graph load --scope finals` and `coacc-etl graph verify` are not
shipped yet. When they are added, they must read curated parquet in bounded
DuckDB batches, write indexed Neo4j nodes and relationships with batched
`UNWIND`, and report drift against the parquet source tables before the graph is
used for demos.

Until those commands exist, treat Neo4j as optional UI infrastructure only. Do
not compute signals in Neo4j and do not use graph counts as audit evidence.
Authenticated investigation workspaces still use Neo4j for user-owned state.
When a user attaches an entity that is present in curated dimensions but absent
from the optional graph projection, the API writes a minimal `LakeEntityRef`
workspace node and keeps the entity evidence/analysis reads lake-backed.
Frontend refresh actions should also tolerate an empty optional graph
projection: entity signal refresh returns the current materialized lake signal
payload for curated entities, and case refresh returns the current lake-backed
case dossier before falling back to reviewer-owned graph investigations.
