# Agent Onboarding

If you are a new agent on this repository, read these files in order before editing the pipeline:

1. [README.md](C:/projects/tables/README.md)
2. [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
4. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
5. [GENERATION_CHAIN.md](C:/projects/tables/docs/GENERATION_CHAIN.md) when the task is about origin reachability, dependency layers, or per-bit derivability
6. [SKILL.md](C:/projects/tables/agents/skills/table-reduction/SKILL.md)
7. [Cargo.toml](C:/projects/tables/Cargo.toml)
8. [lib.rs](C:/projects/tables/src/lib.rs)
9. [main.rs](C:/projects/tables/src/main.rs) if the task affects the runner or artifact layout

## Minimum Mental Model

- A table is a constraint over an ordered local set of bit identifiers.
- Rows are integer masks over local bit positions, not dictionaries or global masks.
- Projection and rewriting always operate relative to local bit order.
- Persisted artifacts should use canonical ascending bit order.
- The active pipeline is a fixed-point loop over multiple reduction steps, not a single pass.
- `rank` is a derived metric: `row_count ** (1 / bit_count)`.
- the supported production entrypoint is the default `tables` binary in `src/main.rs`.
- pairwise merge, subset absorption, and pair reduction each also have dedicated binaries under `src/bin/`.
- single-table bit filtering is part of the default fixed-point runner and projects away bits that occur in exactly one active table.
- single-table bit filtering is intentionally semantics-changing; treat it as a documented heuristic, not an equivalence-preserving reduction.
- zero-collapse bit filtering is part of the default fixed-point runner and removes bits whose zeroed projection contains exactly half as many distinct rows.
- zero-collapse bit filtering is equivalence-preserving; treat it as a semantic simplification, not as a heuristic.
- tautology filtering is part of the default fixed-point runner and removes full `2^arity` tables after bit rewriting.
- bounded neighborhood join filtering is a retained standalone row filter and removes rows that fail an exact join-and-project check inside a bounded local neighborhood.
- the current default bounds for that standalone step are `32` union bits, up to `10` tables per neighborhood, and at least `3` tables required to run the join.
- pairwise merge is a retained standalone natural-join step, but it is not part of the default fixed-point runner.
- `zero-collapse` is a retained diagnostic metric for one bit inside one table: it measures the relative collapsed-row share after zeroing that bit and deduplicating rows.
- the generation chain is a retained derived analysis over one system: generation `0` is `origins`, later generations are exact one-bit dependency layers, and constants are tracked separately from origin-derived generations.
- when timing the all-table zero-collapse diagnostic, prefer `bit_zero_collapse_all --summary-only` in `--release`, otherwise JSON serialization dominates the measurement.
- use `--disable-zero-collapse-bit-filter` when you need a same-build baseline without that step.

## Before You Change Anything

- identify whether the change is a new reduction step, a faster implementation of an existing step, or documentation only;
- verify artifact names against [main.rs](C:/projects/tables/src/main.rs) and the pipeline docs;
- preserve logical equivalence unless the task explicitly asks for a new semantics-changing operation;
- produce before/after counts for tables, bits, rows, and rank summaries.

## For Performance Work

If the task is about speed, throughput, or memory:

1. read [PERF_WORKFLOW.md](C:/projects/tables/docs/PERF_WORKFLOW.md);
2. read [pipeline-performance SKILL.md](C:/projects/tables/agents/skills/pipeline-performance/SKILL.md);
3. keep the baseline and candidate runs reproducible under `runs/<run-id>/`.

## When Adding A New Step

- define the step precisely in docs first;
- integrate it into the common fixed-point runner only after its standalone behavior is clear;
- state whether it removes rows, bits, tables, or only rewrites identifiers;
- ensure downstream steps still accept the resulting table form.
