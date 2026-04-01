# Agent Onboarding

If you are a new agent on this repository, read these files in order before editing the pipeline:

1. [README.md](C:/projects/tables/README.md)
2. [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
4. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
5. [SKILL.md](C:/projects/tables/agents/skills/table-reduction/SKILL.md)
6. [Cargo.toml](C:/projects/tables/Cargo.toml)
7. [lib.rs](C:/projects/tables/src/lib.rs)
8. [main.rs](C:/projects/tables/src/main.rs) if the task affects the runner or artifact layout

## Minimum Mental Model

- A table is a constraint over an ordered local set of bit identifiers.
- Rows are integer masks over local bit positions, not dictionaries or global masks.
- Projection and rewriting always operate relative to local bit order.
- Persisted artifacts should use canonical ascending bit order.
- The active pipeline is a fixed-point loop over multiple reduction steps, not a single pass.
- `rank` is a derived metric: `row_count ** (1 / bit_count)`.
- pairwise merge is a natural join step and is currently bounded by `max_merge_arity = 16` by default.
- the supported production entrypoint is the default `tables` binary in `src/main.rs`.
- pairwise merge, subset absorption, and pair reduction each also have dedicated binaries under `src/bin/`.
- when a retained pairwise merge is kept in the active pipeline, its source tables may be removed immediately because the merge implies them.

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
