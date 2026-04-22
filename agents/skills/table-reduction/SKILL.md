# Table Reduction

## Purpose

This skill helps an AI agent work safely on the table-reduction project without asking the user to restate the domain model each time.

## Required Reading

Before editing code or interpreting data, read:

1. [README.md](C:/projects/tables/README.md)
2. [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
4. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
5. [GENERATION_CHAIN.md](C:/projects/tables/docs/GENERATION_CHAIN.md) when the task is about origin reachability, dependency layers, or generation artifacts
6. [AGENT_ONBOARDING.md](C:/projects/tables/docs/AGENT_ONBOARDING.md)
7. [SUBTABLE_ROUNDTRIP.md](C:/projects/tables/docs/SUBTABLE_ROUNDTRIP.md) when the task is about decomposing one large table into exact smaller subtables or checking reconstruction from them
8. [Cargo.toml](C:/projects/tables/Cargo.toml)
9. [lib.rs](C:/projects/tables/src/lib.rs)
10. [main.rs](C:/projects/tables/src/main.rs) when the task touches the pipeline runner or artifact outputs

## Project Assumptions

- A table is an object with `bits` and `rows`.
- Bit position inside `bits` defines bit position inside each row mask.
- When arity allows it, rows should be processed with bit operations on `uint32`.
- Table rows are local masks, not global bit assignments.
- Equal-schema tables merge by row intersection.
- The active baseline pipeline is a fixed-point loop, not a one-pass reducer.
- `rank` is defined as `row_count ** (1 / bit_count)`.
- The supported production entrypoint is the default `tables` binary in `src/main.rs`.

## Operations You Must Know

- subset absorption
- table merge as natural join over shared bits
- forced-bit propagation via row `AND` and `OR`
- single-table bit filtering as a documented lossy projection heuristic
- pair reduction via equal/opposite bit relations
- zero-collapse bit filtering for locally unrestricted bits
- tautology filtering for full `2^arity` row sets
- bounded neighborhood join filtering for rows that fail exact join-and-project in a small local neighborhood
- node filtering via shared projected subtables
- zero-collapse as a per-bit diagnostic metric on one table
- generation chain as a retained derived analysis from `origins` through exact one-bit dependency layers with constants tracked separately
- progressive subtable roundtrip for large tables: exact `2`-bit projections, drop tautological projections before each join stage, then either use the exhaustive pools `2`, `2+3`, and `2+3+4`, or use the retained selective strategy that adds only chosen higher-arity exact projections

In the active common pipeline, the fixed-point loop is:

1. `subset_absorption`
2. `forced_bits`
3. `single_table_bit_filter`
4. `pair_reduction`
5. `zero_collapse_bit_filter`
6. `tautology_filter`
7. `node_filter`

Pairwise merge is retained as a standalone Rust operation for experiments, but it is not part of the default fixed-point runner.
The supported implementation path for subset absorption is also Rust.
The supported implementation path for pair reduction is also Rust.
If a retained pairwise merge is kept, the source tables covered by that merge may be dropped immediately.
Use `src/bin/bit_zero_collapse.rs` for one-table diagnostics and `src/bin/bit_zero_collapse_all.rs --summary-only` when measuring metric throughput instead of JSON emission cost.
Use `src/bin/bit_generations.rs` for canonical generation-chain artifacts.
Treat `single_table_bit_filter` as lossy, but treat `zero_collapse_bit_filter` as equivalence-preserving.
`bounded_neighborhood_join_filter` is retained as a standalone equivalence-preserving row filter.
Its default bounds are `max_union_bits=32`, `max_tables_per_neighborhood=10`, and `min_tables_per_neighborhood=3`.

Do not change or describe these operations loosely. Use the project docs terminology.

## Default Workflow

1. Start with a system summary.
2. Check whether a similar reduction step already exists in `src/`.
3. Add new operations as steps in the crate pipeline or as dedicated binaries under `src/bin/`.
   For large-table factorization and reconstruction checks, prefer the shared `src/subtable_roundtrip.rs` module and the dedicated `subtable_roundtrip` CLI instead of re-implementing the workflow ad hoc.
4. If the task changes semantics, update docs before or with the code.
5. After each step, record:
   - table count;
   - bit count;
   - row count;
   - rank summary;
   - arity distribution.
6. Name new artifacts and reports consistently by stage.

## Artifact Rules

- Use the stage-based names documented in [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md) and implemented in [main.rs](C:/projects/tables/src/main.rs).
- Keep generated systems under `data/derived/`.
- Keep machine-readable reports under `data/reports/`.
- Keep important run metadata under `runs/<run-id>/`.
- For generation-chain outputs, use `bits.<stage>.generations.json`, `bits.<stage>.generation_by_bit.json`, `bits.<stage>.unreachable_from_origins.json`, `bits.<stage>.constant.json`, `report.<stage>.generation_chain.json`, `summary.<stage>.generation_chain.json`, and `report.<left-stage>_vs_<right-stage>.generation_chain.json` for canonical stage-to-stage comparisons.

## Implementation Rules

- Prefer bitwise integer operations over slow object-heavy representations.
- Preserve canonical ascending bit order in persisted outputs.
- Do not leave an empty table in the system.
- If a step exposes a tautology, remove it and report it.
- If a step only removes rows, state that explicitly in the report.
- If a step rewrites bits, preserve a mapping back to original bits.
- Stage reports must include input and output rank summaries.

## Safety Rules

- Do not delete historical artifacts without an explicit request.
- Do not change the `bits` / `rows` format.
- Do not claim improvement without numeric before/after comparison.
- Do not assume a new operation is correct until it is run to a fixed point when applicable.
- If semantics or invariants change, update the docs in the same task.
