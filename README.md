# Tables Research Project

This repository stores data and code for research on a system of bit tables:

- raw and derived table sets;
- reduction steps and fixed-point pipelines;
- experiment reports and run artifacts;
- project-specific material for AI agents.

The active code and data now follow the structured layout described in [docs/PROJECT_STRUCTURE.md](C:/projects/tables/docs/PROJECT_STRUCTURE.md).

## Domain Summary

A table is a constraint over an ordered local set of bit identifiers:

- `bits` stores the bit ids;
- `rows` stores allowed assignments as integer masks over local positions in `bits`;
- for `bits[i]`, the row value is `((row >> i) & 1)`.

The active system is reduced by repeatedly applying reduction and heuristic operations such as subset absorption, forced-bit propagation, single-table bit filtering, pair reduction, tautology filtering, and node filtering until a fixed point is reached.

In the active baseline pipeline:

- the supported production runner is the crate's default binary;
- `subset_absorption`, `forced_bits`, `single_table_bit_filter`, `pair_reduction`, `tautology_filter`, and `node_filter` run in a fixed-point loop;
- `pairwise_merge`, `subset_absorption`, and `pair_reduction` also have dedicated step CLIs under `src/bin/`;
- pairwise merge remains available as a standalone operation when we need it, but it is not part of the default pipeline.
- `single_table_bit_filter` is an intentionally lossy heuristic: it removes bits that occur in exactly one active table by projecting them out of that table.

The repository also uses the derived metric `rank` for tables:

- `rank = row_count ** (1 / bit_count)`
- equivalently, `rank ** bit_count = row_count`

## What Should Stay In Root

Only a small set of files should remain in the root:

- entry documentation such as `README.md`;
- at most a few top-level launch scripts;
- stable project directories.

## Target Layout

- `src/` for the crate library and the main fixed-point pipeline binary;
- `src/bin/` for standalone step CLIs;
- `examples/` for exploratory or retained one-off Rust utilities;
- `archive/` for retired scripts and one-off experiments that are no longer part of the active pipeline;
- `data/raw/` for immutable inputs;
- `data/derived/` for intermediate and final JSON artifacts;
- `data/reports/` for machine-readable and human-readable reports;
- `docs/` for data model and pipeline documentation;
- `agents/skills/` for project-specific AI agent skills;
- `runs/` for reproducible run folders;
- `tests/` and `benchmarks/` for validation and performance work.

## Main Entrypoints

- official fixed-point pipeline CLI: `cargo run --release -- ...`
- official fixed-point pipeline implementation: [src/main.rs](C:/projects/tables/src/main.rs)
- shared library modules: [src/lib.rs](C:/projects/tables/src/lib.rs)
- standalone pairwise merge CLI: `cargo run --release --bin tables-pairwise-merge -- ...`
- standalone subset absorption CLI: `cargo run --release --bin tables-subset-absorption -- ...`
- standalone pair reduction CLI: `cargo run --release --bin tables-pair-reduction -- ...`
- forced-bit implementation module: [src/forced_bits.rs](C:/projects/tables/src/forced_bits.rs)
- single-table bit filter implementation module: [src/single_table_bit_filter.rs](C:/projects/tables/src/single_table_bit_filter.rs)
- tautology filter implementation module: [src/tautology_filter.rs](C:/projects/tables/src/tautology_filter.rs)
- node filtering implementation module: [src/node_filter.rs](C:/projects/tables/src/node_filter.rs)

## Read First

- data model: [docs/DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
- reduction operations: [docs/OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
- fixed-point pipeline: [docs/PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
- performance workflow: [docs/PERF_WORKFLOW.md](C:/projects/tables/docs/PERF_WORKFLOW.md) for speed or memory work
- agent onboarding: [docs/AGENT_ONBOARDING.md](C:/projects/tables/docs/AGENT_ONBOARDING.md)
- project structure: [docs/PROJECT_STRUCTURE.md](C:/projects/tables/docs/PROJECT_STRUCTURE.md)

## Artifact Naming

Active artifacts use a stage-based scheme:

- `tables.<stage>.json`
- `bits.<stage>.forced.json`
- `bits.<stage>.rewrite_map.json`
- `bits.<stage>.components.json`
- `pairs.<stage>.subset_superset.json`
- `pairs.<stage>.relations.json`
- `nodes.<stage>.json`
- `report.<stage>.json`

Stage reports should also include rank summaries for the input and output table systems.

## Run Examples

```powershell
cargo run --release --
cargo run --release -- --max-rounds 1
cargo run --release --bin tables-pairwise-merge -- --help
cargo run --release --bin tables-subset-absorption -- --help
cargo run --release --bin tables-pair-reduction -- --help
```
