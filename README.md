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

The active system is reduced by repeatedly applying semantic operations such as subset absorption, forced-bit propagation, pair reduction, and node filtering until a fixed point is reached.

## What Should Stay In Root

Only a small set of files should remain in the root:

- entry documentation such as `README.md`;
- at most a few top-level launch scripts;
- stable project directories.

## Target Layout

- `data/raw/` for immutable inputs;
- `data/derived/` for intermediate and final JSON artifacts;
- `data/reports/` for machine-readable and human-readable reports;
- `src/steps/` for individual reduction operations;
- `src/pipeline/` for common fixed-point runners;
- `docs/` for data model and pipeline documentation;
- `agents/skills/` for project-specific AI agent skills;
- `runs/` for reproducible run folders;
- `tests/` and `benchmarks/` for validation and performance work.

## Main Entrypoints

- main fixed-point pipeline: [src/pipeline/common_fixed_point.py](C:/projects/tables/src/pipeline/common_fixed_point.py)
- subset absorption step: [src/steps/subset_absorption.py](C:/projects/tables/src/steps/subset_absorption.py)
- forced-bit step helpers: [src/steps/forced_bits.py](C:/projects/tables/src/steps/forced_bits.py)
- pair reduction step: [src/steps/pair_reduction.py](C:/projects/tables/src/steps/pair_reduction.py)
- node filtering step: [src/steps/node_filter.py](C:/projects/tables/src/steps/node_filter.py)

## Read First

- data model: [docs/DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
- reduction operations: [docs/OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
- fixed-point pipeline: [docs/PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
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

## Run Examples

```powershell
python .\src\pipeline\common_fixed_point.py
python .\src\steps\subset_absorption.py --prune-included
python .\src\steps\node_filter.py
```
