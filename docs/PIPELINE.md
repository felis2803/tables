# Reduction Pipeline

Current baseline pipeline:

1. `subset_absorption`
2. `forced_bits`
3. `pair_reduction`
4. `node_filter`

This step list is executed until a fixed point is reached.

Code entrypoint:

- [common_fixed_point.py](C:/projects/tables/src/pipeline/common_fixed_point.py)

Artifact naming helper:

- [artifacts.py](C:/projects/tables/src/utils/artifacts.py)

## Step Intent

### `subset_absorption`

- canonicalize tables;
- use smaller tables to remove incompatible rows from larger tables that contain their bit sets;
- drop included subset tables after absorption.

### `forced_bits`

- detect per-bit constants from row `AND` and `OR`;
- propagate these constants through all tables;
- remove forced bits from the active system and record them separately.

### `pair_reduction`

- detect equal or opposite bit pairs from table row patterns;
- build transitive parity components;
- rewrite all bits in each component to one representative.

### `node_filter`

- build shared projected subtables over bit intersections;
- propagate node restrictions through member tables until no node tightens further.

## Adding New Steps

A new step should:

- preserve the table format invariants;
- be valid after any earlier step in the pipeline;
- emit compact statistics;
- behave well inside a fixed-point loop.

## Core Invariants

- `bits` inside a table are sorted by ascending bit id;
- bit position inside `bits` matches bit position inside the row mask;
- `rows` only contain masks valid for that table arity;
- no step may leave an empty table behind;
- tautologies should be removed if a step fully exposes them.

## Expected Outputs

The common pipeline should keep these outputs in sync:

- reduced table system;
- forced original bits;
- bit rewrite map;
- rewrite components;
- dropped included tables;
- pair relations;
- node subtables;
- machine-readable run report.

Each stage report is also expected to include rank data for the table system it transforms:

- input rank summary;
- output or final rank summary;
- rank-by-arity breakdown when available.

## Agent Rule

If you add or modify a reduction step, also update:

- [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
- [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md) if semantics changed
- [SKILL.md](C:/projects/tables/agents/skills/table-reduction/SKILL.md)
