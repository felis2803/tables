# Reduction Pipeline

Current baseline pipeline:

1. `subset_absorption`
2. `forced_bits`
3. `single_table_bit_filter`
4. `pair_reduction`
5. `tautology_filter`
6. `node_filter`

This step list is executed until a fixed point is reached.

Code entrypoint:

- official runner: `cargo run --release -- ...`
- implementation: [src/main.rs](C:/projects/tables/src/main.rs)

Artifact naming and default output paths:

- active stage names and default output paths are defined in [main.rs](C:/projects/tables/src/main.rs)

## Step Intent

### `subset_absorption`

- canonicalize tables;
- use smaller tables to remove incompatible rows from larger tables that contain their bit sets;
- the supported implementation path for this step is the crate pipeline and the `tables-subset-absorption` CLI;
- drop included subset tables after absorption.

### `forced_bits`

- detect per-bit constants from row `AND` and `OR`;
- propagate these constants through all tables;
- remove forced bits from the active system and record them separately.

### `single_table_bit_filter`

- count how many active tables contain each bit;
- if a bit appears in exactly one active table, project that bit out of that table;
- deduplicate projected rows and collapse any duplicate schemas created by that projection;
- this step is intentionally lossy and does not preserve strict table-system equivalence;
- run before `pair_reduction` and `tautology_filter`, so the simplification can expose new relations and tautologies in the same round.

### `pair_reduction`

- detect equal or opposite bit pairs from table row patterns;
- build transitive parity components;
- rewrite all bits in each component to one representative.
- the supported implementation path for this step is the crate pipeline and the `tables-pair-reduction` CLI;

### `tautology_filter`

- drop any table whose row set is the full `2^arity` assignment set for its schema;
- run after `single_table_bit_filter` and `pair_reduction`, so it removes tautologies exposed by forcing, single-table bit removal, and bit rewriting;
- leave non-tautological tables unchanged.

### `node_filter`

- build shared projected subtables over bit intersections;
- propagate node restrictions through member tables until no node tightens further.

## Standalone Steps

### `pairwise_merge`

- merge every pair of tables that shares more than one bit;
- keep only merges whose resulting arity does not exceed `max_merge_arity`;
- the supported implementation path for this step is the `tables-pairwise-merge` CLI;
- after retaining merged tables, immediately drop source tables implied by those merges.

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
- tautologies should be removed by the dedicated `tautology_filter` step once they are exposed.
- `single_table_bit_filter` is the current baseline step that is explicitly allowed to change semantics.

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
