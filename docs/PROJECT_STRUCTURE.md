# Project Structure

## Status

The repository uses a conventional single-crate Cargo layout centered on `src/`, `src/bin/`, and `examples/`.

## Goal

The repository layout should support:

- long reduction chains and many experiment variants;
- multiple generations of artifacts;
- reproducible runs;
- smooth collaboration between humans and AI agents;
- incremental growth of the common fixed-point pipeline.

## Target Tree

```text
tables/
  README.md
  Cargo.toml
  Cargo.lock
  docs/
    PROJECT_STRUCTURE.md
    DATA_MODEL.md
    OPERATIONS.md
    AGENT_ONBOARDING.md
    PIPELINE.md
    PERF_WORKFLOW.md
    runbooks/
      experiment_checklist.md
    adr/
  agents/
    prompts/
    skills/
      table-reduction/
        SKILL.md
      pipeline-performance/
        SKILL.md
  configs/
    pipeline/
    experiments/
  data/
    raw/
      tables.json
    derived/
      tables.common_node_fixed_point.json
      ...
    reports/
      report.common_node_fixed_point.json
      ...
  src/
    lib.rs
    main.rs
    common.rs
    bit_zero_collapse.rs
    pairwise_merge.rs
    table_merge_fast.rs
    subset_absorption.rs
    forced_bits.rs
    single_table_bit_filter.rs
    pair_reduction.rs
    zero_collapse_bit_filter.rs
    node_filter.rs
    tautology_filter.rs
    rank_stats.rs
    bin/
      bit_zero_collapse.rs
      bit_zero_collapse_all.rs
      pairwise_merge.rs
      subset_absorption.rs
      pair_reduction.rs
  examples/
    forced_bits_fast.rs
    pairwise_merge_generate.rs
  research/
    notes/
    notebooks/
  runs/
    2026-03-31-baseline-validation/
      config.json
      summary.md
      outputs/
  tests/
  benchmarks/
  archive/
```

## Directory Rules

### `docs/`

Permanent project documentation:

- data model for tables and rows;
- description of each reduction step;
- architectural decisions;
- operational runbooks.

### `agents/`

AI-agent support material:

- `skills/` for project-specific skills;
- `prompts/` for reusable prompts and checklists.

### `configs/`

Explicit run configuration:

- pipeline step order;
- step-level parameters;
- experiment flags;
- performance-related knobs.

### `data/raw/`

Immutable source data only.

### `data/derived/`

All derived JSON artifacts:

- intermediate table systems;
- final table systems;
- bit rewrite maps;
- dropped-table lists;
- node sets.

Recommended naming:

- `tables.<stage>.json`
- `tables.<stage>.dropped_included.json`
- `bits.<stage>.rewrite_map.json`
- `bits.<stage>.forced.json`
- `bits.<stage>.components.json`
- `pairs.<stage>.subset_superset.json`
- `pairs.<stage>.relations.json`
- `nodes.<stage>.json`

### `data/reports/`

Reports and summaries only:

- `report.<stage>.json`
- `summary.<stage>.md`

Stage reports should include:

- input and output counts for tables, bits, and rows;
- input and output rank summaries;
- arity distributions;
- step-specific statistics.

Current active stages:

- `subset_merged`
- `subset_pruned`
- `subset_pruned.forced`
- `subset_pruned.forced.pair_reduced`
- `common_fixed_point`
- `common_fixed_point.node_filtered`
- `common_node_fixed_point`

### `src/`

Active production implementation:

- `lib.rs` exposes the shared crate modules;
- `main.rs` contains the default fixed-point pipeline binary;
- one Rust module per reduction step lives directly under `src/`;
- `src/bin/` contains operational CLIs for `pairwise_merge`, `subset_absorption`, and `pair_reduction`, plus retained diagnostic utilities such as `bit_zero_collapse` and `bit_zero_collapse_all`.

### `examples/`

Retained exploratory or one-off Rust utilities:

- utilities that are useful enough to keep under version control;
- not part of the default production build path;
- runnable through `cargo run --example ...`.

### `research/`

Material that is not part of the production pipeline:

- draft notes;
- hypotheses;
- exploratory notebooks;
- comparison tables.

### `runs/`

Each important run should have its own folder:

- a copy of the config;
- a short summary;
- output links or copied outputs;
- runtime and size metrics.

This separates current best results from ordinary intermediate artifacts.

### `archive/`

For material that must be kept but should not stay in the active root:

- retired experiments;
- imported reference material;
- one-off experiments;
- retired artifacts.

## Recommended Next Steps

1. Normalize artifact naming by stage.
2. Add explicit pipeline profiles under `configs/pipeline/`.
3. Add regression tests for step-level invariants.
4. Record each important run in `runs/<run-id>/summary.md`.
