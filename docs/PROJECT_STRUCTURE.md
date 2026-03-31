# Project Structure

## Status

Migration from the flat root layout to the structured layout is complete for active code, active artifacts, and primary documentation.

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
  docs/
    PROJECT_STRUCTURE.md
    DATA_MODEL.md
    OPERATIONS.md
    AGENT_ONBOARDING.md
    PIPELINE.md
    runbooks/
      experiment_checklist.md
    adr/
  agents/
    prompts/
    skills/
      table-reduction/
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
    io/
    utils/
    steps/
      subset_absorption.py
      forced_bits.py
      pair_reduction.py
      node_filter.py
    pipeline/
      common_fixed_point.py
  research/
    notes/
    notebooks/
  runs/
    2026-03-31-common-node-fixed-point/
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

Current active stages:

- `subset_merged`
- `subset_pruned`
- `subset_pruned.forced`
- `subset_pruned.forced.pair_reduced`
- `common_fixed_point`
- `common_fixed_point.node_filtered`
- `common_node_fixed_point`

### `src/steps/`

One reduction operation per module.

Examples:

- `subset_absorption.py`
- `forced_bits.py`
- `pair_reduction.py`
- `node_filter.py`

Recommended interface:

```python
def run_step(tables: list[dict], state: dict, round_index: int) -> tuple[list[dict], dict, bool]:
    ...
```

### `src/pipeline/`

Composition layer:

- fixed-point runners;
- pipeline profiles;
- orchestration;
- run-state serialization.

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

- old flat-layout files;
- pilot scripts;
- one-off experiments;
- retired artifacts.

## Migration Notes

### Scripts

- `merge_subset_tables.py` -> `src/steps/subset_absorption.py`
- `collapse_bit_pairs.py` -> `src/steps/pair_reduction.py`
- `reduce_nodes_fixed_point.py` -> `src/steps/node_filter.py`
- `reduce_common_fixed_point.py` -> `src/pipeline/common_fixed_point.py`
- one-off research scripts -> `archive/` or `research/`

### Data

- `tables.json` -> `data/raw/tables.json`
- `tables_*.json` -> `data/derived/`
- `*_report.json` -> `data/reports/`
- `*_bit_components.json`, `*_bit_rewrite_map.json`, `*_forced_bits.json` -> `data/derived/`

## Recommended Next Steps

1. Normalize artifact naming by stage.
2. Add explicit pipeline profiles under `configs/pipeline/`.
3. Add regression tests for step-level invariants.
4. Record each important run in `runs/<run-id>/summary.md`.
