# Table Reduction

## Purpose

This skill helps an AI agent work safely on the table-reduction project without asking the user to restate the domain model each time.

## Required Reading

Before editing code or interpreting data, read:

1. [README.md](C:/projects/tables/README.md)
2. [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
4. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
5. [AGENT_ONBOARDING.md](C:/projects/tables/docs/AGENT_ONBOARDING.md)

## Project Assumptions

- A table is an object with `bits` and `rows`.
- Bit position inside `bits` defines bit position inside each row mask.
- When arity allows it, rows should be processed with bit operations on `uint32`.
- Table rows are local masks, not global bit assignments.
- Equal-schema tables merge by row intersection.
- The active baseline pipeline is a fixed-point loop, not a one-pass reducer.

## Operations You Must Know

- subset absorption
- forced-bit propagation via row `AND` and `OR`
- pair reduction via equal/opposite bit relations
- node filtering via shared projected subtables

Do not change or describe these operations loosely. Use the project docs terminology.

## Default Workflow

1. Start with a system summary.
2. Check whether a similar reduction step already exists.
3. Add new operations as steps in the common fixed-point pipeline.
4. If the task changes semantics, update docs before or with the code.
5. After each step, record:
   - table count;
   - bit count;
   - row count;
   - arity distribution.
6. Name new artifacts and reports consistently by stage.

## Artifact Rules

- Use stage-based names through [artifacts.py](C:/projects/tables/src/utils/artifacts.py).
- Keep generated systems under `data/derived/`.
- Keep machine-readable reports under `data/reports/`.
- Keep important run metadata under `runs/<run-id>/`.

## Implementation Rules

- Prefer bitwise integer operations over slow object-heavy representations.
- Preserve canonical ascending bit order in persisted outputs.
- Do not leave an empty table in the system.
- If a step exposes a tautology, remove it and report it.
- If a step only removes rows, state that explicitly in the report.
- If a step rewrites bits, preserve a mapping back to original bits.

## Safety Rules

- Do not delete historical artifacts without an explicit request.
- Do not change the `bits` / `rows` format.
- Do not claim improvement without numeric before/after comparison.
- Do not assume a new operation is correct until it is run to a fixed point when applicable.
- If semantics or invariants change, update the docs in the same task.
