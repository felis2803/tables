# Agent Onboarding

If you are a new agent on this repository, read these files in order before editing the pipeline:

1. [README.md](C:/projects/tables/README.md)
2. [DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [OPERATIONS.md](C:/projects/tables/docs/OPERATIONS.md)
4. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
5. [SKILL.md](C:/projects/tables/agents/skills/table-reduction/SKILL.md)

## Minimum Mental Model

- A table is a constraint over an ordered local set of bit identifiers.
- Rows are integer masks over local bit positions, not dictionaries or global masks.
- Projection and rewriting always operate relative to local bit order.
- Persisted artifacts should use canonical ascending bit order.
- The active pipeline is a fixed-point loop over multiple reduction steps, not a single pass.
- `rank` is a derived metric: `row_count ** (1 / bit_count)`.

## Before You Change Anything

- identify whether the change is a new reduction step, a faster implementation of an existing step, or documentation only;
- verify artifact names through [artifacts.py](C:/projects/tables/src/utils/artifacts.py);
- preserve logical equivalence unless the task explicitly asks for a new semantics-changing operation;
- produce before/after counts for tables, bits, rows, and rank summaries.

## When Adding A New Step

- define the step precisely in docs first;
- integrate it into the common fixed-point runner only after its standalone behavior is clear;
- state whether it removes rows, bits, tables, or only rewrites identifiers;
- ensure downstream steps still accept the resulting table form.
