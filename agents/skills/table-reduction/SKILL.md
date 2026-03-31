# Table Reduction

## Purpose

This skill helps an AI agent work safely on the table-reduction project.

## Project Assumptions

- A table is an object with `bits` and `rows`.
- Bit position inside `bits` defines bit position inside each row mask.
- When arity allows it, rows should be processed with bit operations on `uint32`.

## Default Workflow

1. Start with a system summary.
2. Check whether a similar reduction step already exists.
3. Add new operations as steps in the common fixed-point pipeline.
4. After each step, record:
   - table count;
   - bit count;
   - row count;
   - arity distribution.
5. Name new artifacts and reports consistently by stage.

## Safety Rules

- Do not delete historical artifacts without an explicit request.
- Do not change the `bits` / `rows` format.
- Do not claim improvement without numeric before/after comparison.
- If a step only changes rows, the report must state that explicitly.
