# Reduction Pipeline

Current baseline pipeline:

1. `subset_absorption`
2. `forced_bits`
3. `pair_reduction`
4. `node_filter`

This step list is executed until a fixed point is reached.

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
