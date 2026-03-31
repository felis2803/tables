# Data Model

## Core Object

Each table is a JSON object:

```json
{
  "bits": [12, 48, 77],
  "rows": [0, 3, 5]
}
```

## Semantics

- `bits` is an ordered array of bit identifiers.
- The position of a bit inside `bits` defines its position inside each row mask.
- `rows` stores allowed assignments for those bits.
- Each row is treated as an unsigned integer mask.

For bit `bits[i]`, its value inside row `row` is:

```text
(row >> i) & 1
```

## Normal Form

- `bits` should be sorted in ascending order in persisted artifacts unless a step explicitly needs a transient order.
- `rows` should be deduplicated.
- Tables with identical `bits` are merged by intersecting their row sets.

## Reduction Invariants

- subset absorption only removes rows or entire included tables;
- forced-bit propagation removes fixed bits from tables and records them separately;
- pair reduction rewrites equivalent or opposite bits to one representative;
- node filtering only removes rows;
- every persisted stage must remain logically equivalent to the previous stage.
