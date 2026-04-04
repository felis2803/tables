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
- A table represents the set of allowed local assignments for exactly the bits listed in `bits`.
- Rows are local to the table. The same integer value means different assignments if the `bits` arrays differ.

For bit `bits[i]`, its value inside row `row` is:

```text
(row >> i) & 1
```

Example:

```json
{
  "bits": [10, 25, 40],
  "rows": [0, 3, 5]
}
```

Interpretation:

- row `0` means `10=0, 25=0, 40=0`
- row `3` means `10=1, 25=1, 40=0`
- row `5` means `10=1, 25=0, 40=1`

## Projection

Many reductions project a row from a larger table onto a subset of its bits.

If a table uses local order `[b0, b1, b2, b3]` and we project to `[b1, b3]`, the projected row is rebuilt in the new local order:

- projected bit `0` comes from original position of `b1`
- projected bit `1` comes from original position of `b3`

This is why projection must remap positions, not simply mask by global bit id.

## Equality Of Tables

Two tables are considered schema-equal only if they have the same canonical ordered `bits` array.

When schema-equal tables are merged:

- their `rows` are intersected;
- they do not get unioned.

## Rank Metric

The project uses a derived table metric called `rank`:

```text
rank = row_count ** (1 / bit_count)
```

Equivalently:

```text
rank ** bit_count = row_count
```

Interpretation:

- `rank` is the per-bit growth factor implied by the number of rows;
- for fixed arity, larger `rank` means a less restrictive table;
- `rank` is a summary metric only and does not replace semantic checks based on actual row sets.

## Normal Form

- `bits` should be sorted in ascending order in persisted artifacts unless a step explicitly needs a transient order.
- `rows` should be deduplicated.
- Tables with identical `bits` are merged by intersecting their row sets.

## Reduction Invariants

- pairwise merge creates a new table on the union of two schemas and may immediately replace the source tables when the merge is retained;
- pairwise merge remains available as a standalone operation, but it is not part of the active default pipeline;
- subset absorption only removes rows or entire included tables;
- forced-bit propagation removes fixed bits from tables and records them separately;
- single-table bit filtering removes bits whose support is exactly one active table by projecting them out of that table;
- pair reduction rewrites equivalent or opposite bits to one representative;
- zero-collapse bit filtering removes bits whose zeroed projection contains exactly half as many distinct rows as the original table;
- tautology filtering removes tables whose row sets cover every assignment on their schemas;
- node filtering only removes rows;
- all persisted stages except `single_table_bit_filter` preserve logical equivalence to the previous stage;
- `single_table_bit_filter` is an intentional lossy heuristic in the active baseline pipeline.

## Implementation Guidance

- prefer integer bit operations over per-bit boxed structures when possible;
- use `uint32`-style row handling for active steps unless arity grows beyond that assumption;
- keep persisted artifacts deterministic: sorted bits, sorted rows, stable artifact names.
- when writing reports, include rank summaries for input and output systems.
