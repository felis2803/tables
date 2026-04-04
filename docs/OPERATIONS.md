# Reduction Operations

This document describes the table operations that active agents are expected to understand before changing the pipeline.

## Shared Conventions

- A table constrains a specific ordered bit set.
- `bits[i]` is the bit identifier stored at position `i`.
- Each row is an integer mask over that local bit order.
- A row value for `bits[i]` is `((row >> i) & 1)`.
- Persisted tables should use ascending bit order.
- Unless a step explicitly says otherwise, row processing should prefer bitwise integer operations over per-bit object-heavy code.

## Canonicalization

Canonicalization is not a separate research step, but it is a required normalization:

- sort table bits in ascending order;
- remap row masks to the new local positions;
- deduplicate rows;
- if two tables end up with the same `bits`, merge them by intersecting their row sets.

This is the baseline normalization expected after projection-heavy steps.

## Table Merge

Table merge is different from subset absorption.

Input:

- table `A` with bits `Ba`;
- table `B` with bits `Bb`.

Output:

- a new table on the union of the bit sets `Ba` and `Bb`.

Semantics:

- a row of the merged table is valid if its projection onto `Ba` is a row of `A`;
- and its projection onto `Bb` is a row of `B`.

Equivalent view:

- this is the natural join of the two row sets over their shared bits.

Practical consequences:

- if `Ba` and `Bb` are disjoint, merge is a Cartesian product of rows;
- if `Ba = Bb`, merge is row intersection;
- if shared bits are inconsistent for all row pairs, the merged table is empty.

Implementation note:

- the fast path should bucket rows by assignments on shared bits, then combine only matching buckets;
- row construction should stay in integer-mask form and avoid per-bit object-heavy joins.

Pipeline note:

- pairwise merge remains available as a standalone operation for experiments and targeted runs;
- when pairwise merge is used as a pipeline step, it may immediately drop source tables that are implied by retained merged tables;
- the current fixed-point runner does not invoke pairwise merge by default;
- the standalone pairwise merge CLI is limited by a maximum merged arity parameter, which defaults to `16`.

## Subset Absorption

Input:

- subset table `S` with bits `Bs`;
- superset table `T` with bits `Bt`, where `Bs` is a strict subset of `Bt`.

Operation:

- locate the positions of `Bs` inside `Bt`;
- project each row of `T` onto those positions;
- keep only rows of `T` whose projection exists in `S`.

Effect:

- rows can be removed from the superset table;
- after full inclusion, the subset table can be removed from the active system.

Agent note:

- this is a semantic restriction, not a set-theory shortcut over raw row counts;
- the meaningful test is projection membership, not table size.

## Forced Bits

For each table:

- compute bitwise `AND` across all rows;
- compute bitwise `OR` across all rows.

Interpretation:

- if bit position `i` is `1` in the `AND` result, the corresponding bit is forced to `1`;
- if bit position `i` is `0` in the `OR` result, the corresponding bit is forced to `0`.

Propagation:

- every table containing that bit must drop rows that disagree with the forced value;
- the forced bit is then removed from the table schema;
- if a table becomes a tautology after removing forced bits, it should be removed;
- forced original bits must be recorded separately from the reduced system.

## Pair Reduction

Goal:

- detect pairs of bits where one bit is fully determined by another;
- the relation is either equality or negation.

Per-table test:

- inspect each bit pair across all rows;
- if only `00` and `11` occur, the pair is equal;
- if only `01` and `10` occur, the pair is opposite.

Global reduction:

- collect all such relations across tables;
- build transitive components with parity-aware union-find;
- choose one representative bit per component;
- rewrite every other bit in the component to the representative, with inversion when needed;
- remove contradictions created by inconsistent row assignments during rewriting.

Agent note:

- pair reduction is transitive;
- if `A = B` and `B = !C`, then `A = !C`;
- the pipeline should keep only one representative bit per connected component.

## Node Filtering

Nodes encode common projected subtables shared by multiple tables.

Construction idea:

- identify exact shared bit intersections between tables;
- use the shared bit set as the node schema;
- project each member table onto that shared schema;
- intersect those projected row sets to obtain the node subtable.

Propagation:

- a table row is valid only if its projection onto the node bits belongs to the node row set;
- after a table changes, every node containing that table must be recomputed;
- if a recomputed node loses rows, its tighter restriction must propagate to all member tables;
- repeat until no node or table changes.

Effect:

- node filtering only removes rows;
- it can expose new forced bits, new pair relations, and new subset absorptions in later rounds.

## Fixed-Point Loop

The active baseline pipeline applies:

1. subset absorption
2. forced bits
3. pair reduction
4. node filtering

The loop stops only when a full round makes no change.

## Reporting Requirements

Every new step or variant should report at least:

- input and output table count;
- input and output bit count;
- input and output row count;
- input and output rank summaries;
- arity distribution before and after;
- number of changed tables;
- number of removed rows;
- any removed or rewritten bits;
- whether the step removes rows only, bits only, tables only, or a mix.

Recommended rank fields:

- `input_rank_summary`
- `final_rank_summary` or `output_rank_summary`
