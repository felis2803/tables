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
- forced original bits must be recorded separately from the reduced system.

## Single-Table Bit Filter

Goal:

- remove bits whose current support is exactly one active table.

Operation:

- count, for each bit, how many active tables contain it;
- for each table, project away every bit whose count is `1`;
- deduplicate the projected rows;
- if projection creates duplicate schemas, intersect them in canonical form.

Effect:

- remove bits from the active system;
- sometimes reduce row counts because projection can collapse duplicate rows;
- expose new pair relations and tautologies for later steps.

Semantics note:

- this step is intentionally lossy;
- unlike subset absorption, forced-bit propagation, pair reduction, and tautology filtering, it does not preserve strict logical equivalence of the table system.

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

## Zero-Collapse Bit Filter

Goal:

- remove bits that do not encode any remaining restriction inside their current table.

Test:

- for one table bit, zero that bit in every row;
- deduplicate the projected rows;
- if the projected table has exactly half as many rows as before, the bit is locally unrestricted and can be removed.

Equivalent criterion:

- `row_count_after_zeroing_and_dedup * 2 == row_count_before`
- equivalently, `zero-collapse(bit) == 0.5`.

Effect:

- remove such bits from the table schema;
- deduplicate the projected rows after each removal;
- if several tables collapse to the same schema, intersect them in canonical form;
- often expose tautologies and lower-arity node constraints for later steps.

Semantics note:

- unlike `single_table_bit_filter`, this step preserves logical equivalence;
- if zeroing a bit halves the row count, every surviving projection already appears with both bit values, so the bit carries no restriction.

## Tautology Filter

Goal:

- remove tables that impose no remaining restriction on their own schema.

Test:

- a table on `k` bits is a tautology iff it contains exactly all `2^k` local rows.

Effect:

- remove such tables from the active system;
- leave all other tables unchanged.

## Bounded Neighborhood Join Filter

Goal:

- remove rows that cannot be extended to a jointly consistent assignment across a bounded local neighborhood.

Neighborhood selection:

- start from one anchor table;
- collect direct neighboring tables that share at least one bit with the anchor;
- greedily keep neighbors while the merged union schema stays within a fixed bit budget and the neighborhood stays within a fixed table-count budget;
- only run the exact join when at least three tables fit in that bounded neighborhood.
- the current standalone defaults are `max_union_bits=32`, `max_tables_per_neighborhood=10`, `min_tables_per_neighborhood=3`.

Test:

- compute the exact natural join of the selected neighborhood;
- project the joined rows back onto the anchor table bits;
- keep only anchor rows that still appear in that projection.

Effect:

- only remove rows;
- preserve logical equivalence;
- stronger than pairwise support checks because it can remove rows that are separately compatible with each neighbor but incompatible with them jointly.

## Zero-Collapse Diagnostic

Goal:

- measure how strongly a specific bit contributes to row distinctness inside one table.

Definition:

- zero the chosen bit in every row;
- deduplicate the resulting projected rows;
- compute `zero-collapse(bit) = (row_count_before - row_count_after_zeroing_and_dedup) / row_count_before`.

Interpretation:

- `0.0` means zeroing the bit does not collapse any rows;
- larger values mean more row pairs differ only by that bit;
- this is a diagnostic metric, not a reduction step.

Implementation note:

- the metric definition is the deduplicated projection above;
- optimized implementations may count matching row pairs instead of materializing and sorting projected rows, but they must remain numerically identical to that definition.

## Generation Chain

Goal:

- build the origin-reachability generation chain for bits in one system.

Definition:

- generation `0` is exactly `origins`;
- a later generation contains bits that are exactly determined by already-known bits through minimal non-empty one-bit dependency witnesses inside tables;
- constants are tracked separately and are not counted as origin-derived generations.

Effect:

- no tables are changed;
- the output is a derived artifact set that records generations, per-bit reachability, unreachable bits, and constants.

Agent note:

- this is an analysis over the current system, not a proof about any earlier system;
- on pipeline outputs, the result inherits the semantics of that reduced system, including the effect of any lossy steps that were run earlier.

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

## Subtable Roundtrip

Goal:

- factor one large table into exact lower-arity projected subtables and test whether those subtables reconstruct the source exactly.

Fixed method:

1. extract every exact 2-bit projection of the source table;
2. remove the tautological 2-bit projections from that pool;
3. reconstruct from the remaining 2-bit pool;
4. if that fails, extract every exact 3-bit projection, remove the tautological ones, add the rest, and reconstruct again;
5. if that still fails, do the same for exact 4-bit projections and reconstruct again.

Projection rule:

- keep the chosen source-bit subset in source-local order;
- project every source row onto that subset;
- sort and deduplicate projected rows.

Reconstruction rule:

- reconstruction is the exact natural join of the current factor pool;
- the result matches only when both `bits` and `rows` match the source table exactly.

Policy note:

- tautological projections are removed from every arity before that layer is added to the join pool;
- reports should still keep both the full extracted counts and the filtered selected counts.

Selective variant:

- a retained performance-oriented variant starts from the same 2-bit pool;
- when source bits are still missing, it adds only higher-arity candidates that introduce those missing bits;
- when all source bits are already present but the reconstruction still has extra rows, it adds only chosen higher-arity witnesses that cut those extra rows;
- this variant is exact in its projections and joins, but it is heuristic in factor selection and should be reported as `selective`, not as the exhaustive baseline.

Implementation note:

- use the shared library module `src/subtable_roundtrip.rs` and the CLI `src/bin/subtable_roundtrip.rs` for this workflow;
- this is a retained analysis workflow, not a baseline pipeline step.

## Fixed-Point Loop

The active baseline pipeline applies:

1. subset absorption
2. forced bits
3. single-table bit filter
4. pair reduction
5. zero-collapse bit filter
6. tautology filter
7. node filtering

Bounded neighborhood join filter is retained as a standalone row-filtering operation, but it is not part of the baseline fixed-point runner.

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
