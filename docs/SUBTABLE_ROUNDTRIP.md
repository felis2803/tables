# Subtable Roundtrip

## Purpose

This document fixes the current standard method for splitting one large table into exact lower-arity subtables and checking whether the original table can be reconstructed from them.

This is an analysis and factorization workflow.
It is not part of the default reduction pipeline in [docs/PIPELINE.md](C:/projects/tables/docs/PIPELINE.md).

## Scope

Input:

- one source table with canonical ordered `bits`;
- deduplicated `rows`.

Output:

- exact projected subtables for selected arities;
- filtered subtable pools with tautologies removed before join;
- progressive reconstruction checks from pools `2`, `2+3`, `2+3+4`, and so on when requested.

## Exhaustive Method

For a source table `T`:

1. extract every exact 2-bit projection of `T`;
2. remove the tautological 2-bit projections from that pool;
3. attempt exact reconstruction of `T` from the remaining 2-bit pool;
4. if reconstruction fails, extract every exact 3-bit projection, remove the tautological ones, add the rest, and retry;
5. if reconstruction still fails, do the same for exact 4-bit projections and retry.

The current standard stopping rule is:

- stop as soon as the reconstructed table matches the source table exactly;
- if the requested maximum subtable arity is reached and the source table still does not match, report the failure and keep the last reconstructed table.

This exhaustive method remains the reference workflow because it is straightforward and reproducible.

## Selective Method

The repository now also keeps a performance-oriented selective variant.

It uses the same exact 2-bit start:

1. extract every exact 2-bit projection of `T`;
2. remove the tautological 2-bit projections from that pool;
3. reconstruct from the remaining 2-bit pool.

Then, instead of appending every 3-bit and 4-bit projection:

- if the current reconstruction is still missing source bits, consider only higher-arity candidates that introduce those missing bits;
- if the current reconstruction already covers all source bits but still has extra rows, derive candidate higher-arity factors from extra-row witnesses and add them greedily;
- at each step, prefer the candidate that first minimizes the count of still-missing source bits and then minimizes the count of extra reconstructed rows.

The selective method still uses only:

- exact source projections;
- exact natural joins;
- exact equality checks against the source table.

But it does not guarantee the same factor pool as the exhaustive method.
It is a retained optimization strategy, not the normative reference procedure.

## Exact Projection Rule

For one chosen subset of source bit positions:

- keep those bits in their source-local order;
- project every source row onto those positions;
- sort and deduplicate the projected rows;
- persist the resulting subtable.

This is an exact projection.
No latent bits, approximations, or learned rewrites are introduced in this workflow.

## Why Tautologies Are Removed Before Join

The fixed method now removes tautologies from every arity before adding that layer to the join pool.

Reason:

- tautologies carry no restriction and only inflate the join pool;
- removing them is safe for the intended progressive exact-reconstruction check;
- the report still keeps both the full extracted counts and the filtered selected counts, so the artifact set remains reproducible.

If that policy changes in the future, this document must be updated explicitly.

## Reconstruction Semantics

Reconstruction is the exact natural join of the current pool of factor tables:

- if two factors share bits, rows must agree on those shared assignments;
- if two factors are disjoint, the join on those factors is a Cartesian product;
- if the full join becomes empty, the factor pool is contradictory.

The reconstruction matches the source only when:

- reconstructed `bits == source.bits`;
- reconstructed `rows == source.rows`.

Row-count equality alone is not sufficient.

## Canonical Naming

Recommended artifact names:

- `2_bit.all.tables`
- `2_bit.non_taut.tables`
- `3_bit.all.tables`
- `3_bit.non_taut.tables`
- `4_bit.all.tables`
- `4_bit.non_taut.tables`
- `pool.2.tables`
- `pool.2_3.tables`
- `pool.2_3_4.tables`
- `reconstructed.from_2.tables`
- `reconstructed.from_2_3.tables`
- `reconstructed.from_2_3_4.tables`

## Rust Entrypoints

Shared implementation:

- [src/subtable_roundtrip.rs](C:/projects/tables/src/subtable_roundtrip.rs)

Standalone CLI:

- [src/bin/subtable_roundtrip.rs](C:/projects/tables/src/bin/subtable_roundtrip.rs)

End-to-end pipeline plus best-rank-chain driver:

- [src/bin/pipeline_rank_chain_subtables.rs](C:/projects/tables/src/bin/pipeline_rank_chain_subtables.rs)

## CLI Usage

```powershell
cargo run --release --bin subtable_roundtrip -- --input <tables> --table-index <n> --output-root <dir> [--strategy <exhaustive|selective>]
```

Example:

```powershell
cargo run --release --bin subtable_roundtrip -- `
  --input codex-output-2026-04-22-pipeline-rank-chain-subtables-01/rank-chain/final.tables `
  --table-index 0 `
  --output-root runs/2026-04-22-subtable-roundtrip `
  --strategy exhaustive `
  --max-subtable-arity 4
```

Selective example:

```powershell
cargo run --release --bin subtable_roundtrip -- `
  --input codex-output-2026-04-22-pipeline-rank-chain-subtables-01/rank-chain/final.tables `
  --table-index 0 `
  --output-root runs/2026-04-22-subtable-roundtrip-selective `
  --strategy selective `
  --max-subtable-arity 4
```

## Reporting Expectations

A roundtrip report should include:

- source table summary;
- extracted subtable counts by arity;
- selected subtable counts by arity after tautology filtering;
- count of removed tautologies by arity when relevant;
- for each pool, the factor count, factor-arity distribution, tautology count, reconstructed summary, and exact match flag.

For the selective strategy, also report:

- candidate counts by arity;
- selected factor counts by arity;
- missing-bit and extra-row counts before and after each selective stage.
