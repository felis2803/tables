# Generation Chain

This document defines the project's current notion of the origin-reachability generation chain for bits.

## Purpose

The generation chain is a derived analysis over one table system and one protected origin set.

It answers:

- which bits are reachable from `origins` through exact one-bit functional dependencies;
- at which minimum generation each reachable bit first appears;
- which bits remain unreachable from `origins`;
- which bits are constant in the current system but are not counted as origin-derived generations.

This analysis is useful both on the raw system and on reduced pipeline outputs.

## Base Set

Generation `0` is exactly the chosen `origins` array.

The current standard origin set lives in:

- [data/raw/origins.json](C:/projects/tables/data/raw/origins.json)

## Dependency Rule

For one table and one target bit `t` inside that table, a determining bit set `D` is an exact one-bit dependency witness when:

- `t` is not in `D`;
- for every two rows of the table that agree on `D`, the value of `t` is also the same.

Equivalent view:

- inside that table, `t` is a function of `D`.

The current implementation keeps only minimal determining sets per target bit:

- if `D1` already determines `t`, a strict superset `D2` is not kept as an independent minimal witness.

## Generation Rule

A non-origin bit belongs to generation `g > 0` when:

- it has at least one exact one-bit dependency witness;
- every bit in one such witness is already known in generations `< g`;
- and `g` is the smallest generation where that is true.

The implementation computes this by iterative closure:

1. start with the known-bit set equal to `origins`;
2. add every bit whose witness uses only already-known bits;
3. record all bits first added in that round as the next generation;
4. repeat until either the requested generation bound is reached or no new bits can be added.

## Constant Bits

The current standard excludes empty determining sets from the generation chain.

That means:

- a bit that is constant in the current system is tracked separately;
- it is not placed into generation `1` just because it is determined by the empty set.

This matters for the raw system:

- bits `0..29` are constant in one-bit tables;
- they are not origin-derived;
- therefore they are listed as constants, not as generation-`1` bits.

## Reachability

For one chosen system:

- `reachable` means the bit appears in generation `0` or later in the iterative closure;
- `unreachable` means the bit exists in the system but never enters any generation.

The current implementation writes both:

- per-bit generation assignments for every reachable bit;
- the explicit list of unreachable bits.

## Interpretation On Raw Versus Pipeline Systems

Generation chains must be interpreted relative to the analyzed system.

For the raw system:

- the chain describes exact one-bit dependencies present in the raw tables.

For the common fixed-point pipeline output:

- the chain describes exact one-bit dependencies present after the pipeline reductions;
- because the baseline pipeline includes `single_table_bit_filter`, which is lossy, this chain is not semantically identical to the raw-system chain.

In other words:

- the pipeline generation chain is a property of the reduced system, not a proof about the untouched raw system.

## Canonical Artifacts

Use stage-based artifact names.

Derived artifacts:

- `bits.<stage>.generations.json`
- `bits.<stage>.generation_by_bit.json`
- `bits.<stage>.unreachable_from_origins.json`
- `bits.<stage>.constant.json`

Report artifacts:

- `report.<stage>.generation_chain.json`
- `summary.<stage>.generation_chain.json`
- `report.<left-stage>_vs_<right-stage>.generation_chain.json` for canonical raw-vs-reduced comparisons

Recommended current stages:

- `raw`
- `common_node_fixed_point`

Recommended current comparison report:

- `report.raw_vs_common_node_fixed_point.generation_chain.json`

## CLI

Use the dedicated CLI:

```powershell
cargo run --release --bin bit_generations -- --help
```

Typical raw-system run:

```powershell
cargo run --release --bin bit_generations -- `
  --input data/raw/tables.json `
  --origins data/raw/origins.json `
  --until-fixed-point `
  --generations-output data/derived/bits.raw.generations.json `
  --generation-by-bit-output data/derived/bits.raw.generation_by_bit.json `
  --unreachable-bits-output data/derived/bits.raw.unreachable_from_origins.json `
  --constant-bits-output data/derived/bits.raw.constant.json `
  --summary-output data/reports/summary.raw.generation_chain.json `
  --report-output data/reports/report.raw.generation_chain.json
```

Typical pipeline-output run:

```powershell
cargo run --release --bin bit_generations -- `
  --input data/derived/tables.common_node_fixed_point.json `
  --origins data/raw/origins.json `
  --until-fixed-point `
  --generations-output data/derived/bits.common_node_fixed_point.generations.json `
  --generation-by-bit-output data/derived/bits.common_node_fixed_point.generation_by_bit.json `
  --unreachable-bits-output data/derived/bits.common_node_fixed_point.unreachable_from_origins.json `
  --constant-bits-output data/derived/bits.common_node_fixed_point.constant.json `
  --summary-output data/reports/summary.common_node_fixed_point.generation_chain.json `
  --report-output data/reports/report.common_node_fixed_point.generation_chain.json
```
