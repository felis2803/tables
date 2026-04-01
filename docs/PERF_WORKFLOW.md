# Performance Workflow

Use this workflow when changing the pipeline for runtime, throughput, or memory reasons and the intended semantics should stay the same.

## Goal

Produce a measurable improvement without weakening result validation.

## Standard Flow

1. Fix a baseline commit, input, and flag set before changing code.
2. Run the baseline in `--release` and save the full outputs and process metrics.
3. Measure at least:
   - wall time;
   - total CPU time;
   - peak working set;
   - peak private memory;
   - peak virtual memory.
4. Exclude build time from the measurements.
5. If the hotspot is not obvious, add temporary internal checkpoints around stages or suspected operations.
6. Identify not only the absolute peak, but also the largest memory jump between adjacent checkpoints.
7. Quantify the dominant structure numerically:
   - number of keys or entries;
   - total references or links;
   - average fanout or support;
   - rough lower-bound memory estimate.
8. Prefer an algorithmic fix over container churn when the root cause is eager materialization or redundant work.
9. If semantics should stay unchanged, add a regression test that compares the new logic with the old logic on small controlled inputs.
10. Compare the candidate against the baseline on the same input and flags, preferably from a separate worktree for the baseline commit.
11. Verify output identity from persisted artifacts, not only from headline counts.
12. Remove temporary tracing or diagnostic code unless it is intentionally kept as a supported feature.
13. Run the final candidate again after cleanup.
14. Only then commit and push.

## Required Comparison Rules

- Use the same executable mode for both sides, normally `target/release/tables.exe`.
- Keep `max_merge_arity`, round limits, and all other flags identical.
- Save both runs under `runs/<run-id>/`.
- Preserve both ordinary outputs and measurement logs.
- Do not claim improvement from debug builds, partial runs, or mixed workloads.

## Output Validation

When the intended semantics are unchanged:

- compare the final artifact hashes, not only the summary counters;
- normalize path-like fields in `report.json` before comparing reports from different run folders;
- treat any artifact mismatch as a regression until explained.

Typical final artifacts to compare:

- `tables`
- `forced`
- `mapping`
- `components`
- `dropped`
- `relations`
- `nodes`
- normalized `report.json`

## What To Record

Each important performance task should leave behind:

- the baseline commit or reference;
- the candidate commit or worktree state;
- the exact command line;
- measured runtime and memory numbers;
- a short statement of whether outputs matched;
- the final before/after delta in percent and absolute units.

## Guardrails

- Do not replace correctness checks with speed claims.
- Do not keep always-on tracing in production code unless the task explicitly asks for it.
- Do not trust intuition when a direct count or trace can settle the question.
- Do not optimize around a suspected hotspot until the hotspot is measured.
- Do not switch algorithms silently; if semantics change, document that separately.
