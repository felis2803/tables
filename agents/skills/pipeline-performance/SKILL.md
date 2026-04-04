---
name: pipeline-performance
description: Investigate and improve runtime or memory usage of the tables fixed-point pipeline. Use when Codex needs to profile hotspots, compare a baseline against a candidate, add temporary instrumentation, validate semantic equivalence after a performance change, or document performance work in this repository.
---

# Pipeline Performance

## Required Reading

Before changing code for performance reasons, read:

1. [README.md](C:/projects/tables/README.md)
2. [PIPELINE.md](C:/projects/tables/docs/PIPELINE.md)
3. [PERF_WORKFLOW.md](C:/projects/tables/docs/PERF_WORKFLOW.md)
4. [AGENT_ONBOARDING.md](C:/projects/tables/docs/AGENT_ONBOARDING.md)
5. [main.rs](C:/projects/tables/src/main.rs) if the task touches the runner, outputs, or measurement flags
6. the step modules that dominate the measured run

## Default Workflow

1. Fix a baseline commit, input, and flag set.
2. Measure the baseline in `--release` and save outputs under `runs/<run-id>/`.
3. Record wall time, CPU time, peak working set, peak private memory, and peak virtual memory.
4. If the hotspot is unclear, add temporary internal checkpoints around the suspected operations.
5. Quantify the structure that creates the hotspot with counts, fanout, and a rough memory estimate.
6. Prefer removing eager materialization or redundant work over micro-tuning container types.
7. If semantics should stay unchanged, add a regression test that compares old and new behavior on small inputs.
8. Compare candidate and baseline on the same workload, ideally with the baseline in a separate worktree.
9. Verify artifact identity from hashes and a normalized `report.json`.
10. Remove temporary diagnostics and rerun the final candidate before committing.
11. If the workload is `zero-collapse`, benchmark `bit_zero_collapse_all --summary-only` before timing the full JSON-emitting variant, so metric compute time is not hidden by report serialization.
12. If you are benchmarking the pipeline impact of zero-collapse bit filtering, compare the same build with and without `--disable-zero-collapse-bit-filter`.

## Measurement Rules

- Use the same flags for baseline and candidate.
- Exclude build time from reported runtime.
- Prefer measuring the release binary directly instead of timing `cargo run`.
- When the measured tool can emit very large reports, separate compute-only timing from serialization timing and report both.
- Keep run folders and comparison artifacts so the result is reproducible.

## Validation Rules

- Do not claim an optimization from summary counts alone.
- Do not accept a memory win that changes semantics unless the user explicitly asked for a semantic change.
- Do not leave tracing hooks in production code unless they are intentionally supported.
- If a hotspot is large enough to matter, explain it numerically before choosing the fix.

## Expected Outputs

Performance work should usually end with:

- a before/after metric table;
- a statement that outputs matched or differed;
- a committed implementation that no longer depends on temporary diagnostics;
- updated docs when the workflow or supported measurement path changed.
