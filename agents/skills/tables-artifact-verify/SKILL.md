---
name: tables-artifact-verify
description: Validate `.tables` artifacts and compare them against JSON artifacts or pipeline outputs in this repository. Use when Codex needs to check semantic identity after conversion, compare output artifacts from two runs, or measure `.tables` versus JSON size and runtime impact.
---

# Tables Artifact Verify

## Required Reading

Before validating `.tables` artifacts, read:

1. [docs/BINARY_FORMAT.md](C:/projects/tables/docs/BINARY_FORMAT.md)
2. [docs/PERF_WORKFLOW.md](C:/projects/tables/docs/PERF_WORKFLOW.md)
3. [src/bin/tables_convert.rs](C:/projects/tables/src/bin/tables_convert.rs)
4. [src/main.rs](C:/projects/tables/src/main.rs) when the task includes pipeline runs

## Default Workflow

1. Decide what needs to match:
   - raw converted input artifacts
   - final table outputs only
   - full pipeline outputs including reports
2. If one side is `.tables`, convert it back to JSON before equality checks on `Table` content.
3. For semantic identity, compare deserialized JSON structures, not raw bytes.
4. For performance comparisons, measure:
   - elapsed wall time
   - peak working set
   - peak private bytes
   - input artifact size
   - output artifact size
5. Use the same build and the same flags on both sides of the comparison.

## Comparison Rules

- Artifact bytes may differ even when table content is identical.
- `report.json` may differ in path strings or output filenames; compare table outputs separately.
- If a comparison fails, report whether the mismatch is in:
   - origin arrays
   - table count
   - schema bits
   - row sets

## Typical Commands

```powershell
target\release\tables.exe --input <tables.json-or-tables> --output <output.json-or-tables> ...
target\release\tables_convert.exe tables-to-json --input <file.tables> --tables-output <tables.json>
node -e "const fs=require('fs'); const assert=require('assert'); const a=JSON.parse(fs.readFileSync('a.json','utf8')); const b=JSON.parse(fs.readFileSync('b.json','utf8')); assert.deepStrictEqual(b,a);"
```

## Expected Output

A validation task should usually end with:

- a clear identity verdict
- before/after sizes
- before/after timing and memory numbers when benchmarking
- exact artifact paths that were compared

