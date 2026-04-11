---
name: tables-binary-format
description: Work on the `.tables` binary container format in this repository. Use when Codex needs to change the `.tables` specification, edit the reader/writer implementation, add support for new record semantics, or debug parsing/serialization issues in `src/tables_file.rs`.
---

# Tables Binary Format

## Required Reading

Before changing the format or its implementation, read:

1. [docs/BINARY_FORMAT.md](C:/projects/tables/docs/BINARY_FORMAT.md)
2. [docs/DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md)
3. [src/tables_file.rs](C:/projects/tables/src/tables_file.rs)
4. [src/common.rs](C:/projects/tables/src/common.rs)
5. [src/main.rs](C:/projects/tables/src/main.rs) if the task touches pipeline I/O

## What This Skill Covers

- `.tables` file header, record header, and record alignment
- origin-array records and table records
- `RowWordKind`, `RowWords`, `StoredTable`, `TablesReader`, and `TablesWriter`
- automatic `.json` / `.tables` dispatch through `common::read_tables` and `common::write_tables`

## Default Workflow

1. Confirm whether the task changes the binary spec, the Rust API, or both.
2. Keep [docs/BINARY_FORMAT.md](C:/projects/tables/docs/BINARY_FORMAT.md) and [src/tables_file.rs](C:/projects/tables/src/tables_file.rs) in sync in the same change.
3. Prefer preserving streaming semantics:
   - no mandatory seek/backpatch
   - no required global index
   - no unnecessary intermediate materialization
4. Preserve direct mapping to primitive integer row words whenever possible.
5. If the on-disk semantics change, add or update roundtrip tests in [src/tables_file.rs](C:/projects/tables/src/tables_file.rs).
6. Rebuild and rerun the `.tables` tests before finishing.

## Invariants To Preserve

- all integers are little-endian
- records are self-delimiting
- table `bits` stay `u32`
- table rows stay raw primitive unsigned integers, not varints or packed sub-byte encodings
- `common::Table` remains the library's normal in-memory `u32` table representation
- `.tables` input must still be readable through `common::read_tables`

## Validation Checklist

- `cargo test --lib`
- `cargo build --release`
- if conversion behavior changed, roundtrip a real artifact through `tables_convert`

