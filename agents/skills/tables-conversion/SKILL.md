---
name: tables-conversion
description: Convert between JSON table artifacts and the `.tables` binary format in this repository. Use when Codex needs to create `.tables` files, extract JSON back out, bundle origins with tables, or verify roundtrip identity with `src/bin/tables_convert.rs`.
---

# Tables Conversion

## Required Reading

Before converting artifacts, read:

1. [docs/BINARY_FORMAT.md](C:/projects/tables/docs/BINARY_FORMAT.md)
2. [src/bin/tables_convert.rs](C:/projects/tables/src/bin/tables_convert.rs)
3. [src/common.rs](C:/projects/tables/src/common.rs)

## Supported Paths

- `json-to-tables`
- `tables-to-json`

Primary binary:

- [src/bin/tables_convert.rs](C:/projects/tables/src/bin/tables_convert.rs)

## Default Workflow

1. Identify whether the source is:
   - tables JSON only
   - tables JSON plus origins JSON
   - `.tables` bundle with one or more origin arrays
2. Use the release binary for real conversions:

```powershell
target\release\tables_convert.exe json-to-tables --tables <tables.json> [--origins <origins.json>] --output <file.tables>
target\release\tables_convert.exe tables-to-json --input <file.tables> --tables-output <tables.json> [--origins-output <origins.json>]
```

3. When origins are present, keep them in the same `.tables` file unless the user asked otherwise.
4. After conversion, verify identity by converting back and comparing the restored JSON payloads.
5. Report the input and output byte sizes.

## Rules

- Do not claim roundtrip success without comparing restored JSON against the original source.
- When a `.tables` file contains multiple origin arrays, explicitly select the requested `--origin-name`.
- Prefer writing derived conversion artifacts under `data/derived/` unless the user asked for a specific target path.

