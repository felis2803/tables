# Binary Table Format

## Purpose

This document defines a binary container for table systems used in this repository.

The format is designed around the project's actual in-memory model:

- `bits` is a `Vec<u32>`;
- `rows` is a `Vec<T>` where `T` is an unsigned integer row word;
- `origins` is one or more arrays of `u32` bit ids.

The format prioritizes:

- streaming write without seek/backpatch;
- streaming read without building a global index;
- no required intermediate normalization structures;
- exact mapping to Rust primitive unsigned integer row types;
- minimum decoding work and predictable layout.

This format is intentionally not compressed, not dictionary-based, and not varint-based.

## Scope

Version 1 stores:

- zero or more origin-bit arrays;
- zero or more tables;
- one mandatory end record.

Version 1 supports row words backed by Rust primitive unsigned integer types:

- `u8`
- `u16`
- `u32`
- `u64`
- `u128`

That means a single table in version 1 can represent arity up to 128 bits.

## Semantic Mapping

The semantic model is the same as in [docs/DATA_MODEL.md](C:/projects/tables/docs/DATA_MODEL.md):

- bit ids are stored as `u32`;
- a row is an unsigned integer mask over the table-local order of `bits`;
- for `bits[i]`, the row value is `((row >> i) & 1)`.

The binary format stores the same information as JSON, with no schema rewriting:

- each table record stores its own `bits`;
- each table record stores its own `rows`;
- origin arrays are stored as raw `u32` bit-id arrays in the same file.

## Endianness And Alignment

- All integers are little-endian.
- The file header is 16 bytes.
- Every record starts at a 16-byte aligned file offset.
- Every record stores its own total size in bytes.
- Within a table record, the row payload is aligned to the row word size.
- Within an origin-array record, the value payload is aligned to 4 bytes.

Alignment is included to keep memory-mapped or buffered readers simple and fast.

## File Layout

The canonical layout is:

1. file header
2. zero or more origin-array records
3. zero or more table records
4. one end record

Records are self-delimiting. A writer can emit them in one pass. A reader can consume them in one pass.

## File Header

Offset from file start:

| Offset | Size | Type | Value |
|---|---:|---|---|
| `0` | `8` | bytes | magic = `TBLBIN1\0` |
| `8` | `2` | `u16` | major version = `1` |
| `10` | `2` | `u16` | minor version = `0` |
| `12` | `4` | `u32` | file flags = `0` for v1 |

Readers must reject unknown major versions.

## Common Record Header

Every record begins with the same 32-byte header:

| Offset | Size | Type | Name |
|---|---:|---|---|
| `0` | `1` | `u8` | `tag` |
| `1` | `1` | `u8` | `subtype` |
| `2` | `2` | `u16` | `flags` |
| `4` | `4` | `u32` | `count0` |
| `8` | `8` | `u64` | `count1` |
| `16` | `8` | `u64` | `data_offset` |
| `24` | `8` | `u64` | `record_bytes` |

Definitions:

- `data_offset` is the byte offset from the start of the current record to the primary value payload.
- `record_bytes` is the total byte length of the record, including the 32-byte header and all padding.
- `record_bytes` must be a multiple of 16.
- `record_bytes >= 32`.

Tag values:

- `0x01` = origin-array record
- `0x02` = table record
- `0xFF` = end record

Other tags are reserved.

## Origin-Array Record

An origin-array record stores one named array of origin bit ids.

### Header Meaning

For `tag = 0x01`:

- `subtype`
  - `0` = UTF-8 name bytes
- `flags`
  - bit `0`: values are strictly ascending
  - bit `1`: values are unique
- `count0`
  - name length in bytes
- `count1`
  - number of `u32` values in the origin array
- `data_offset`
  - byte offset from record start to the first origin value
- `record_bytes`
  - total record size

### Payload Layout

Immediately after the 32-byte header:

1. `count0` bytes of UTF-8 name, without trailing NUL
2. zero padding until `data_offset`
3. `count1` little-endian `u32` values
4. zero padding until `record_bytes`

Constraints:

- `data_offset >= 32 + count0`
- `data_offset` must be divisible by `4`
- `record_bytes >= data_offset + 4 * count1`

Canonical recommendations:

- use the name `"origins"` for the primary origin set;
- keep values sorted and unique;
- set both origin flags when that is true.

The format allows multiple origin arrays in one file by repeating this record.

## Table Record

A table record stores one table with its own schema and row words.

### Row Kind

For `tag = 0x02`, `subtype` is the row-word kind:

| `subtype` | Rust type | Bytes | Max table arity |
|---:|---|---:|---:|
| `0` | `u8` | `1` | `8` |
| `1` | `u16` | `2` | `16` |
| `2` | `u32` | `4` | `32` |
| `3` | `u64` | `8` | `64` |
| `4` | `u128` | `16` | `128` |

Let `row_bytes` be the width implied by `subtype`.

### Header Meaning

For `tag = 0x02`:

- `flags`
  - bit `0`: `bits` are strictly ascending
  - bit `1`: `rows` are sorted ascending
  - bit `2`: `rows` are deduplicated
- `count0`
  - number of bit ids in the table
- `count1`
  - number of rows in the table
- `data_offset`
  - byte offset from record start to the first row word
- `record_bytes`
  - total record size

### Payload Layout

Immediately after the 32-byte header:

1. `count0` little-endian `u32` bit ids
2. zero padding until `data_offset`
3. `count1` row words in the exact primitive width implied by `subtype`
4. zero padding until `record_bytes`

Constraints:

- `count0 <= 8 * row_bytes`
- `data_offset >= 32 + 4 * count0`
- `data_offset` must be divisible by `row_bytes`
- `record_bytes >= data_offset + row_bytes * count1`

Canonical recommendations:

- write `bits` in ascending order;
- write `rows` sorted and deduplicated;
- set all corresponding flags when that is true.

### Row Encoding

Rows are stored as little-endian representations of the exact Rust primitive type selected by `subtype`.

Examples:

- `subtype = 2`: each row is stored exactly as one little-endian `u32`
- `subtype = 4`: each row is stored exactly as one little-endian `u128`

There is no per-row prefix, no varint, and no packing across row boundaries.

This keeps the writer and reader on the fast path:

- same-width source data can be written directly;
- same-width target buffers can be read directly;
- width conversion is only needed when the caller explicitly wants a different in-memory row type.

### Zero-Bit Tables

Version 1 allows zero-bit tables:

- `count0 = 0`
- `bits` payload length = `0`
- rows still use one of the defined row kinds

For canonical output, use `subtype = 0` (`u8`) and store rows as zero values only.

## End Record

The end record marks successful completion of the stream.

For `tag = 0xFF`:

- `subtype = 0`
- `flags = 0`
- `count0`
  - number of origin-array records written
- `count1`
  - number of table records written
- `data_offset = 32`
- `record_bytes = 48`

Payload at byte offset `32` inside the end record:

| Offset from record start | Size | Type | Meaning |
|---|---:|---|---|
| `32` | `8` | `u64` | total count of origin values across all origin arrays |
| `40` | `8` | `u64` | total count of table rows across all tables |

Readers should treat a missing end record as a truncated or incomplete file.

## Write Procedure

A writer does not need to know the total number of tables in advance.

Recommended procedure:

1. write the 16-byte file header
2. for each origin array:
   - compute `name_len`, `value_count`, `data_offset`, `record_bytes`
   - write the 32-byte origin header
   - write name bytes
   - write zero padding up to `data_offset`
   - write raw little-endian `u32` values
   - write trailing zero padding up to `record_bytes`
3. for each table:
   - choose `subtype` equal to the actual in-memory row-word type whenever possible
   - compute `bit_count`, `row_count`, `data_offset`, `record_bytes`
   - write the 32-byte table header
   - write raw little-endian `u32` bit ids
   - write zero padding up to `data_offset`
   - write raw row words in the exact chosen width
   - write trailing zero padding up to `record_bytes`
4. write the 48-byte end record with accumulated counts

No backpatching and no seek are required.

## Read Procedure

A streaming reader proceeds in one pass:

1. read and validate the 16-byte file header
2. loop over records:
   - read the 32-byte record header
   - dispatch by `tag`
3. for an origin-array record:
   - read name bytes
   - skip padding up to `data_offset`
   - read `count1` `u32` values
   - skip trailing padding up to `record_bytes`
4. for a table record:
   - read `count0` `u32` bit ids
   - skip padding up to `data_offset`
   - read `count1` row words of the width implied by `subtype`
   - skip trailing padding up to `record_bytes`
5. for the end record:
   - validate counts if desired
   - stop

No global table directory is needed.

## Validation Rules

Readers may run in either strict or trusted mode.

Strict validation should at least check:

- header magic and version;
- known `tag` and `subtype`;
- `record_bytes` alignment and lower bounds;
- `data_offset` lower bounds and alignment;
- `count0 <= max arity for row kind` for table records;
- all row values fit inside `count0` low bits when materialized;
- end record exists.

Trusted readers may skip expensive semantic checks and rely on the writer contract.

## Why This Layout

This layout is intentionally close to the Rust codebase:

- one record corresponds to one `Table` or one origin-bit array;
- table schemas are stored inline as `Vec<u32>`;
- rows are stored as raw primitive integer words, not as encoded bitsets;
- there are no cross-record references, dictionaries, or schema pools;
- record sizes and payload offsets are known before payload emission.

That choice trades some space for simpler code paths and higher throughput.

## Recommended File Extension

Recommended extension for version 1 files:

- `.tables`

Example names:

- `tables.fixed_point.tables`
- `tables.with_origins.tables`
