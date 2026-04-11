use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::common::Table;

pub const TABLES_EXTENSION: &str = "tables";

const FILE_MAGIC: [u8; 8] = *b"TBLBIN1\0";
const FILE_HEADER_BYTES: u64 = 16;
const RECORD_HEADER_BYTES: u64 = 32;
const END_RECORD_BYTES: u64 = 48;

const TAG_ORIGIN_ARRAY: u8 = 0x01;
const TAG_TABLE: u8 = 0x02;
const TAG_END: u8 = 0xFF;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowWordKind {
    U8,
    U16,
    U32,
    U64,
    U128,
}

impl RowWordKind {
    fn subtype(self) -> u8 {
        match self {
            Self::U8 => 0,
            Self::U16 => 1,
            Self::U32 => 2,
            Self::U64 => 3,
            Self::U128 => 4,
        }
    }

    fn from_subtype(subtype: u8) -> Result<Self> {
        match subtype {
            0 => Ok(Self::U8),
            1 => Ok(Self::U16),
            2 => Ok(Self::U32),
            3 => Ok(Self::U64),
            4 => Ok(Self::U128),
            _ => bail!("unsupported row subtype {subtype}"),
        }
    }

    pub fn byte_width(self) -> usize {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
            Self::U128 => 16,
        }
    }

    pub fn max_arity(self) -> usize {
        self.byte_width() * 8
    }
}

pub trait RowWord: Copy + Ord + Into<u128> + 'static {
    const KIND: RowWordKind;
}

macro_rules! impl_row_word {
    ($ty:ty, $kind:ident) => {
        impl RowWord for $ty {
            const KIND: RowWordKind = RowWordKind::$kind;
        }
    };
}

impl_row_word!(u8, U8);
impl_row_word!(u16, U16);
impl_row_word!(u32, U32);
impl_row_word!(u64, U64);
impl_row_word!(u128, U128);

pub trait PrimitiveIo: RowWord {
    fn write_slice_le<W: Write>(writer: &mut W, values: &[Self]) -> Result<()>;
    fn read_vec_le<R: Read>(reader: &mut R, len: usize) -> Result<Vec<Self>>;
}

macro_rules! impl_primitive_io {
    ($ty:ty) => {
        impl PrimitiveIo for $ty {
            fn write_slice_le<W: Write>(writer: &mut W, values: &[Self]) -> Result<()> {
                #[cfg(target_endian = "little")]
                {
                    let byte_len = values
                        .len()
                        .checked_mul(size_of::<Self>())
                        .context("row byte length overflow while writing")?;
                    let bytes = unsafe {
                        // SAFETY: integer slices are contiguous and plain-old-data.
                        std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                    };
                    writer.write_all(bytes).context("failed to write row payload")?;
                }

                #[cfg(not(target_endian = "little"))]
                {
                    for value in values {
                        writer
                            .write_all(&value.to_le_bytes())
                            .context("failed to write row payload")?;
                    }
                }

                Ok(())
            }

            fn read_vec_le<R: Read>(reader: &mut R, len: usize) -> Result<Vec<Self>> {
                #[cfg(target_endian = "little")]
                {
                    let mut values = vec![0 as Self; len];
                    let byte_len = len
                        .checked_mul(size_of::<Self>())
                        .context("row byte length overflow while reading")?;
                    let bytes = unsafe {
                        // SAFETY: integer slices are contiguous and plain-old-data.
                        std::slice::from_raw_parts_mut(
                            values.as_mut_ptr() as *mut u8,
                            byte_len,
                        )
                    };
                    reader.read_exact(bytes).context("failed to read row payload")?;
                    Ok(values)
                }

                #[cfg(not(target_endian = "little"))]
                {
                    let mut values = Vec::with_capacity(len);
                    for _ in 0..len {
                        let mut bytes = [0u8; size_of::<Self>()];
                        reader
                            .read_exact(&mut bytes)
                            .context("failed to read row payload")?;
                        values.push(<$ty>::from_le_bytes(bytes));
                    }
                    Ok(values)
                }
            }
        }
    };
}

impl_primitive_io!(u8);
impl_primitive_io!(u16);
impl_primitive_io!(u32);
impl_primitive_io!(u64);
impl_primitive_io!(u128);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginArray {
    pub name: String,
    pub values: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowWords {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    U128(Vec<u128>),
}

impl RowWords {
    pub fn kind(&self) -> RowWordKind {
        match self {
            Self::U8(_) => RowWordKind::U8,
            Self::U16(_) => RowWordKind::U16,
            Self::U32(_) => RowWordKind::U32,
            Self::U64(_) => RowWordKind::U64,
            Self::U128(_) => RowWordKind::U128,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::U8(rows) => rows.len(),
            Self::U16(rows) => rows.len(),
            Self::U32(rows) => rows.len(),
            Self::U64(rows) => rows.len(),
            Self::U128(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_into_u32_rows(self) -> Result<Vec<u32>> {
        match self {
            Self::U8(rows) => Ok(rows.into_iter().map(u32::from).collect()),
            Self::U16(rows) => Ok(rows.into_iter().map(u32::from).collect()),
            Self::U32(rows) => Ok(rows),
            Self::U64(rows) => rows
                .into_iter()
                .map(|row| {
                    u32::try_from(row).with_context(|| format!("row {row} does not fit in u32"))
                })
                .collect(),
            Self::U128(rows) => rows
                .into_iter()
                .map(|row| {
                    u32::try_from(row).with_context(|| format!("row {row} does not fit in u32"))
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTable {
    pub bits: Vec<u32>,
    pub rows: RowWords,
}

impl StoredTable {
    pub fn row_kind(&self) -> RowWordKind {
        self.rows.kind()
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn from_table(table: &Table) -> Self {
        Self {
            bits: table.bits.clone(),
            rows: RowWords::U32(table.rows.clone()),
        }
    }

    pub fn try_into_table(self) -> Result<Table> {
        if self.bits.len() > 32 {
            bail!(
                "table arity {} does not fit the library Table/u32 representation",
                self.bits.len()
            );
        }
        Ok(Table {
            bits: self.bits,
            rows: self.rows.try_into_u32_rows()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablesBundle {
    pub origin_arrays: Vec<OriginArray>,
    pub tables: Vec<StoredTable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndRecord {
    pub origin_record_count: u32,
    pub table_record_count: u64,
    pub total_origin_values: u64,
    pub total_table_rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TablesRecord {
    OriginArray(OriginArray),
    Table(StoredTable),
    End(EndRecord),
}

#[derive(Clone, Copy, Debug)]
struct RecordHeader {
    tag: u8,
    subtype: u8,
    flags: u16,
    count0: u32,
    count1: u64,
    data_offset: u64,
    record_bytes: u64,
}

impl RecordHeader {
    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; RECORD_HEADER_BYTES as usize];
        reader
            .read_exact(&mut bytes)
            .context("failed to read record header")?;
        Ok(Self {
            tag: bytes[0],
            subtype: bytes[1],
            flags: u16::from_le_bytes([bytes[2], bytes[3]]),
            count0: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            count1: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
            data_offset: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
            record_bytes: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
        })
    }

    fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut bytes = [0u8; RECORD_HEADER_BYTES as usize];
        bytes[0] = self.tag;
        bytes[1] = self.subtype;
        bytes[2..4].copy_from_slice(&self.flags.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.count0.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.count1.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.data_offset.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.record_bytes.to_le_bytes());
        writer
            .write_all(&bytes)
            .context("failed to write record header")?;
        Ok(())
    }
}

pub fn has_tables_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case(TABLES_EXTENSION))
}

pub struct TablesWriter<W> {
    writer: W,
    origin_record_count: u32,
    table_record_count: u64,
    total_origin_values: u64,
    total_table_rows: u64,
    finished: bool,
}

impl<W: Write> TablesWriter<W> {
    pub fn new(mut writer: W) -> Result<Self> {
        let mut header = [0u8; FILE_HEADER_BYTES as usize];
        header[..8].copy_from_slice(&FILE_MAGIC);
        header[8..10].copy_from_slice(&1u16.to_le_bytes());
        header[10..12].copy_from_slice(&0u16.to_le_bytes());
        header[12..16].copy_from_slice(&0u32.to_le_bytes());
        writer
            .write_all(&header)
            .context("failed to write .tables file header")?;
        Ok(Self {
            writer,
            origin_record_count: 0,
            table_record_count: 0,
            total_origin_values: 0,
            total_table_rows: 0,
            finished: false,
        })
    }

    pub fn write_origin_array(&mut self, name: &str, values: &[u32]) -> Result<()> {
        let name_bytes = name.as_bytes();
        let name_len = u32::try_from(name_bytes.len()).context("origin name too long")?;
        let value_count = u64::try_from(values.len()).context("too many origin values")?;
        let name_end = RECORD_HEADER_BYTES
            .checked_add(u64::from(name_len))
            .context("origin record size overflow")?;
        let data_offset = align_up(name_end, 4);
        let values_bytes = checked_payload_bytes(values.len(), 4)?;
        let record_bytes = align_up(
            data_offset
                .checked_add(values_bytes)
                .context("origin record size overflow")?,
            16,
        );
        let (sorted, unique) = analyze_u32_order(values);
        let mut flags = 0u16;
        if sorted {
            flags |= 1;
        }
        if unique {
            flags |= 1 << 1;
        }

        RecordHeader {
            tag: TAG_ORIGIN_ARRAY,
            subtype: 0,
            flags,
            count0: name_len,
            count1: value_count,
            data_offset,
            record_bytes,
        }
        .write(&mut self.writer)?;
        self.writer
            .write_all(name_bytes)
            .context("failed to write origin array name")?;
        write_zero_padding(&mut self.writer, data_offset - name_end)?;
        u32::write_slice_le(&mut self.writer, values)?;
        write_zero_padding(
            &mut self.writer,
            record_bytes
                .checked_sub(data_offset + values_bytes)
                .context("invalid origin record padding")?,
        )?;

        self.origin_record_count = self
            .origin_record_count
            .checked_add(1)
            .context("origin record count overflow")?;
        self.total_origin_values = self
            .total_origin_values
            .checked_add(value_count)
            .context("origin value count overflow")?;
        Ok(())
    }

    pub fn write_table_rows<T: PrimitiveIo>(&mut self, bits: &[u32], rows: &[T]) -> Result<()> {
        validate_row_width::<T>(bits, rows)?;
        let bit_count = u32::try_from(bits.len()).context("too many bits in table")?;
        let row_count = u64::try_from(rows.len()).context("too many rows in table")?;
        let row_bytes = u64::try_from(T::KIND.byte_width()).unwrap();
        let bits_bytes = checked_payload_bytes(bits.len(), 4)?;
        let bits_end = RECORD_HEADER_BYTES
            .checked_add(bits_bytes)
            .context("table record size overflow")?;
        let data_offset = align_up(bits_end, row_bytes);
        let rows_bytes = checked_payload_bytes(rows.len(), T::KIND.byte_width())?;
        let record_bytes = align_up(
            data_offset
                .checked_add(rows_bytes)
                .context("table record size overflow")?,
            16,
        );

        let mut flags = 0u16;
        if is_strictly_increasing(bits) {
            flags |= 1;
        }

        RecordHeader {
            tag: TAG_TABLE,
            subtype: T::KIND.subtype(),
            flags,
            count0: bit_count,
            count1: row_count,
            data_offset,
            record_bytes,
        }
        .write(&mut self.writer)?;
        u32::write_slice_le(&mut self.writer, bits)?;
        write_zero_padding(&mut self.writer, data_offset - bits_end)?;
        T::write_slice_le(&mut self.writer, rows)?;
        write_zero_padding(
            &mut self.writer,
            record_bytes
                .checked_sub(data_offset + rows_bytes)
                .context("invalid table record padding")?,
        )?;

        self.table_record_count = self
            .table_record_count
            .checked_add(1)
            .context("table record count overflow")?;
        self.total_table_rows = self
            .total_table_rows
            .checked_add(row_count)
            .context("total table row count overflow")?;
        Ok(())
    }

    pub fn write_stored_table(&mut self, table: &StoredTable) -> Result<()> {
        match &table.rows {
            RowWords::U8(rows) => self.write_table_rows(&table.bits, rows),
            RowWords::U16(rows) => self.write_table_rows(&table.bits, rows),
            RowWords::U32(rows) => self.write_table_rows(&table.bits, rows),
            RowWords::U64(rows) => self.write_table_rows(&table.bits, rows),
            RowWords::U128(rows) => self.write_table_rows(&table.bits, rows),
        }
    }

    pub fn finish(mut self) -> Result<W> {
        if !self.finished {
            RecordHeader {
                tag: TAG_END,
                subtype: 0,
                flags: 0,
                count0: self.origin_record_count,
                count1: self.table_record_count,
                data_offset: RECORD_HEADER_BYTES,
                record_bytes: END_RECORD_BYTES,
            }
            .write(&mut self.writer)?;
            self.writer
                .write_all(&self.total_origin_values.to_le_bytes())
                .context("failed to write end record origin count")?;
            self.writer
                .write_all(&self.total_table_rows.to_le_bytes())
                .context("failed to write end record table row count")?;
            self.finished = true;
        }
        Ok(self.writer)
    }
}

pub struct TablesReader<R> {
    reader: R,
    done: bool,
}

impl<R: Read> TablesReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let mut header = [0u8; FILE_HEADER_BYTES as usize];
        reader
            .read_exact(&mut header)
            .context("failed to read .tables file header")?;
        if header[..8] != FILE_MAGIC {
            bail!("invalid .tables magic header");
        }
        let major = u16::from_le_bytes([header[8], header[9]]);
        let minor = u16::from_le_bytes([header[10], header[11]]);
        let flags = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        if major != 1 {
            bail!("unsupported .tables major version {major}");
        }
        if minor != 0 {
            bail!("unsupported .tables minor version {minor}");
        }
        if flags != 0 {
            bail!("unsupported .tables file flags {flags}");
        }
        Ok(Self {
            reader,
            done: false,
        })
    }

    pub fn next_record(&mut self) -> Result<Option<TablesRecord>> {
        if self.done {
            return Ok(None);
        }

        let header = RecordHeader::read(&mut self.reader)?;
        validate_record_header(&header)?;

        match header.tag {
            TAG_ORIGIN_ARRAY => self.read_origin_record(header).map(Some),
            TAG_TABLE => self.read_table_record(header).map(Some),
            TAG_END => {
                let end = self.read_end_record(header)?;
                self.done = true;
                Ok(Some(TablesRecord::End(end)))
            }
            other => bail!("unsupported .tables record tag {other:#04x}"),
        }
    }

    fn read_origin_record(&mut self, header: RecordHeader) -> Result<TablesRecord> {
        if header.subtype != 0 {
            bail!("unsupported origin-array subtype {}", header.subtype);
        }
        let name_len = usize::try_from(header.count0).context("origin name length too large")?;
        let value_count =
            usize::try_from(header.count1).context("origin array length too large")?;
        let mut name_bytes = vec![0u8; name_len];
        self.reader
            .read_exact(&mut name_bytes)
            .context("failed to read origin array name")?;
        let name = String::from_utf8(name_bytes).context("origin array name is not valid UTF-8")?;
        let values_start = RECORD_HEADER_BYTES
            .checked_add(u64::from(header.count0))
            .context("origin name byte range overflow")?;
        skip_bytes(&mut self.reader, header.data_offset - values_start)?;
        let values = u32::read_vec_le(&mut self.reader, value_count)?;
        let values_bytes = checked_payload_bytes(value_count, 4)?;
        let consumed = header
            .data_offset
            .checked_add(values_bytes)
            .context("origin record size overflow")?;
        skip_bytes(&mut self.reader, header.record_bytes - consumed)?;
        Ok(TablesRecord::OriginArray(OriginArray { name, values }))
    }

    fn read_table_record(&mut self, header: RecordHeader) -> Result<TablesRecord> {
        let row_kind = RowWordKind::from_subtype(header.subtype)?;
        let bit_count = usize::try_from(header.count0).context("table bit count too large")?;
        let row_count = usize::try_from(header.count1).context("table row count too large")?;
        if bit_count > row_kind.max_arity() {
            bail!(
                "table arity {bit_count} exceeds row width capacity {}",
                row_kind.max_arity()
            );
        }
        let bits = u32::read_vec_le(&mut self.reader, bit_count)?;
        let bits_bytes = checked_payload_bytes(bit_count, 4)?;
        let bits_end = RECORD_HEADER_BYTES
            .checked_add(bits_bytes)
            .context("table bit payload size overflow")?;
        skip_bytes(&mut self.reader, header.data_offset - bits_end)?;
        let rows = match row_kind {
            RowWordKind::U8 => RowWords::U8(u8::read_vec_le(&mut self.reader, row_count)?),
            RowWordKind::U16 => RowWords::U16(u16::read_vec_le(&mut self.reader, row_count)?),
            RowWordKind::U32 => RowWords::U32(u32::read_vec_le(&mut self.reader, row_count)?),
            RowWordKind::U64 => RowWords::U64(u64::read_vec_le(&mut self.reader, row_count)?),
            RowWordKind::U128 => {
                RowWords::U128(u128::read_vec_le(&mut self.reader, row_count)?)
            }
        };
        let rows_bytes = checked_payload_bytes(row_count, row_kind.byte_width())?;
        let consumed = header
            .data_offset
            .checked_add(rows_bytes)
            .context("table record size overflow")?;
        skip_bytes(&mut self.reader, header.record_bytes - consumed)?;
        Ok(TablesRecord::Table(StoredTable { bits, rows }))
    }

    fn read_end_record(&mut self, header: RecordHeader) -> Result<EndRecord> {
        if header.subtype != 0 {
            bail!("unsupported end-record subtype {}", header.subtype);
        }
        if header.flags != 0 {
            bail!("unsupported end-record flags {}", header.flags);
        }
        if header.data_offset != RECORD_HEADER_BYTES {
            bail!("invalid end-record data offset {}", header.data_offset);
        }
        if header.record_bytes != END_RECORD_BYTES {
            bail!("invalid end-record size {}", header.record_bytes);
        }

        let mut bytes = [0u8; 16];
        self.reader
            .read_exact(&mut bytes)
            .context("failed to read end record payload")?;
        Ok(EndRecord {
            origin_record_count: header.count0,
            table_record_count: header.count1,
            total_origin_values: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            total_table_rows: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
        })
    }
}

pub fn read_tables_bundle(path: &Path) -> Result<TablesBundle> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = TablesReader::new(BufReader::new(file))?;
    let mut origin_arrays = Vec::new();
    let mut tables = Vec::new();
    let mut saw_end = false;

    while let Some(record) = reader.next_record()? {
        match record {
            TablesRecord::OriginArray(origin_array) => origin_arrays.push(origin_array),
            TablesRecord::Table(table) => tables.push(table),
            TablesRecord::End(_) => {
                saw_end = true;
                break;
            }
        }
    }

    if !saw_end {
        bail!("missing end record in {}", path.display());
    }

    Ok(TablesBundle {
        origin_arrays,
        tables,
    })
}

pub fn write_tables_bundle(path: &Path, bundle: &TablesBundle) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = TablesWriter::new(BufWriter::new(file))?;
    for origin_array in &bundle.origin_arrays {
        writer.write_origin_array(&origin_array.name, &origin_array.values)?;
    }
    for table in &bundle.tables {
        writer.write_stored_table(table)?;
    }
    let mut writer = writer.finish()?;
    writer.flush().context("failed to flush .tables output")?;
    Ok(())
}

pub fn read_tables_from_tables_file(path: &Path) -> Result<Vec<Table>> {
    let bundle = read_tables_bundle(path)?;
    bundle
        .tables
        .into_iter()
        .map(StoredTable::try_into_table)
        .collect()
}

pub fn write_tables_to_tables_file(path: &Path, tables: &[Table]) -> Result<()> {
    let bundle = TablesBundle {
        origin_arrays: Vec::new(),
        tables: tables.iter().map(StoredTable::from_table).collect(),
    };
    write_tables_bundle(path, &bundle)
}

fn validate_record_header(header: &RecordHeader) -> Result<()> {
    if header.record_bytes < RECORD_HEADER_BYTES {
        bail!("record size {} is too small", header.record_bytes);
    }
    if header.record_bytes % 16 != 0 {
        bail!("record size {} is not 16-byte aligned", header.record_bytes);
    }
    if header.data_offset < RECORD_HEADER_BYTES {
        bail!("record data offset {} is too small", header.data_offset);
    }
    if header.data_offset > header.record_bytes {
        bail!(
            "record data offset {} exceeds record size {}",
            header.data_offset,
            header.record_bytes
        );
    }
    Ok(())
}

fn align_up(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    (value + (alignment - 1)) & !(alignment - 1)
}

fn checked_payload_bytes(count: usize, width: usize) -> Result<u64> {
    let count = u64::try_from(count).context("payload count too large")?;
    let width = u64::try_from(width).unwrap();
    count
        .checked_mul(width)
        .context("payload byte length overflow")
}

fn write_zero_padding<W: Write>(writer: &mut W, bytes: u64) -> Result<()> {
    const ZEROES: [u8; 32] = [0; 32];
    let mut remaining = bytes;
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(ZEROES.len() as u64)).unwrap();
        writer
            .write_all(&ZEROES[..chunk])
            .context("failed to write padding")?;
        remaining -= chunk as u64;
    }
    Ok(())
}

fn skip_bytes<R: Read>(reader: &mut R, bytes: u64) -> Result<()> {
    const CHUNK: usize = 8192;
    let mut scratch = [0u8; CHUNK];
    let mut remaining = bytes;
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(CHUNK as u64)).unwrap();
        reader
            .read_exact(&mut scratch[..chunk])
            .context("failed to skip padding")?;
        remaining -= chunk as u64;
    }
    Ok(())
}

fn analyze_u32_order(values: &[u32]) -> (bool, bool) {
    if values.len() < 2 {
        return (true, true);
    }

    let mut sorted = true;
    let mut unique = true;
    for window in values.windows(2) {
        if window[0] > window[1] {
            sorted = false;
            unique = false;
            break;
        }
        if window[0] == window[1] {
            unique = false;
        }
    }
    (sorted, unique)
}

fn is_strictly_increasing(values: &[u32]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

fn validate_row_width<T: RowWord>(bits: &[u32], rows: &[T]) -> Result<()> {
    if bits.len() > T::KIND.max_arity() {
        bail!(
            "table arity {} exceeds row width capacity {}",
            bits.len(),
            T::KIND.max_arity()
        );
    }

    let max_allowed = if bits.len() == T::KIND.max_arity() {
        u128::MAX
    } else {
        (1u128 << bits.len()) - 1
    };

    for &row in rows {
        let value: u128 = row.into();
        if value > max_allowed {
            bail!(
                "row value {} exceeds the low {} bits required by the schema",
                value,
                bits.len()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_roundtrip_preserves_origins_and_tables() {
        let bundle = TablesBundle {
            origin_arrays: vec![OriginArray {
                name: "origins".to_string(),
                values: vec![3, 7, 11],
            }],
            tables: vec![
                StoredTable {
                    bits: vec![10, 20, 30],
                    rows: RowWords::U32(vec![0, 3, 5]),
                },
                StoredTable {
                    bits: vec![1, 2, 4, 8],
                    rows: RowWords::U8(vec![0, 7, 15]),
                },
            ],
        };

        let path = std::env::temp_dir()
            .join(format!("tables-file-roundtrip-{}.tables", std::process::id()));
        write_tables_bundle(&path, &bundle).unwrap();
        let restored = read_tables_bundle(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(restored, bundle);
    }

    #[test]
    fn common_table_roundtrip_uses_u32_rows() {
        let tables = vec![Table {
            bits: vec![1, 5, 9],
            rows: vec![0, 3, 7],
        }];

        let path = std::env::temp_dir().join(format!(
            "tables-file-common-roundtrip-{}.tables",
            std::process::id()
        ));
        write_tables_to_tables_file(&path, &tables).unwrap();
        let restored = read_tables_from_tables_file(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(restored, tables);
    }
}
