use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use tables::tables_file::{write_tables_bundle, RowWords, StoredTable, TablesBundle};

#[derive(Clone, Debug, Deserialize)]
struct WideJsonTable {
    bits: Vec<u32>,
    rows: Vec<u64>,
}

struct Args {
    input: PathBuf,
    output: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = None;
        let mut output = None;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--output" => output = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            input: input.ok_or_else(|| anyhow!("--input is required"))?,
            output: output.ok_or_else(|| anyhow!("--output is required"))?,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_wide_json_tables(&args.input)?;
    let stored_tables = tables
        .into_iter()
        .map(try_into_stored_table)
        .collect::<Result<Vec<_>>>()?;

    let table_count = stored_tables.len();
    let total_rows: usize = stored_tables.iter().map(StoredTable::row_count).sum();
    let max_arity = stored_tables
        .iter()
        .map(|table| table.bits.len())
        .max()
        .unwrap_or(0);

    let bundle = TablesBundle {
        origin_arrays: Vec::new(),
        tables: stored_tables,
    };
    write_tables_bundle(&args.output, &bundle)?;

    println!("input: {}", args.input.display());
    println!("output: {}", args.output.display());
    println!("tables: {table_count}");
    println!("total_rows: {total_rows}");
    println!("max_arity: {max_arity}");
    for (index, table) in bundle.tables.iter().enumerate() {
        println!(
            "table[{index}]: bits={} rows={} row_kind={:?}",
            table.bits.len(),
            table.row_count(),
            table.row_kind()
        );
    }
    Ok(())
}

fn read_wide_json_tables(path: &Path) -> Result<Vec<WideJsonTable>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).with_context(|| format!("failed to parse {}", path.display()))
}

fn try_into_stored_table(table: WideJsonTable) -> Result<StoredTable> {
    validate_bits(&table.bits)?;
    validate_rows_fit_bits(&table.rows, table.bits.len())?;
    let row_word_kind = choose_row_word_kind(&table.rows, table.bits.len())?;
    let rows = match row_word_kind {
        1 => RowWords::U8(
            table
                .rows
                .into_iter()
                .map(|row| {
                    u8::try_from(row).with_context(|| format!("row {row} does not fit in u8"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        2 => RowWords::U16(
            table
                .rows
                .into_iter()
                .map(|row| {
                    u16::try_from(row).with_context(|| format!("row {row} does not fit in u16"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        4 => RowWords::U32(
            table
                .rows
                .into_iter()
                .map(|row| {
                    u32::try_from(row).with_context(|| format!("row {row} does not fit in u32"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        8 => RowWords::U64(table.rows),
        16 => RowWords::U128(table.rows.into_iter().map(u128::from).collect()),
        other => bail!("unsupported row word byte width {other}"),
    };
    Ok(StoredTable {
        bits: table.bits,
        rows,
    })
}

fn validate_bits(bits: &[u32]) -> Result<()> {
    if bits.windows(2).any(|window| window[0] >= window[1]) {
        bail!("table bits must be strictly increasing for .tables output");
    }
    Ok(())
}

fn validate_rows_fit_bits(rows: &[u64], bit_count: usize) -> Result<()> {
    if bit_count >= 64 {
        return Ok(());
    }

    let limit = 1u64 << bit_count;
    if let Some(&row) = rows.iter().find(|&&row| row >= limit) {
        bail!(
            "row {row} exceeds the declared arity {bit_count}; expected all rows to be below {limit}"
        );
    }
    Ok(())
}

fn choose_row_word_kind(rows: &[u64], bit_count: usize) -> Result<usize> {
    if bit_count > 128 {
        bail!("table arity {bit_count} exceeds supported .tables width 128");
    }
    let max_row = rows.iter().copied().max().unwrap_or(0);
    let needed_bits = if max_row == 0 {
        1usize
    } else {
        64usize - max_row.leading_zeros() as usize
    };
    let required_bits = bit_count.max(needed_bits);
    Ok(match required_bits {
        0..=8 => 1,
        9..=16 => 2,
        17..=32 => 4,
        33..=64 => 8,
        _ => 16,
    })
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin wide_json_to_tables -- --input <path_table.json> --output <path_table.tables>"
    );
}

#[cfg(test)]
mod tests {
    use super::{choose_row_word_kind, validate_rows_fit_bits};

    #[test]
    fn rejects_row_that_needs_more_bits_than_declared_schema() {
        let error = validate_rows_fit_bits(&[0, 8], 3).unwrap_err().to_string();
        assert!(error.contains("exceeds the declared arity 3"));
    }

    #[test]
    fn accepts_rows_for_wide_schema() {
        validate_rows_fit_bits(&[0, u64::MAX], 64).unwrap();
        assert_eq!(choose_row_word_kind(&[0, u64::MAX], 64).unwrap(), 8);
    }
}
