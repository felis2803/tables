use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde::Serialize;
use tables::tables_file::{read_tables_bundle, RowWords, StoredTable};

struct Args {
    source: PathBuf,
    bits_json: PathBuf,
    projected: PathBuf,
    report: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut source = None;
        let mut bits_json = None;
        let mut projected = None;
        let mut report = None;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--source" => source = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--bits-json" => bits_json = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--projected" => projected = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--report" => report = Some(PathBuf::from(expect_value(&mut args, &arg)?)),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }
        Ok(Self {
            source: source.ok_or_else(|| anyhow!("--source is required"))?,
            bits_json: bits_json.ok_or_else(|| anyhow!("--bits-json is required"))?,
            projected: projected.ok_or_else(|| anyhow!("--projected is required"))?,
            report,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct BitsList(Vec<u32>);

#[derive(Debug, Serialize)]
struct VerificationReport {
    source: String,
    projected: String,
    selected_bits_requested: usize,
    selected_bits_present: Vec<u32>,
    source_row_count: usize,
    projected_row_count: usize,
    exact_projection_row_count: usize,
    max_possible_projection_rows: usize,
    extracted_matches_exact_projection: bool,
    exact_projection_is_tautology: bool,
    missing_rows_in_extracted: usize,
    extra_rows_in_extracted: usize,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let selected_bits = read_bits_json(&args.bits_json)?;
    let source_bundle = read_tables_bundle(&args.source)?;
    let projected_bundle = read_tables_bundle(&args.projected)?;
    if source_bundle.tables.len() != 1 {
        bail!(
            "expected exactly one source table, found {}",
            source_bundle.tables.len()
        );
    }
    if projected_bundle.tables.len() != 1 {
        bail!(
            "expected exactly one projected table, found {}",
            projected_bundle.tables.len()
        );
    }

    let source_table = &source_bundle.tables[0];
    let projected_table = &projected_bundle.tables[0];
    let selected_set: HashSet<u32> = selected_bits.iter().copied().collect();
    let kept_positions: Vec<usize> = source_table
        .bits
        .iter()
        .enumerate()
        .filter_map(|(index, bit)| selected_set.contains(bit).then_some(index))
        .collect();
    let kept_bits: Vec<u32> = kept_positions
        .iter()
        .map(|&index| source_table.bits[index])
        .collect();

    if kept_bits.len() != projected_table.bits.len() || kept_bits != projected_table.bits {
        bail!(
            "projected table bits {:?} do not match source-selected bits {:?}",
            projected_table.bits,
            kept_bits
        );
    }
    if kept_bits.len() >= usize::BITS as usize {
        bail!(
            "projection arity {} too large for exact bitset verification",
            kept_bits.len()
        );
    }

    let exact_support = compute_support_bitset(source_table, &kept_positions)?;
    let extracted_support = rows_to_bitset(projected_table, kept_bits.len())?;

    let exact_projection_row_count = exact_support
        .iter()
        .map(|word| word.count_ones() as usize)
        .sum();
    let projected_row_count = extracted_support
        .iter()
        .map(|word| word.count_ones() as usize)
        .sum();
    let max_possible_projection_rows = 1usize << kept_bits.len();

    let mut missing_rows_in_extracted = 0usize;
    let mut extra_rows_in_extracted = 0usize;
    let mut extracted_matches = exact_support.len() == extracted_support.len();
    for (left, right) in exact_support.iter().zip(&extracted_support) {
        let missing = left & !right;
        let extra = right & !left;
        if missing != 0 || extra != 0 {
            extracted_matches = false;
            missing_rows_in_extracted += missing.count_ones() as usize;
            extra_rows_in_extracted += extra.count_ones() as usize;
        }
    }

    let report = VerificationReport {
        source: args.source.display().to_string(),
        projected: args.projected.display().to_string(),
        selected_bits_requested: selected_bits.len(),
        selected_bits_present: kept_bits,
        source_row_count: row_count(&source_table.rows),
        projected_row_count,
        exact_projection_row_count,
        max_possible_projection_rows,
        extracted_matches_exact_projection: extracted_matches,
        exact_projection_is_tautology: exact_projection_row_count == max_possible_projection_rows,
        missing_rows_in_extracted,
        extra_rows_in_extracted,
    };

    if let Some(report_path) = &args.report {
        let mut bytes = serde_json::to_vec_pretty(&report)
            .with_context(|| format!("failed to serialize {}", report_path.display()))?;
        bytes.push(b'\n');
        std::fs::write(report_path, bytes)
            .with_context(|| format!("failed to write {}", report_path.display()))?;
    }

    println!(
        "selected_bits_present: {}",
        report.selected_bits_present.len()
    );
    println!("source_row_count: {}", report.source_row_count);
    println!(
        "exact_projection_row_count: {}",
        report.exact_projection_row_count
    );
    println!("projected_row_count: {}", report.projected_row_count);
    println!(
        "max_possible_projection_rows: {}",
        report.max_possible_projection_rows
    );
    println!(
        "extracted_matches_exact_projection: {}",
        report.extracted_matches_exact_projection
    );
    println!(
        "exact_projection_is_tautology: {}",
        report.exact_projection_is_tautology
    );
    println!(
        "missing_rows_in_extracted: {}",
        report.missing_rows_in_extracted
    );
    println!(
        "extra_rows_in_extracted: {}",
        report.extra_rows_in_extracted
    );
    Ok(())
}

fn compute_support_bitset(table: &StoredTable, kept_positions: &[usize]) -> Result<Vec<u64>> {
    let width = kept_positions.len();
    let support_len = 1usize << width;
    let mut support = vec![0u64; support_len.div_ceil(64)];
    match &table.rows {
        RowWords::U8(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, project_row(u128::from(row), kept_positions));
            }
        }
        RowWords::U16(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, project_row(u128::from(row), kept_positions));
            }
        }
        RowWords::U32(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, project_row(u128::from(row), kept_positions));
            }
        }
        RowWords::U64(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, project_row(u128::from(row), kept_positions));
            }
        }
        RowWords::U128(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, project_row(row, kept_positions));
            }
        }
    }
    Ok(support)
}

fn rows_to_bitset(table: &StoredTable, width: usize) -> Result<Vec<u64>> {
    let support_len = 1usize << width;
    let mut support = vec![0u64; support_len.div_ceil(64)];
    match &table.rows {
        RowWords::U8(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, usize::from(row));
            }
        }
        RowWords::U16(rows) => {
            for &row in rows {
                set_projection_bit(&mut support, usize::from(row));
            }
        }
        RowWords::U32(rows) => {
            for &row in rows {
                set_projection_bit(
                    &mut support,
                    usize::try_from(row).context("u32 row does not fit usize")?,
                );
            }
        }
        RowWords::U64(rows) => {
            for &row in rows {
                set_projection_bit(
                    &mut support,
                    usize::try_from(row).context("u64 row does not fit usize")?,
                );
            }
        }
        RowWords::U128(rows) => {
            for &row in rows {
                set_projection_bit(
                    &mut support,
                    usize::try_from(row).context("u128 row does not fit usize")?,
                );
            }
        }
    }
    Ok(support)
}

fn set_projection_bit(bitset: &mut [u64], value: usize) {
    bitset[value / 64] |= 1u64 << (value % 64);
}

fn project_row(row: u128, kept_positions: &[usize]) -> usize {
    let mut projected = 0usize;
    for (new_index, &old_index) in kept_positions.iter().enumerate() {
        if ((row >> old_index) & 1) != 0 {
            projected |= 1usize << new_index;
        }
    }
    projected
}

fn row_count(rows: &RowWords) -> usize {
    rows.len()
}

fn read_bits_json(path: &Path) -> Result<Vec<u32>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut bits: Vec<u32> = serde_json::from_slice::<BitsList>(&bytes)
        .map(|bits| bits.0)
        .or_else(|_| serde_json::from_slice(&bytes))
        .with_context(|| format!("failed to parse {}", path.display()))?;
    bits.sort_unstable();
    bits.dedup();
    Ok(bits)
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin verify_tables_projection -- --source <source.tables> --bits-json <bits.json> --projected <projected.tables> [--report <report.json>]"
    );
}
