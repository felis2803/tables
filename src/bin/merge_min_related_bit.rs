use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{collect_bits, read_tables, total_rows, write_json, Table};
use tables::rank_stats::compute_rank;
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    incident_output: PathBuf,
    merged_output: PathBuf,
    report_output: PathBuf,
    exclude_self: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            incident_output: PathBuf::from(
                "runs/2026-04-04-min-related-bit-merge/incident_tables.json",
            ),
            merged_output: PathBuf::from(
                "runs/2026-04-04-min-related-bit-merge/merged_table.json",
            ),
            report_output: PathBuf::from(
                "runs/2026-04-04-min-related-bit-merge/report.json",
            ),
            exclude_self: false,
        }
    }
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self::default();
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => args.input = PathBuf::from(expect_value(&mut iter, "--input")?),
                "--incident-output" => {
                    args.incident_output =
                        PathBuf::from(expect_value(&mut iter, "--incident-output")?)
                }
                "--merged-output" => {
                    args.merged_output = PathBuf::from(expect_value(&mut iter, "--merged-output")?)
                }
                "--report" => {
                    args.report_output = PathBuf::from(expect_value(&mut iter, "--report")?)
                }
                "--exclude-self" => args.exclude_self = true,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(args)
    }
}

#[derive(Serialize)]
struct Report {
    method: String,
    input: String,
    exclude_self: bool,
    selected_bit: u32,
    related_count: usize,
    related_bits: Vec<u32>,
    incident_table_ids: Vec<usize>,
    incident_table_count: usize,
    incident_bit_count: usize,
    incident_row_count: usize,
    merged_bit_count: usize,
    merged_row_count: usize,
    merged_rank: f64,
    output_incident_tables: String,
    output_merged_table: String,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let source_tables = read_tables(&args.input)?;
    let tables: Vec<Table> = source_tables
        .into_iter()
        .map(|table| {
            let (bits, rows) = canonicalize_table(&table);
            Table { bits, rows }
        })
        .collect();

    let related_bits_map = build_related_bits_map(&tables, args.exclude_self);
    let (selected_bit, related_bits) = select_min_related_bit(&related_bits_map)?;
    let incident_table_ids: Vec<usize> = tables
        .iter()
        .enumerate()
        .filter_map(|(table_id, table)| table.bits.binary_search(&selected_bit).ok().map(|_| table_id))
        .collect();
    let incident_tables: Vec<Table> = incident_table_ids
        .iter()
        .map(|&table_id| tables[table_id].clone())
        .collect();

    let merged = merge_all_tables(&incident_tables, selected_bit)?;
    let report = Report {
        method: "Select the bit with the minimum related-bit count under the current related_bits definition, collect all incident tables containing that bit, and merge them exactly into one table".to_string(),
        input: args.input.display().to_string(),
        exclude_self: args.exclude_self,
        selected_bit,
        related_count: related_bits.len(),
        related_bits,
        incident_table_ids,
        incident_table_count: incident_tables.len(),
        incident_bit_count: collect_bits(&incident_tables).len(),
        incident_row_count: total_rows(&incident_tables),
        merged_bit_count: merged.bits.len(),
        merged_row_count: merged.rows.len(),
        merged_rank: compute_rank(merged.rows.len(), merged.bits.len()),
        output_incident_tables: args.incident_output.display().to_string(),
        output_merged_table: args.merged_output.display().to_string(),
    };

    write_json(&args.incident_output, &incident_tables)?;
    write_json(&args.merged_output, &vec![merged])?;
    write_json(&args.report_output, &report)?;

    println!("selected_bit={}", report.selected_bit);
    println!("related_count={}", report.related_count);
    println!("incident_tables={}", report.incident_table_count);
    println!("incident_bits={}", report.incident_bit_count);
    println!("incident_rows={}", report.incident_row_count);
    println!("merged_bits={}", report.merged_bit_count);
    println!("merged_rows={}", report.merged_row_count);
    println!("merged_rank={:.12}", report.merged_rank);
    println!("incident_output={}", args.incident_output.display());
    println!("merged_output={}", args.merged_output.display());
    println!("report={}", args.report_output.display());

    Ok(())
}

fn build_related_bits_map(
    tables: &[Table],
    exclude_self: bool,
) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut related_bits: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

    for table in tables {
        let bits: BTreeSet<u32> = table.bits.iter().copied().collect();
        for &bit in &table.bits {
            let entry = related_bits.entry(bit).or_default();
            entry.extend(bits.iter().copied());
            if exclude_self {
                entry.remove(&bit);
            }
        }
    }

    related_bits
}

fn select_min_related_bit(related_bits_map: &BTreeMap<u32, BTreeSet<u32>>) -> Result<(u32, Vec<u32>)> {
    related_bits_map
        .iter()
        .min_by(|(left_bit, left_related), (right_bit, right_related)| {
            left_related
                .len()
                .cmp(&right_related.len())
                .then_with(|| left_bit.cmp(right_bit))
        })
        .map(|(&bit, related_bits)| (bit, related_bits.iter().copied().collect()))
        .context("related_bits_map is empty")
}

fn merge_all_tables(tables: &[Table], selected_bit: u32) -> Result<Table> {
    let Some(first) = tables.first() else {
        bail!("no incident tables found for selected bit {selected_bit}");
    };

    let mut merged = first.clone();
    for table in &tables[1..] {
        let next =
            merge_tables_fast_from_slices(&merged.bits, &merged.rows, &table.bits, &table.rows)
                .map_err(|error| {
                    anyhow!(
                        "failed to merge incident tables for selected bit {selected_bit}: {error}"
                    )
                })?;
        merged = Table {
            bits: next.bits,
            rows: next.rows,
        };
    }

    Ok(merged)
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --bin merge_min_related_bit -- --input <path> --incident-output <path> --merged-output <path> --report <path> [--exclude-self]"
    );
}
