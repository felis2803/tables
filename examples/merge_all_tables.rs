use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::json;
use tables::common::{collect_bits, read_tables, total_rows, write_json, Table};
use tables::rank_stats::compute_rank;
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Debug)]
struct Args {
    input: PathBuf,
    output_table: PathBuf,
    output_report: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = None;
        let mut output_table = None;
        let mut output_report = None;
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--output-table" => {
                    output_table = Some(PathBuf::from(expect_value(&mut iter, "--output-table")?))
                }
                "--output-report" => {
                    output_report = Some(PathBuf::from(expect_value(&mut iter, "--output-report")?))
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            input: input.context("missing --input")?,
            output_table: output_table.context("missing --output-table")?,
            output_report: output_report.context("missing --output-report")?,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct MergeReport {
    input: String,
    input_table_count: usize,
    input_bit_count: usize,
    input_row_count: usize,
    merged_bit_count: usize,
    merged_row_count: usize,
    merged_rank: f64,
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

    let merged = merge_all_tables(&tables)?;
    let report = MergeReport {
        input: args.input.display().to_string(),
        input_table_count: tables.len(),
        input_bit_count: collect_bits(&tables).len(),
        input_row_count: total_rows(&tables),
        merged_bit_count: merged.bits.len(),
        merged_row_count: merged.rows.len(),
        merged_rank: compute_rank(merged.rows.len(), merged.bits.len()),
    };

    write_json(&args.output_table, &json!([merged]))?;
    write_json(&args.output_report, &report)?;

    println!("input_tables={}", report.input_table_count);
    println!("input_bits={}", report.input_bit_count);
    println!("input_rows={}", report.input_row_count);
    println!("merged_bits={}", report.merged_bit_count);
    println!("merged_rows={}", report.merged_row_count);
    println!("merged_rank={:.12}", report.merged_rank);
    println!("output_table={}", args.output_table.display());
    println!("output_report={}", args.output_report.display());
    Ok(())
}

fn merge_all_tables(tables: &[Table]) -> Result<Table> {
    let Some(first) = tables.first() else {
        bail!("input table set is empty");
    };

    let mut merged = first.clone();
    for table in &tables[1..] {
        let next =
            merge_tables_fast_from_slices(&merged.bits, &merged.rows, &table.bits, &table.rows)
                .map_err(|error| {
                    anyhow!(
                        "failed to merge {:?} with {:?}: {error}",
                        merged.bits,
                        table.bits
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
        "usage: cargo run --release --example merge_all_tables -- --input <tables.json> --output-table <table.json> --output-report <report.json>"
    );
}
