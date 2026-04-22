use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::write_json;
use tables::subset_absorption::canonicalize_table;
use tables::tables_file::{read_tables_bundle, write_tables_bundle, StoredTable, TablesBundle};

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut iter = env::args().skip(1);
        let mut input = None;
        let mut output = None;
        let mut report = None;

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--output" => output = Some(PathBuf::from(expect_value(&mut iter, "--output")?)),
                "--report" => report = Some(PathBuf::from(expect_value(&mut iter, "--report")?)),
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
            report: report.ok_or_else(|| anyhow!("--report is required"))?,
        })
    }
}

#[derive(Serialize)]
struct Report {
    input: String,
    output: String,
    input_table_count: usize,
    output_table_count: usize,
    duplicate_table_count: usize,
    duplicate_group_count: usize,
    max_group_size: usize,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let input_bundle = read_tables_bundle(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;

    let mut grouped: BTreeMap<(Vec<u32>, Vec<u32>), usize> = BTreeMap::new();
    let mut output_tables = Vec::new();

    for stored in input_bundle.tables {
        let canonical = StoredTable::try_into_table(stored)?;
        let (bits, rows) = canonicalize_table(&canonical);
        let key = (bits.clone(), rows.clone());
        match grouped.get_mut(&key) {
            Some(count) => *count += 1,
            None => {
                grouped.insert(key, 1);
                output_tables.push(StoredTable::from_table(&tables::common::Table {
                    bits,
                    rows,
                }));
            }
        }
    }

    let duplicate_group_count = grouped.values().filter(|&&count| count > 1).count();
    let duplicate_table_count: usize = grouped
        .values()
        .filter(|&&count| count > 1)
        .map(|count| count - 1)
        .sum();
    let max_group_size = grouped.values().copied().max().unwrap_or(0);

    let output_bundle = TablesBundle {
        origin_arrays: input_bundle.origin_arrays,
        tables: output_tables,
    };
    write_tables_bundle(&args.output, &output_bundle)
        .with_context(|| format!("failed to write {}", args.output.display()))?;

    let report = Report {
        input: args.input.display().to_string(),
        output: args.output.display().to_string(),
        input_table_count: grouped.values().sum(),
        output_table_count: output_bundle.tables.len(),
        duplicate_table_count,
        duplicate_group_count,
        max_group_size,
    };
    write_json(&args.report, &report)?;

    println!("input_tables: {}", report.input_table_count);
    println!("output_tables: {}", report.output_table_count);
    println!("duplicate_tables: {}", report.duplicate_table_count);
    println!("duplicate_groups: {}", report.duplicate_group_count);
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());
    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin dedup_exact_tables_bundle -- --input <input.tables> --output <output.tables> --report <report.json>"
    );
}
