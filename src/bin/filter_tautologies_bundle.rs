use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{is_full_row_set, write_json};
use tables::tables_file::{read_tables_bundle, write_tables_bundle, TablesBundle};

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/tables.with_any_origin.projected_to_origins.tables",
            ),
            output: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/tables.with_any_origin.projected_to_origins.non_taut.tables",
            ),
            report: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/tables.with_any_origin.projected_to_origins.non_taut.report.json",
            ),
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
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
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
    input: String,
    output: String,
    input_table_count: usize,
    input_row_count: usize,
    kept_table_count: usize,
    kept_row_count: usize,
    removed_tautology_count: usize,
    removed_tautology_rows: usize,
    removed_by_arity: BTreeMap<usize, usize>,
    kept_by_arity: BTreeMap<usize, usize>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let input_bundle = read_tables_bundle(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;

    let input_table_count = input_bundle.tables.len();
    let input_row_count: usize = input_bundle
        .tables
        .iter()
        .map(|table| table.row_count())
        .sum();

    let mut kept_tables = Vec::new();
    let mut kept_row_count = 0usize;
    let mut removed_tautology_count = 0usize;
    let mut removed_tautology_rows = 0usize;
    let mut removed_by_arity: BTreeMap<usize, usize> = BTreeMap::new();
    let mut kept_by_arity: BTreeMap<usize, usize> = BTreeMap::new();

    for table in input_bundle.tables {
        if is_full_row_set(table.row_count(), table.bits.len()) {
            removed_tautology_count += 1;
            removed_tautology_rows += table.row_count();
            *removed_by_arity.entry(table.bits.len()).or_insert(0) += 1;
            continue;
        }

        kept_row_count += table.row_count();
        *kept_by_arity.entry(table.bits.len()).or_insert(0) += 1;
        kept_tables.push(table);
    }

    let output_bundle = TablesBundle {
        origin_arrays: input_bundle.origin_arrays,
        tables: kept_tables,
    };
    write_tables_bundle(&args.output, &output_bundle)
        .with_context(|| format!("failed to write {}", args.output.display()))?;

    let report = Report {
        input: args.input.display().to_string(),
        output: args.output.display().to_string(),
        input_table_count,
        input_row_count,
        kept_table_count: output_bundle.tables.len(),
        kept_row_count,
        removed_tautology_count,
        removed_tautology_rows,
        removed_by_arity,
        kept_by_arity,
    };
    write_json(&args.report, &report)?;

    println!("input_tables: {}", report.input_table_count);
    println!("kept_tables: {}", report.kept_table_count);
    println!("removed_tautologies: {}", report.removed_tautology_count);
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
        "usage:\n  cargo run --release --bin filter_tautologies_bundle -- [--input <input.tables>] [--output <output.tables>] [--report <report.json>]"
    );
}
