use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::bit_zero_collapse::{build_table_bit_zero_collapse_report, BitZeroCollapseMetric};
use tables::common::{read_tables, write_json};

struct Args {
    input: PathBuf,
    table_index: usize,
    output: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            table_index: usize::MAX,
            output: PathBuf::from("data/reports/report.bit_zero_collapse.json"),
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
                "--table-index" => {
                    args.table_index = expect_value(&mut iter, "--table-index")?
                        .parse()
                        .with_context(|| "invalid value for --table-index")?;
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        if args.table_index == usize::MAX {
            bail!("missing required argument: --table-index");
        }

        Ok(args)
    }
}

#[derive(Clone, Debug, Serialize)]
struct BitZeroCollapseCliReport {
    method: String,
    metric_name: String,
    metric_definition: String,
    input: String,
    table_index: usize,
    table_bits: Vec<u32>,
    row_count_before: usize,
    bit_count: usize,
    max_zero_collapse: f64,
    mean_zero_collapse: f64,
    metrics: Vec<BitZeroCollapseMetric>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)?;
    let table = tables
        .get(args.table_index)
        .ok_or_else(|| anyhow!("table index {} is out of range", args.table_index))?;

    let mut report = build_table_bit_zero_collapse_report(table);
    report.metrics.sort_by(|left, right| {
        right
            .zero_collapse
            .total_cmp(&left.zero_collapse)
            .then_with(|| right.collapsed_rows.cmp(&left.collapsed_rows))
            .then_with(|| left.bit.cmp(&right.bit))
            .then_with(|| left.bit_index.cmp(&right.bit_index))
    });

    let cli_report = BitZeroCollapseCliReport {
        method:
            "For each bit in the selected table, zero that bit in every row, deduplicate rows, and measure the collapsed-row share"
                .to_string(),
        metric_name: report.metric_name,
        metric_definition: report.metric_definition,
        input: path_string(&args.input),
        table_index: args.table_index,
        table_bits: table.bits.clone(),
        row_count_before: report.row_count_before,
        bit_count: report.bit_count,
        max_zero_collapse: report.max_zero_collapse,
        mean_zero_collapse: report.mean_zero_collapse,
        metrics: report.metrics,
    };

    write_json(&args.output, &cli_report)?;

    println!("metric: {}", cli_report.metric_name);
    println!("table_index: {}", cli_report.table_index);
    println!("bits: {}", cli_report.bit_count);
    println!("rows_before: {}", cli_report.row_count_before);
    println!("max_zero_collapse: {:.12}", cli_report.max_zero_collapse);
    println!("mean_zero_collapse: {:.12}", cli_report.mean_zero_collapse);
    println!("output: {}", args.output.display());

    Ok(())
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin bit_zero_collapse -- --table-index <n> [--input <path>] [--output <path>]"
    );
}
