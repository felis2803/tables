use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use tables::bit_zero_collapse::{compute_bit_zero_collapse_metrics, BitZeroCollapseMetric};
use tables::common::{read_tables, total_rows, write_json};

struct Args {
    input: PathBuf,
    output: PathBuf,
    summary_only: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from("data/reports/report.bit_zero_collapse_all.json"),
            summary_only: false,
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
                "--summary-only" => args.summary_only = true,
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

#[derive(Clone, Debug, Serialize)]
struct TableBitZeroCollapseCliReport {
    table_index: usize,
    table_bits: Vec<u32>,
    row_count_before: usize,
    bit_count: usize,
    max_zero_collapse: f64,
    mean_zero_collapse: f64,
    metrics: Vec<BitZeroCollapseMetric>,
}

#[derive(Clone, Debug, Serialize)]
struct GlobalMaxMetric {
    table_index: usize,
    table_bits: Vec<u32>,
    bit: u32,
    bit_index: usize,
    row_count_before: usize,
    row_count_after_zeroing: usize,
    collapsed_rows: usize,
    zero_collapse: f64,
}

#[derive(Clone, Debug, Serialize)]
struct BitZeroCollapseAllReport {
    method: String,
    metric_name: String,
    metric_definition: String,
    input: String,
    summary_only: bool,
    table_count: usize,
    total_row_count: usize,
    total_bit_metrics: usize,
    total_zeroing_row_visits: usize,
    global_max_zero_collapse: f64,
    global_mean_zero_collapse: f64,
    global_max_metric: Option<GlobalMaxMetric>,
    read_seconds: f64,
    compute_seconds: f64,
    tables_per_second: f64,
    bit_metrics_per_second: f64,
    zeroing_row_visits_per_second: f64,
    tables: Option<Vec<TableBitZeroCollapseCliReport>>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    let read_started = Instant::now();
    let tables = read_tables(&args.input)?;
    let read_seconds = read_started.elapsed().as_secs_f64();

    let total_row_count = total_rows(&tables);
    let compute_started = Instant::now();

    let mut total_bit_metrics = 0usize;
    let mut total_zeroing_row_visits = 0usize;
    let mut global_sum_zero_collapse = 0.0f64;
    let mut global_max_zero_collapse = 0.0f64;
    let mut global_max_metric: Option<GlobalMaxMetric> = None;
    let mut table_reports = if args.summary_only {
        None
    } else {
        Some(Vec::with_capacity(tables.len()))
    };
    let mut metric_name = String::new();
    let mut metric_definition = String::new();

    for (table_index, table) in tables.iter().enumerate() {
        let mut metrics = compute_bit_zero_collapse_metrics(table);
        if metric_name.is_empty() {
            metric_name = "zero-collapse".to_string();
            metric_definition =
                "zero-collapse(bit) = (row_count_before - row_count_after_zeroing_and_dedup) / row_count_before"
                    .to_string();
        }

        let row_count_before = table.rows.len();
        let bit_count = table.bits.len();
        let mut max_zero_collapse = 0.0f64;
        let mut sum_zero_collapse = 0.0f64;
        let mut local_best_index: Option<usize> = None;

        for (metric_index, metric) in metrics.iter().enumerate() {
            sum_zero_collapse += metric.zero_collapse;
            if local_best_index
                .map(|best_index| compare_metrics(metric, &metrics[best_index]).is_lt())
                .unwrap_or(true)
            {
                max_zero_collapse = metric.zero_collapse;
                local_best_index = Some(metric_index);
            }
        }

        let mean_zero_collapse = if metrics.is_empty() {
            0.0
        } else {
            sum_zero_collapse / metrics.len() as f64
        };

        total_bit_metrics += bit_count;
        total_zeroing_row_visits += row_count_before * bit_count;
        global_sum_zero_collapse += sum_zero_collapse;

        if let Some(metric_index) = local_best_index {
            let metric = &metrics[metric_index];
            if metric.zero_collapse > global_max_zero_collapse {
                global_max_zero_collapse = metric.zero_collapse;
                global_max_metric = Some(GlobalMaxMetric {
                    table_index,
                    table_bits: table.bits.clone(),
                    bit: metric.bit,
                    bit_index: metric.bit_index,
                    row_count_before: metric.row_count_before,
                    row_count_after_zeroing: metric.row_count_after_zeroing,
                    collapsed_rows: metric.collapsed_rows,
                    zero_collapse: metric.zero_collapse,
                });
            }
        }

        if let Some(table_reports) = &mut table_reports {
            metrics.sort_by(compare_metrics);
            table_reports.push(TableBitZeroCollapseCliReport {
                table_index,
                table_bits: table.bits.clone(),
                row_count_before,
                bit_count,
                max_zero_collapse,
                mean_zero_collapse,
                metrics,
            });
        }

        if (table_index + 1) % 10_000 == 0 {
            println!("processed_tables={}", table_index + 1);
        }
    }

    let compute_seconds = compute_started.elapsed().as_secs_f64();
    let global_mean_zero_collapse = if total_bit_metrics == 0 {
        0.0
    } else {
        global_sum_zero_collapse / total_bit_metrics as f64
    };
    let tables_per_second = rate(tables.len(), compute_seconds);
    let bit_metrics_per_second = rate(total_bit_metrics, compute_seconds);
    let zeroing_row_visits_per_second = rate(total_zeroing_row_visits, compute_seconds);

    let report = BitZeroCollapseAllReport {
        method:
            "For every table and every bit in that table, zero the bit in all rows, deduplicate rows, and measure the relative collapsed-row share"
                .to_string(),
        metric_name,
        metric_definition,
        input: path_string(&args.input),
        summary_only: args.summary_only,
        table_count: tables.len(),
        total_row_count,
        total_bit_metrics,
        total_zeroing_row_visits,
        global_max_zero_collapse,
        global_mean_zero_collapse,
        global_max_metric,
        read_seconds,
        compute_seconds,
        tables_per_second,
        bit_metrics_per_second,
        zeroing_row_visits_per_second,
        tables: table_reports,
    };

    write_json(&args.output, &report)?;

    println!("metric: {}", report.metric_name);
    println!("tables: {}", report.table_count);
    println!("rows: {}", report.total_row_count);
    println!("bit_metrics: {}", report.total_bit_metrics);
    println!(
        "global_max_zero_collapse: {:.12}",
        report.global_max_zero_collapse
    );
    println!(
        "global_mean_zero_collapse: {:.12}",
        report.global_mean_zero_collapse
    );
    println!("read_seconds: {:.6}", report.read_seconds);
    println!("compute_seconds: {:.6}", report.compute_seconds);
    println!("tables_per_second: {:.3}", report.tables_per_second);
    println!(
        "bit_metrics_per_second: {:.3}",
        report.bit_metrics_per_second
    );
    println!(
        "zeroing_row_visits_per_second: {:.3}",
        report.zeroing_row_visits_per_second
    );
    if let Some(metric) = &report.global_max_metric {
        println!("max_table_index: {}", metric.table_index);
        println!("max_bit: {}", metric.bit);
        println!("max_collapsed_rows: {}", metric.collapsed_rows);
        println!("max_row_count_before: {}", metric.row_count_before);
        println!(
            "max_row_count_after_zeroing: {}",
            metric.row_count_after_zeroing
        );
    }
    println!("output: {}", args.output.display());

    Ok(())
}

fn rate(units: usize, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        0.0
    } else {
        units as f64 / seconds
    }
}

fn compare_metrics(
    left: &BitZeroCollapseMetric,
    right: &BitZeroCollapseMetric,
) -> std::cmp::Ordering {
    right
        .zero_collapse
        .total_cmp(&left.zero_collapse)
        .then_with(|| right.collapsed_rows.cmp(&left.collapsed_rows))
        .then_with(|| left.bit.cmp(&right.bit))
        .then_with(|| left.bit_index.cmp(&right.bit_index))
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
        "usage: cargo run --release --bin bit_zero_collapse_all -- [--input <tables.json>] [--output <report.json>] [--summary-only]"
    );
}
