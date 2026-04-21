use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json, write_tables, Table};
use tables::subtable_roundtrip::{build_roundtrip_check, summarize_table, TableSummary};
use tables::subset_absorption::canonicalize_table;

#[derive(Clone, Debug)]
struct Args {
    source: PathBuf,
    source_table_index: usize,
    factors: PathBuf,
    output_root: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            source: PathBuf::from(
                "codex-output-2026-04-22-pipeline-rank-chain-subtables-01/rank-chain/final.tables",
            ),
            source_table_index: 0,
            factors: PathBuf::from(
                "codex-output-2026-04-22-subtable-roundtrip-exhaustive-02/pools/pool.2_3.tables",
            ),
            output_root: PathBuf::from("runs/2026-04-22-check-factor-pool-roundtrip"),
        }
    }
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self::default();
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--source" => args.source = PathBuf::from(expect_value(&mut iter, "--source")?),
                "--source-table-index" => {
                    args.source_table_index = expect_value(&mut iter, "--source-table-index")?
                        .parse()
                        .with_context(|| "invalid value for --source-table-index")?;
                }
                "--factors" => args.factors = PathBuf::from(expect_value(&mut iter, "--factors")?),
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
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
struct Report {
    source: String,
    source_table_index: usize,
    factors: String,
    output_root: String,
    source_summary: TableSummary,
    factor_count: usize,
    factor_arity_distribution: BTreeMap<String, usize>,
    factor_tautology_count: usize,
    reconstructed_summary: Option<TableSummary>,
    matches_source: bool,
    reconstructed_path: String,
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    std::fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;

    let source = load_table(&args.source, args.source_table_index)?;
    let factors = load_tables(&args.factors)?;
    let check = build_roundtrip_check("factor_pool", &source, &factors)?;

    let reconstructed_path = args.output_root.join("reconstructed.tables");
    match &check.reconstructed {
        Some(table) => write_tables(&reconstructed_path, std::slice::from_ref(table))?,
        None => write_tables(&reconstructed_path, &[])?,
    }

    let report = Report {
        source: path_string(&args.source),
        source_table_index: args.source_table_index,
        factors: path_string(&args.factors),
        output_root: path_string(&args.output_root),
        source_summary: summarize_table(&source),
        factor_count: factors.len(),
        factor_arity_distribution: check.check.factor_arity_distribution.clone(),
        factor_tautology_count: check.check.factor_tautology_count,
        reconstructed_summary: check.check.reconstructed_summary.clone(),
        matches_source: check.check.matches_source,
        reconstructed_path: path_string(&reconstructed_path),
    };

    let report_path = args.output_root.join("report.json");
    write_json(&report_path, &report)?;

    println!("factor_count={}", report.factor_count);
    println!("factor_tautology_count={}", report.factor_tautology_count);
    println!("matches_source={}", report.matches_source);
    println!("report={}", report_path.display());
    Ok(())
}

fn load_table(path: &Path, table_index: usize) -> Result<Table> {
    let tables = read_tables(path)?;
    let table = tables
        .get(table_index)
        .cloned()
        .with_context(|| format!("table_index {} is out of range", table_index))?;
    Ok(canonicalize(&table))
}

fn load_tables(path: &Path) -> Result<Vec<Table>> {
    Ok(read_tables(path)?.iter().map(canonicalize).collect())
}

fn canonicalize(table: &Table) -> Table {
    let (bits, rows) = canonicalize_table(table);
    Table { bits, rows }
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn path_string(path: &Path) -> String {
    path.display().to_string()
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin check_factor_pool_roundtrip -- [--source <source.tables>] [--source-table-index <index>] --factors <factors.tables> [--output-root <dir>]"
    );
}
