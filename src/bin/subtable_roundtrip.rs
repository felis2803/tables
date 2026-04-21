use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json, write_tables, Table};
use tables::subtable_roundtrip::{
    run_progressive_roundtrip, run_selective_roundtrip, summarize_table, NamedTablePool,
    ProgressiveRoundtripResult, RoundtripCheck, SelectiveRoundtripResult,
    SelectiveStageStats, TableSummary,
};
use tables::subset_absorption::canonicalize_table;

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    table_index: usize,
    output_root: PathBuf,
    max_subtable_arity: usize,
    strategy: Strategy,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from(
                "codex-output-2026-04-22-pipeline-rank-chain-subtables-01/rank-chain/final.tables",
            ),
            table_index: 0,
            output_root: PathBuf::from("runs/2026-04-22-subtable-roundtrip"),
            max_subtable_arity: 4,
            strategy: Strategy::Exhaustive,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
enum Strategy {
    Exhaustive,
    Selective,
}

impl Strategy {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "exhaustive" => Ok(Self::Exhaustive),
            "selective" => Ok(Self::Selective),
            other => bail!("unknown --strategy value: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Exhaustive => "exhaustive",
            Self::Selective => "selective",
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
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--max-subtable-arity" => {
                    args.max_subtable_arity = expect_value(&mut iter, "--max-subtable-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-subtable-arity")?;
                }
                "--strategy" => {
                    args.strategy = Strategy::parse(&expect_value(&mut iter, "--strategy")?)?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        if args.max_subtable_arity < 2 {
            bail!(
                "--max-subtable-arity must be >= 2 for the progressive roundtrip, got {}",
                args.max_subtable_arity
            );
        }

        Ok(args)
    }
}

#[derive(Clone, Debug, Serialize)]
struct PoolArtifact {
    name: String,
    factors_path: String,
    reconstructed_path: String,
    check: RoundtripCheck,
}

#[derive(Clone, Debug, Serialize)]
struct Report {
    method: String,
    strategy: String,
    input: String,
    table_index: usize,
    output_root: String,
    max_subtable_arity: usize,
    source_summary: TableSummary,
    extracted_counts_by_arity: BTreeMap<String, usize>,
    selected_counts_by_arity: BTreeMap<String, usize>,
    two_bit_non_taut_count: usize,
    subtable_outputs: BTreeMap<String, String>,
    pool_outputs: Vec<PoolArtifact>,
    selective_stage_stats: Option<Vec<SelectiveStageStats>>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    std::fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;
    let subtables_dir = args.output_root.join("subtables");
    let pools_dir = args.output_root.join("pools");
    let reconstruction_dir = args.output_root.join("reconstruction");
    std::fs::create_dir_all(&subtables_dir)
        .with_context(|| format!("failed to create {}", subtables_dir.display()))?;
    std::fs::create_dir_all(&pools_dir)
        .with_context(|| format!("failed to create {}", pools_dir.display()))?;
    std::fs::create_dir_all(&reconstruction_dir)
        .with_context(|| format!("failed to create {}", reconstruction_dir.display()))?;

    let source = load_table(&args.input, args.table_index)?;

    let (method, extracted_counts_by_arity, selected_counts_by_arity, mut subtable_outputs, pool_outputs, two_bit_non_taut_count, selective_stage_stats) =
        match args.strategy {
            Strategy::Exhaustive => {
                let result = run_progressive_roundtrip(&source, args.max_subtable_arity)?;
                let subtable_outputs = write_exhaustive_subtable_artifacts(&subtables_dir, &result)?;
                let pool_outputs =
                    write_pool_artifacts(&pools_dir, &reconstruction_dir, &result.pools)?;
                let extracted_counts_by_arity = result
                    .extracted_by_arity
                    .iter()
                    .map(|(arity, tables)| (arity.to_string(), tables.len()))
                    .collect();
                let selected_counts_by_arity = result
                    .selected_by_arity
                    .iter()
                    .map(|(arity, tables)| (arity.to_string(), tables.len()))
                    .collect();
                (
                    "For one selected table, extract every exact 2-bit projection, drop the tautological projections from that pool, test exact reconstruction from the remaining factors, then progressively extend the pool with every exact 3-bit projection, every exact 4-bit projection, and so on, always dropping tautological projections before the join and stopping as soon as the natural join reconstructs the original table exactly.".to_string(),
                    extracted_counts_by_arity,
                    selected_counts_by_arity,
                    subtable_outputs,
                    pool_outputs,
                    result.two_bit_non_taut.len(),
                    None,
                )
            }
            Strategy::Selective => {
                let result = run_selective_roundtrip(&source, args.max_subtable_arity)?;
                let subtable_outputs = write_selective_subtable_artifacts(&subtables_dir, &result)?;
                let pool_outputs =
                    write_pool_artifacts(&pools_dir, &reconstruction_dir, &result.pools)?;
                let mut extracted_counts_by_arity = BTreeMap::new();
                extracted_counts_by_arity.insert("2".to_string(), result.two_bit_all.len());
                let selected_counts_by_arity = result
                    .selected_by_arity
                    .iter()
                    .map(|(arity, tables)| (arity.to_string(), tables.len()))
                    .collect();
                (
                    "For one selected table, extract the full exact 2-bit layer, drop only the 2-bit tautologies, reconstruct from that pool, then selectively add only those higher-arity exact projections that most reduce the remaining gap to the source table. The selector prioritizes fewer missing source bits and then fewer extra reconstructed rows relative to the exact source projection on the reconstructed schema.".to_string(),
                    extracted_counts_by_arity,
                    selected_counts_by_arity,
                    subtable_outputs,
                    pool_outputs,
                    result.two_bit_non_taut.len(),
                    Some(result.stage_stats),
                )
            }
        };

    let source_path = args.output_root.join("source.tables");
    write_tables(&source_path, std::slice::from_ref(&source))?;
    subtable_outputs.insert("source".to_string(), path_string(&source_path));

    let report = Report {
        method,
        strategy: args.strategy.as_str().to_string(),
        input: path_string(&args.input),
        table_index: args.table_index,
        output_root: path_string(&args.output_root),
        max_subtable_arity: args.max_subtable_arity,
        source_summary: summarize_table(&source),
        extracted_counts_by_arity,
        selected_counts_by_arity,
        two_bit_non_taut_count,
        subtable_outputs,
        pool_outputs,
        selective_stage_stats,
    };

    let report_path = args.output_root.join("report.json");
    write_json(&report_path, &report)?;

    println!("source_arity={}", report.source_summary.bit_count);
    println!("source_rows={}", report.source_summary.row_count);
    println!("two_bit_non_taut_count={}", report.two_bit_non_taut_count);
    for pool in &report.pool_outputs {
        println!(
            "pool[{}]: factors={} matches_source={}",
            pool.name, pool.check.factor_count, pool.check.matches_source
        );
    }
    println!("report={}", report_path.display());
    Ok(())
}

fn load_table(path: &Path, table_index: usize) -> Result<Table> {
    let tables = read_tables(path)?;
    let table = tables
        .get(table_index)
        .cloned()
        .with_context(|| format!("table_index {} is out of range", table_index))?;
    let (bits, rows) = canonicalize_table(&table);
    Ok(Table { bits, rows })
}

fn write_subtable_artifacts(
    output_dir: &Path,
    result: &ProgressiveRoundtripResult,
) -> Result<BTreeMap<String, String>> {
    let mut outputs = BTreeMap::new();

    for (arity, subtables) in &result.extracted_by_arity {
        let path = output_dir.join(format!("{arity}_bit.all.tables"));
        write_tables(&path, subtables)?;
        outputs.insert(format!("{arity}_bit_all"), path_string(&path));
    }

    for (arity, subtables) in &result.selected_by_arity {
        let path = output_dir.join(format!("{arity}_bit.non_taut.tables"));
        write_tables(&path, subtables)?;
        outputs.insert(format!("{arity}_bit_non_taut"), path_string(&path));
    }

    Ok(outputs)
}

fn write_exhaustive_subtable_artifacts(
    output_dir: &Path,
    result: &ProgressiveRoundtripResult,
) -> Result<BTreeMap<String, String>> {
    write_subtable_artifacts(output_dir, result)
}

fn write_selective_subtable_artifacts(
    output_dir: &Path,
    result: &SelectiveRoundtripResult,
) -> Result<BTreeMap<String, String>> {
    let mut outputs = BTreeMap::new();

    let two_bit_all_path = output_dir.join("2_bit.all.tables");
    write_tables(&two_bit_all_path, &result.two_bit_all)?;
    outputs.insert("2_bit_all".to_string(), path_string(&two_bit_all_path));

    let two_bit_non_taut_path = output_dir.join("2_bit.non_taut.tables");
    write_tables(&two_bit_non_taut_path, &result.two_bit_non_taut)?;
    outputs.insert(
        "2_bit_non_taut".to_string(),
        path_string(&two_bit_non_taut_path),
    );

    for (arity, subtables) in &result.selected_by_arity {
        if *arity == 2 {
            continue;
        }
        let path = output_dir.join(format!("{arity}_bit.selected.tables"));
        write_tables(&path, subtables)?;
        outputs.insert(format!("{arity}_bit_selected"), path_string(&path));
    }

    Ok(outputs)
}

fn write_pool_artifacts(
    pools_dir: &Path,
    reconstruction_dir: &Path,
    pools: &[NamedTablePool],
) -> Result<Vec<PoolArtifact>> {
    let mut outputs = Vec::new();
    for pool in pools {
        let normalized_name = pool.name.replace('+', "_");
        let factors_path = pools_dir.join(format!("pool.{normalized_name}.tables"));
        write_tables(&factors_path, &pool.factors)?;

        let reconstructed_path =
            reconstruction_dir.join(format!("reconstructed.from_{normalized_name}.tables"));
        match &pool.reconstructed {
            Some(table) => write_tables(&reconstructed_path, std::slice::from_ref(table))?,
            None => write_tables(&reconstructed_path, &[])?,
        }

        outputs.push(PoolArtifact {
            name: pool.name.clone(),
            factors_path: path_string(&factors_path),
            reconstructed_path: path_string(&reconstructed_path),
            check: pool.check.clone(),
        });
    }
    Ok(outputs)
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin subtable_roundtrip -- --input <tables> [--table-index <n>] [--output-root <dir>] [--max-subtable-arity <n>] [--strategy <exhaustive|selective>]"
    );
}
