use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json, write_tables, Table};
use tables::subtable_roundtrip::{
    run_progressive_roundtrip, run_selective_roundtrip, NamedTablePool, ProgressiveRoundtripResult,
    RoundtripCheck, SelectiveRoundtripResult, SelectiveStageStats, TableSummary,
};
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    output_root: PathBuf,
    min_merged_arity: usize,
    max_merged_arity: usize,
    limit: usize,
    max_subtable_arity: usize,
    strategy: Strategy,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/originals.tables"),
            output_root: PathBuf::from("codex-output-2026-04-22-decompose-incident-bit-merges-01"),
            min_merged_arity: 16,
            max_merged_arity: 18,
            limit: 40,
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
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--min-merged-arity" => {
                    args.min_merged_arity = expect_value(&mut iter, "--min-merged-arity")?
                        .parse()
                        .with_context(|| "invalid value for --min-merged-arity")?;
                }
                "--max-merged-arity" => {
                    args.max_merged_arity = expect_value(&mut iter, "--max-merged-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-merged-arity")?;
                }
                "--limit" => {
                    args.limit = expect_value(&mut iter, "--limit")?
                        .parse()
                        .with_context(|| "invalid value for --limit")?;
                }
                "--max-subtable-arity" => {
                    args.max_subtable_arity = expect_value(&mut iter, "--max-subtable-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-subtable-arity")?;
                }
                "--strategy" => {
                    args.strategy = Strategy::parse(&expect_value(&mut iter, "--strategy")?)?
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        if args.min_merged_arity == 0 || args.max_merged_arity > 32 {
            bail!(
                "merged arity bounds must stay within 1..=32, got {}..={}",
                args.min_merged_arity,
                args.max_merged_arity
            );
        }
        if args.min_merged_arity > args.max_merged_arity {
            bail!(
                "--min-merged-arity {} exceeds --max-merged-arity {}",
                args.min_merged_arity,
                args.max_merged_arity
            );
        }
        if args.limit == 0 {
            bail!("--limit must be >= 1");
        }
        if args.max_subtable_arity < 2 || args.max_subtable_arity > 32 {
            bail!(
                "--max-subtable-arity must be in 2..=32, got {}",
                args.max_subtable_arity
            );
        }

        Ok(args)
    }
}

#[derive(Clone, Debug, Serialize)]
struct BitSupportRecord {
    table_ids: Vec<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct PoolArtifact {
    name: String,
    check: RoundtripCheck,
}

#[derive(Clone, Debug, Serialize)]
struct DecompositionRecord {
    strategy: String,
    max_subtable_arity: usize,
    extracted_counts_by_arity: BTreeMap<String, usize>,
    selected_counts_by_arity: BTreeMap<String, usize>,
    two_bit_non_taut_count: usize,
    pool_checks: Vec<PoolArtifact>,
    selective_stage_stats: Option<Vec<SelectiveStageStats>>,
}

#[derive(Clone, Debug, Serialize)]
struct SelectedBitRecord {
    bit: u32,
    incident_table_count: usize,
    incident_table_ids: Vec<usize>,
    merged_summary: TableSummary,
    decomposition: DecompositionRecord,
}

#[derive(Clone, Debug, Serialize)]
struct Report {
    method: String,
    input: String,
    output_root: String,
    strategy: String,
    max_subtable_arity: usize,
    min_merged_arity: usize,
    max_merged_arity: usize,
    requested_limit: usize,
    available_bit_count: usize,
    selected_bit_count: usize,
    selection_method: String,
    bit_support_map_path: String,
    merged_tables_path: String,
    selected_bits: Vec<SelectedBitRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    std::fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;

    let tables = load_canonical_tables(&args.input)?;
    let bit_to_tables = build_bit_to_tables(&tables);

    let bit_support_map: BTreeMap<String, BitSupportRecord> = bit_to_tables
        .iter()
        .map(|(&bit, table_ids)| {
            (
                bit.to_string(),
                BitSupportRecord {
                    table_ids: table_ids.clone(),
                },
            )
        })
        .collect();
    let bit_support_map_path = args.output_root.join("bit_to_tables.json");
    write_json(&bit_support_map_path, &bit_support_map)?;

    let mut selected = Vec::new();
    let mut merged_tables = Vec::new();

    for (&bit, table_ids) in &bit_to_tables {
        if selected.len() == args.limit {
            break;
        }

        let Some(union_arity) =
            bounded_incident_union_arity(&tables, table_ids, args.max_merged_arity)?
        else {
            continue;
        };
        if union_arity < args.min_merged_arity || union_arity > args.max_merged_arity {
            continue;
        }

        let merged = merge_incident_tables(&tables, table_ids)?;
        if merged.rows.is_empty() {
            continue;
        }

        let decomposition = run_decomposition(&merged, args.strategy, args.max_subtable_arity)?;
        let record = SelectedBitRecord {
            bit,
            incident_table_count: table_ids.len(),
            incident_table_ids: table_ids.clone(),
            merged_summary: summarize_table(&merged),
            decomposition,
        };
        merged_tables.push(merged);
        selected.push(record);
    }

    if selected.len() < args.limit {
        bail!(
            "found only {} non-empty incident merges with arity in {}..={}, need {}",
            selected.len(),
            args.min_merged_arity,
            args.max_merged_arity,
            args.limit
        );
    }

    let merged_tables_path = args.output_root.join("merged.tables");
    write_tables(&merged_tables_path, &merged_tables)?;

    let report = Report {
        method: "Build the exact bit -> incident table-id map for the input system, scan bits in ascending bit-id order, keep the first bits whose incident-table union schema has arity within the requested range and whose exact incident-table merge is non-empty, materialize one merged table per selected bit, then run the configured subtable roundtrip decomposition in memory for each merged table.".to_string(),
        input: path_string(&args.input),
        output_root: path_string(&args.output_root),
        strategy: args.strategy.as_str().to_string(),
        max_subtable_arity: args.max_subtable_arity,
        min_merged_arity: args.min_merged_arity,
        max_merged_arity: args.max_merged_arity,
        requested_limit: args.limit,
        available_bit_count: bit_to_tables.len(),
        selected_bit_count: selected.len(),
        selection_method: "Ascending bit id among non-empty exact incident merges whose union-bit arity lies in the requested inclusive range.".to_string(),
        bit_support_map_path: path_string(&bit_support_map_path),
        merged_tables_path: path_string(&merged_tables_path),
        selected_bits: selected,
    };

    let report_path = args.output_root.join("report.json");
    write_json(&report_path, &report)?;

    println!("available_bits={}", report.available_bit_count);
    println!("selected_bits={}", report.selected_bit_count);
    println!("strategy={}", report.strategy);
    println!("merged_tables={}", merged_tables.len());
    println!("bit_support_map={}", bit_support_map_path.display());
    println!("merged_tables_path={}", merged_tables_path.display());
    println!("report={}", report_path.display());
    Ok(())
}

fn load_canonical_tables(path: &Path) -> Result<Vec<Table>> {
    let tables = read_tables(path)?;
    Ok(tables
        .into_iter()
        .map(|table| {
            let (bits, rows) = canonicalize_table(&table);
            Table { bits, rows }
        })
        .collect())
}

fn build_bit_to_tables(tables: &[Table]) -> BTreeMap<u32, Vec<usize>> {
    let mut bit_to_tables: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (table_id, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().push(table_id);
        }
    }
    bit_to_tables
}

fn bounded_incident_union_arity(
    tables: &[Table],
    table_ids: &[usize],
    max_merged_arity: usize,
) -> Result<Option<usize>> {
    let mut union_bits = BTreeSet::new();
    for &table_id in table_ids {
        let table = tables
            .get(table_id)
            .with_context(|| format!("missing table {table_id}"))?;
        union_bits.extend(table.bits.iter().copied());
        if union_bits.len() > max_merged_arity {
            return Ok(None);
        }
    }
    Ok(Some(union_bits.len()))
}

fn merge_incident_tables(tables: &[Table], table_ids: &[usize]) -> Result<Table> {
    let Some((&first_id, rest)) = table_ids.split_first() else {
        bail!("cannot merge empty incident table set");
    };

    let mut merge_order = Vec::with_capacity(table_ids.len());
    merge_order.push(first_id);
    merge_order.extend(rest.iter().copied());
    merge_order.sort_by(|&left_id, &right_id| {
        let left = &tables[left_id];
        let right = &tables[right_id];
        left.rows
            .len()
            .cmp(&right.rows.len())
            .then_with(|| left.bits.len().cmp(&right.bits.len()))
            .then_with(|| left.bits.cmp(&right.bits))
            .then_with(|| left_id.cmp(&right_id))
    });

    let first = tables
        .get(merge_order[0])
        .cloned()
        .with_context(|| format!("missing table {}", merge_order[0]))?;
    let mut merged = first;
    for &table_id in &merge_order[1..] {
        let next = tables
            .get(table_id)
            .with_context(|| format!("missing table {table_id}"))?;
        merged = merge_exact(&merged, next)?;
        if merged.rows.is_empty() {
            break;
        }
    }
    Ok(merged)
}

fn run_decomposition(
    source: &Table,
    strategy: Strategy,
    max_subtable_arity: usize,
) -> Result<DecompositionRecord> {
    match strategy {
        Strategy::Exhaustive => {
            let result = run_progressive_roundtrip(source, max_subtable_arity)?;
            Ok(DecompositionRecord {
                strategy: strategy.as_str().to_string(),
                max_subtable_arity,
                extracted_counts_by_arity: counts_from_progressive_extracted(&result),
                selected_counts_by_arity: counts_from_progressive_selected(&result),
                two_bit_non_taut_count: result.two_bit_non_taut.len(),
                pool_checks: pool_artifacts(&result.pools),
                selective_stage_stats: None,
            })
        }
        Strategy::Selective => {
            let result = run_selective_roundtrip(source, max_subtable_arity)?;
            Ok(DecompositionRecord {
                strategy: strategy.as_str().to_string(),
                max_subtable_arity,
                extracted_counts_by_arity: counts_from_selective_extracted(&result),
                selected_counts_by_arity: counts_from_selective_selected(&result),
                two_bit_non_taut_count: result.two_bit_non_taut.len(),
                pool_checks: pool_artifacts(&result.pools),
                selective_stage_stats: Some(result.stage_stats),
            })
        }
    }
}

fn counts_from_progressive_extracted(
    result: &ProgressiveRoundtripResult,
) -> BTreeMap<String, usize> {
    result
        .extracted_by_arity
        .iter()
        .map(|(arity, tables)| (arity.to_string(), tables.len()))
        .collect()
}

fn counts_from_progressive_selected(
    result: &ProgressiveRoundtripResult,
) -> BTreeMap<String, usize> {
    result
        .selected_by_arity
        .iter()
        .map(|(arity, tables)| (arity.to_string(), tables.len()))
        .collect()
}

fn counts_from_selective_extracted(result: &SelectiveRoundtripResult) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    counts.insert("2".to_string(), result.two_bit_all.len());
    counts
}

fn counts_from_selective_selected(result: &SelectiveRoundtripResult) -> BTreeMap<String, usize> {
    result
        .selected_by_arity
        .iter()
        .map(|(arity, tables)| (arity.to_string(), tables.len()))
        .collect()
}

fn pool_artifacts(pools: &[NamedTablePool]) -> Vec<PoolArtifact> {
    pools
        .iter()
        .map(|pool| PoolArtifact {
            name: pool.name.clone(),
            check: pool.check.clone(),
        })
        .collect()
}

fn summarize_table(table: &Table) -> TableSummary {
    tables::subtable_roundtrip::summarize_table(table)
}

fn merge_exact(left: &Table, right: &Table) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!(error))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
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
        "usage:\n  cargo run --release --bin decompose_incident_bit_merges -- [--input <system.tables>] [--output-root <dir>] [--min-merged-arity <n>] [--max-merged-arity <n>] [--limit <n>] [--max-subtable-arity <n>] [--strategy <exhaustive|selective>]"
    );
}
