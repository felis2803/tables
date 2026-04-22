use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use tables::common::{
    arity_distribution, collect_bits, is_full_row_set, project_row, read_tables, sort_dedup_rows,
    total_rows, write_json, Table,
};
use tables::rank_stats::{summarize_table_ranks, RankSummary};
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    origins: PathBuf,
    input_derived_mains: Option<PathBuf>,
    output_root: PathBuf,
    pipeline_exe: PathBuf,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/derived/tables.common_node_fixed_point.json"),
            origins: PathBuf::from("data/raw/origins.json"),
            input_derived_mains: None,
            output_root: PathBuf::from("runs/2026-04-09-origin-aware-min-related-main-step"),
            pipeline_exe: PathBuf::from("target/release/tables.exe"),
            max_rounds: None,
            disable_zero_collapse_bit_filter: false,
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
                "--origins" => args.origins = PathBuf::from(expect_value(&mut iter, "--origins")?),
                "--input-derived-mains" => {
                    args.input_derived_mains = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--input-derived-mains",
                    )?))
                }
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--pipeline-exe" => {
                    args.pipeline_exe = PathBuf::from(expect_value(&mut iter, "--pipeline-exe")?)
                }
                "--max-rounds" => {
                    args.max_rounds = Some(
                        expect_value(&mut iter, "--max-rounds")?
                            .parse()
                            .with_context(|| "invalid value for --max-rounds")?,
                    );
                }
                "--disable-zero-collapse-bit-filter" => {
                    args.disable_zero_collapse_bit_filter = true;
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
struct SystemMetrics {
    table_count: usize,
    bit_count: usize,
    row_count: usize,
    arity_distribution: BTreeMap<String, usize>,
    rank_summary: RankSummary,
}

#[derive(Clone, Debug, Serialize)]
struct MapStats {
    bit_count: usize,
    total_memberships: usize,
    max_memberships_for_one_bit: usize,
}

#[derive(Clone, Debug, Serialize)]
struct PipelineSummary {
    final_table_count: usize,
    final_bit_count: usize,
    final_row_count: usize,
    productive_round_count: usize,
    round_count_including_final_check: usize,
}

#[derive(Clone, Debug, Serialize)]
struct StepReport {
    method: String,
    input: String,
    origins_input: String,
    input_derived_mains: Option<String>,
    selection_metric: String,
    step4_interpretation: String,
    initial_system_metrics: SystemMetrics,
    remaining_system_metrics_after_removal: SystemMetrics,
    pipeline_input_metrics: SystemMetrics,
    initial_derived_main_metrics: SystemMetrics,
    final_derived_main_metrics: SystemMetrics,
    initial_m1_stats: MapStats,
    initial_m2_stats: MapStats,
    initial_m3_stats: MapStats,
    updated_m1_stats: MapStats,
    updated_m2_stats: MapStats,
    updated_m3_stats: MapStats,
    origin_bit_count: usize,
    candidate_bit_count: usize,
    selected_central_bit: u32,
    selected_central_bit_related_bits: Vec<u32>,
    selected_central_bit_related_bit_count: usize,
    selected_central_bit_existing_main_count: usize,
    selected_central_bit_incident_table_ids: Vec<usize>,
    selected_central_bit_incident_table_count: usize,
    selected_central_bit_incident_row_count: usize,
    selected_central_bit_incident_bit_count: usize,
    merged_main_before_drop_bit_count: usize,
    merged_main_before_drop_row_count: usize,
    derived_main_bit_count: usize,
    derived_main_row_count: usize,
    derived_main_stored: bool,
    derived_main_was_tautology: bool,
    removed_system_table_count: usize,
    removed_system_row_count: usize,
    output_m1_before: String,
    output_m2_before: String,
    output_m3_before: String,
    output_m1_after: String,
    output_m2_after: String,
    output_m3_after: String,
    output_remaining_tables: String,
    output_pipeline_input_tables: String,
    output_derived_mains: String,
    output_report: String,
    pipeline_report: String,
    pipeline_summary: PipelineSummary,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;

    let raw_tables = read_tables(&args.input)?;
    let system_tables = canonicalize_tables_individually(raw_tables);
    let origins = read_origins(&args.origins)?;
    let mut derived_mains = read_optional_tables(args.input_derived_mains.as_deref())?;
    for table in &mut derived_mains {
        let (bits, rows) = canonicalize_table(table);
        table.bits = bits;
        table.rows = rows;
    }

    let initial_system_metrics = collect_metrics(&system_tables);
    let initial_derived_main_metrics = collect_metrics(&derived_mains);

    let initial_m1 = build_m1(&system_tables);
    let initial_m2 = build_m2(&system_tables);
    let initial_m3 = build_m3(&derived_mains);

    let (central_bit, related_bits) = select_central_bit(&initial_m2, &origins)?;
    let incident_table_ids: Vec<usize> = initial_m1
        .get(&central_bit)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect();
    if incident_table_ids.is_empty() {
        bail!("selected central bit {central_bit} has no incident tables");
    }
    let incident_tables = collect_tables(&system_tables, &incident_table_ids)?;
    let incident_row_count: usize = incident_tables.iter().map(|table| table.rows.len()).sum();
    let incident_bit_count = collect_bits(&incident_tables).len();

    let merged_main = merge_all_tables(&incident_tables, central_bit)?;
    let merged_main_before_drop_bit_count = merged_main.bits.len();
    let merged_main_before_drop_row_count = merged_main.rows.len();
    let derived_main = drop_central_bit(merged_main, central_bit)?;
    let derived_main_bit_count = derived_main.bits.len();
    let derived_main_row_count = derived_main.rows.len();
    let derived_main_was_tautology =
        is_full_row_set(derived_main.rows.len(), derived_main.bits.len());
    let derived_main_stored = !derived_main.bits.is_empty() && !derived_main_was_tautology;

    let removed_ids: BTreeSet<usize> = incident_table_ids.iter().copied().collect();
    let removed_system_row_count: usize = removed_ids
        .iter()
        .map(|&table_id| system_tables[table_id].rows.len())
        .sum();

    let remaining_tables: Vec<Table> = system_tables
        .iter()
        .enumerate()
        .filter(|(table_id, _)| !removed_ids.contains(table_id))
        .map(|(_, table)| table.clone())
        .collect();
    let remaining_system_metrics_after_removal = collect_metrics(&remaining_tables);

    if derived_main_stored {
        derived_mains.push(derived_main.clone());
    }
    let final_derived_main_metrics = collect_metrics(&derived_mains);

    let pipeline_input_tables = if derived_main_stored {
        let mut tables = remaining_tables.clone();
        tables.push(derived_main.clone());
        tables
    } else {
        remaining_tables.clone()
    };
    let pipeline_input_metrics = collect_metrics(&pipeline_input_tables);

    let updated_m1 = update_m1_after_removal(&initial_m1, central_bit, &removed_ids);
    let updated_m2 = update_m2_after_central_bit(&initial_m2, central_bit);
    let updated_m3 =
        update_m3_after_step(&initial_m3, central_bit, derived_main_stored, &derived_main);

    let remaining_tables_path = args.output_root.join("tables.remaining_after_step.json");
    let pipeline_input_path = args
        .output_root
        .join("tables.pipeline_input_after_step.json");
    let derived_mains_path = args.output_root.join("derived_mains.after_step.json");
    let report_path = args.output_root.join("report.step.json");
    let m1_before_path = args.output_root.join("m1.before.json");
    let m2_before_path = args.output_root.join("m2.before.json");
    let m3_before_path = args.output_root.join("m3.before.json");
    let m1_after_path = args.output_root.join("m1.after.json");
    let m2_after_path = args.output_root.join("m2.after.json");
    let m3_after_path = args.output_root.join("m3.after.json");
    let pipeline_output_path = args.output_root.join("tables.after_pipeline.json");
    let pipeline_report_path = args.output_root.join("report.after_pipeline.json");
    let pipeline_forced_path = args.output_root.join("bits.after_pipeline.forced.json");
    let pipeline_mapping_path = args
        .output_root
        .join("bits.after_pipeline.rewrite_map.json");
    let pipeline_components_path = args.output_root.join("bits.after_pipeline.components.json");
    let pipeline_dropped_path = args
        .output_root
        .join("tables.after_pipeline.dropped_included.json");
    let pipeline_relations_path = args.output_root.join("pairs.after_pipeline.relations.json");
    let pipeline_nodes_path = args.output_root.join("nodes.after_pipeline.json");

    write_json(&remaining_tables_path, &remaining_tables)?;
    write_json(&pipeline_input_path, &pipeline_input_tables)?;
    write_json(&derived_mains_path, &derived_mains)?;
    write_json(&m1_before_path, &initial_m1)?;
    write_json(&m2_before_path, &initial_m2)?;
    write_json(&m3_before_path, &initial_m3)?;
    write_json(&m1_after_path, &updated_m1)?;
    write_json(&m2_after_path, &updated_m2)?;
    write_json(&m3_after_path, &updated_m3)?;

    run_pipeline(
        &args.pipeline_exe,
        &args.origins,
        &pipeline_input_path,
        &pipeline_output_path,
        &pipeline_report_path,
        &pipeline_forced_path,
        &pipeline_mapping_path,
        &pipeline_components_path,
        &pipeline_dropped_path,
        &pipeline_relations_path,
        &pipeline_nodes_path,
        args.max_rounds,
        args.disable_zero_collapse_bit_filter,
    )?;
    let pipeline_summary = parse_pipeline_summary(&pipeline_report_path)?;

    let report = StepReport {
        method: "Build m1=bit->system tables, m2=bit->related non-self bits from current system tables, and m3=bit->derived main tables; exclude origin bits from candidate selection; choose the non-origin bit with the smallest m2 related-bit count; merge all incident system tables for that bit; drop the central bit; remove consumed system tables; register the derived main in m3; then run the standard fixed-point pipeline on the remaining system plus the stored derived main when it is non-empty and non-tautological.".to_string(),
        input: args.input.display().to_string(),
        origins_input: args.origins.display().to_string(),
        input_derived_mains: args
            .input_derived_mains
            .as_ref()
            .map(|path| path.display().to_string()),
        selection_metric: "m2 related-bit count excluding self".to_string(),
        step4_interpretation: "Step 4 was interpreted as ordering by m2, because m3 stores derived main memberships and is empty before the first derived main unless an input-derived-mains file is provided.".to_string(),
        initial_system_metrics,
        remaining_system_metrics_after_removal,
        pipeline_input_metrics,
        initial_derived_main_metrics,
        final_derived_main_metrics,
        initial_m1_stats: map_stats(&initial_m1),
        initial_m2_stats: map_stats(&initial_m2),
        initial_m3_stats: map_stats(&initial_m3),
        updated_m1_stats: map_stats(&updated_m1),
        updated_m2_stats: map_stats(&updated_m2),
        updated_m3_stats: map_stats(&updated_m3),
        origin_bit_count: origins.len(),
        candidate_bit_count: initial_m2.keys().filter(|bit| !origins.contains(bit)).count(),
        selected_central_bit: central_bit,
        selected_central_bit_related_bits: related_bits.clone(),
        selected_central_bit_related_bit_count: related_bits.len(),
        selected_central_bit_existing_main_count: initial_m3.get(&central_bit).map(BTreeSet::len).unwrap_or(0),
        selected_central_bit_incident_table_ids: incident_table_ids.clone(),
        selected_central_bit_incident_table_count: incident_table_ids.len(),
        selected_central_bit_incident_row_count: incident_row_count,
        selected_central_bit_incident_bit_count: incident_bit_count,
        merged_main_before_drop_bit_count,
        merged_main_before_drop_row_count,
        derived_main_bit_count,
        derived_main_row_count,
        derived_main_stored,
        derived_main_was_tautology,
        removed_system_table_count: removed_ids.len(),
        removed_system_row_count,
        output_m1_before: m1_before_path.display().to_string(),
        output_m2_before: m2_before_path.display().to_string(),
        output_m3_before: m3_before_path.display().to_string(),
        output_m1_after: m1_after_path.display().to_string(),
        output_m2_after: m2_after_path.display().to_string(),
        output_m3_after: m3_after_path.display().to_string(),
        output_remaining_tables: remaining_tables_path.display().to_string(),
        output_pipeline_input_tables: pipeline_input_path.display().to_string(),
        output_derived_mains: derived_mains_path.display().to_string(),
        output_report: report_path.display().to_string(),
        pipeline_report: pipeline_report_path.display().to_string(),
        pipeline_summary,
    };

    write_json(&report_path, &report)?;

    println!("selected_central_bit={}", report.selected_central_bit);
    println!(
        "selected_central_bit_related_bit_count={}",
        report.selected_central_bit_related_bit_count
    );
    println!(
        "selected_central_bit_incident_table_count={}",
        report.selected_central_bit_incident_table_count
    );
    println!(
        "merged_main_before_drop_bits={}",
        report.merged_main_before_drop_bit_count
    );
    println!(
        "merged_main_before_drop_rows={}",
        report.merged_main_before_drop_row_count
    );
    println!("derived_main_bits={}", report.derived_main_bit_count);
    println!("derived_main_rows={}", report.derived_main_row_count);
    println!("derived_main_stored={}", report.derived_main_stored);
    println!(
        "pipeline_final_tables={}",
        report.pipeline_summary.final_table_count
    );
    println!(
        "pipeline_final_bits={}",
        report.pipeline_summary.final_bit_count
    );
    println!(
        "pipeline_final_rows={}",
        report.pipeline_summary.final_row_count
    );
    println!(
        "pipeline_productive_rounds={}",
        report.pipeline_summary.productive_round_count
    );
    println!("output_root={}", args.output_root.display());

    Ok(())
}

fn canonicalize_tables_individually(tables: Vec<Table>) -> Vec<Table> {
    tables
        .into_iter()
        .map(|table| {
            let (bits, rows) = canonicalize_table(&table);
            Table { bits, rows }
        })
        .collect()
}

fn read_origins(path: &Path) -> Result<BTreeSet<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let origins = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(origins)
}

fn read_optional_tables(path: Option<&Path>) -> Result<Vec<Table>> {
    match path {
        Some(path) if path.exists() => read_tables(path),
        Some(_) | None => Ok(Vec::new()),
    }
}

fn collect_metrics(tables: &[Table]) -> SystemMetrics {
    SystemMetrics {
        table_count: tables.len(),
        bit_count: collect_bits(tables).len(),
        row_count: total_rows(tables),
        arity_distribution: arity_distribution(tables),
        rank_summary: summarize_table_ranks(tables, 10),
    }
}

fn build_m1(tables: &[Table]) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut m1: BTreeMap<u32, BTreeSet<usize>> = BTreeMap::new();
    for (table_id, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            m1.entry(bit).or_default().insert(table_id);
        }
    }
    m1
}

fn build_m2(tables: &[Table]) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut m2: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    for table in tables {
        for &bit in &table.bits {
            let entry = m2.entry(bit).or_default();
            entry.extend(table.bits.iter().copied().filter(|other| *other != bit));
        }
    }
    m2
}

fn build_m3(derived_mains: &[Table]) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut m3: BTreeMap<u32, BTreeSet<usize>> = BTreeMap::new();
    for (main_id, table) in derived_mains.iter().enumerate() {
        for &bit in &table.bits {
            m3.entry(bit).or_default().insert(main_id);
        }
    }
    m3
}

fn select_central_bit(
    m2: &BTreeMap<u32, BTreeSet<u32>>,
    origins: &BTreeSet<u32>,
) -> Result<(u32, Vec<u32>)> {
    m2.iter()
        .filter(|(bit, _)| !origins.contains(bit))
        .min_by(|(left_bit, left_related), (right_bit, right_related)| {
            left_related
                .len()
                .cmp(&right_related.len())
                .then_with(|| left_bit.cmp(right_bit))
        })
        .map(|(&bit, related_bits)| (bit, related_bits.iter().copied().collect()))
        .context("no non-origin bit candidates found in m2")
}

fn collect_tables(tables: &[Table], table_ids: &[usize]) -> Result<Vec<Table>> {
    table_ids
        .iter()
        .map(|&table_id| {
            tables
                .get(table_id)
                .cloned()
                .with_context(|| format!("missing table {table_id}"))
        })
        .collect()
}

fn merge_all_tables(tables: &[Table], central_bit: u32) -> Result<Table> {
    let Some(first) = tables.first() else {
        bail!("no incident tables found for central bit {central_bit}");
    };

    let mut merged = first.clone();
    for table in &tables[1..] {
        merged = merge_two_tables(&merged, table, central_bit)?;
    }
    Ok(merged)
}

fn merge_two_tables(left: &Table, right: &Table, central_bit: u32) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| {
            anyhow!("failed to merge tables for central bit {central_bit}: {error}")
        })?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn drop_central_bit(table: Table, central_bit: u32) -> Result<Table> {
    let mut projected_bits = Vec::with_capacity(table.bits.len().saturating_sub(1));
    let mut projected_indices = Vec::with_capacity(table.bits.len().saturating_sub(1));

    for (index, &bit) in table.bits.iter().enumerate() {
        if bit == central_bit {
            continue;
        }
        projected_bits.push(bit);
        projected_indices.push(index);
    }

    if projected_bits.len() + 1 != table.bits.len() {
        bail!(
            "central bit {central_bit} missing from merged table {:?}",
            table.bits
        );
    }

    let mut projected_rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    Ok(Table {
        bits: projected_bits,
        rows: projected_rows,
    })
}

fn update_m1_after_removal(
    initial_m1: &BTreeMap<u32, BTreeSet<usize>>,
    central_bit: u32,
    removed_ids: &BTreeSet<usize>,
) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut updated = BTreeMap::new();
    for (&bit, table_ids) in initial_m1 {
        if bit == central_bit {
            continue;
        }
        let remaining: BTreeSet<usize> = table_ids
            .iter()
            .copied()
            .filter(|table_id| !removed_ids.contains(table_id))
            .collect();
        if !remaining.is_empty() {
            updated.insert(bit, remaining);
        }
    }
    updated
}

fn update_m2_after_central_bit(
    initial_m2: &BTreeMap<u32, BTreeSet<u32>>,
    central_bit: u32,
) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut updated = BTreeMap::new();
    for (&bit, related_bits) in initial_m2 {
        if bit == central_bit {
            continue;
        }
        let filtered: BTreeSet<u32> = related_bits
            .iter()
            .copied()
            .filter(|related_bit| *related_bit != central_bit)
            .collect();
        updated.insert(bit, filtered);
    }
    updated
}

fn update_m3_after_step(
    initial_m3: &BTreeMap<u32, BTreeSet<usize>>,
    central_bit: u32,
    derived_main_stored: bool,
    derived_main: &Table,
) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut updated = BTreeMap::new();
    for (&bit, main_ids) in initial_m3 {
        if bit == central_bit {
            continue;
        }
        updated.insert(bit, main_ids.clone());
    }

    if derived_main_stored {
        let new_main_id = initial_m3
            .values()
            .flat_map(|ids| ids.iter().copied())
            .max()
            .map(|max_id| max_id + 1)
            .unwrap_or(0);
        for &bit in &derived_main.bits {
            if bit == central_bit {
                continue;
            }
            updated.entry(bit).or_default().insert(new_main_id);
        }
    }

    updated
}

fn map_stats<T>(map: &BTreeMap<u32, BTreeSet<T>>) -> MapStats {
    MapStats {
        bit_count: map.len(),
        total_memberships: map.values().map(BTreeSet::len).sum(),
        max_memberships_for_one_bit: map.values().map(BTreeSet::len).max().unwrap_or(0),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_pipeline(
    pipeline_exe: &Path,
    origins_path: &Path,
    input_path: &Path,
    output_path: &Path,
    report_path: &Path,
    forced_path: &Path,
    mapping_path: &Path,
    components_path: &Path,
    dropped_path: &Path,
    relations_path: &Path,
    nodes_path: &Path,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
) -> Result<()> {
    let mut command = Command::new(pipeline_exe);
    command.arg("--input").arg(input_path);
    command.arg("--origins").arg(origins_path);
    if let Some(max_rounds) = max_rounds {
        command.arg("--max-rounds").arg(max_rounds.to_string());
    }
    if disable_zero_collapse_bit_filter {
        command.arg("--disable-zero-collapse-bit-filter");
    }
    command
        .arg("--output")
        .arg(output_path)
        .arg("--report")
        .arg(report_path)
        .arg("--forced")
        .arg(forced_path)
        .arg("--mapping")
        .arg(mapping_path)
        .arg("--components")
        .arg(components_path)
        .arg("--dropped")
        .arg(dropped_path)
        .arg("--relations")
        .arg(relations_path)
        .arg("--nodes")
        .arg(nodes_path);

    let output = command
        .output()
        .with_context(|| format!("failed to run {}", pipeline_exe.display()))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() { stderr } else { stdout };
    bail!("pipeline failed: {detail}");
}

fn parse_pipeline_summary(path: &Path) -> Result<PipelineSummary> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let report: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    Ok(PipelineSummary {
        final_table_count: required_usize(&report, "final_table_count")?,
        final_bit_count: required_usize(&report, "final_bit_count")?,
        final_row_count: required_usize(&report, "final_row_count")?,
        productive_round_count: required_usize(&report, "productive_round_count")?,
        round_count_including_final_check: required_usize(
            &report,
            "round_count_including_final_check",
        )?,
    })
}

fn required_usize(report: &Value, key: &str) -> Result<usize> {
    report[key]
        .as_u64()
        .map(|value| value as usize)
        .ok_or_else(|| anyhow!("missing or invalid {key}"))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --bin apply_origin_aware_min_related_main_step -- [--input <path>] [--origins <path>] [--input-derived-mains <path>] [--output-root <dir>] [--pipeline-exe <path>] [--max-rounds <n>] [--disable-zero-collapse-bit-filter]"
    );
}
