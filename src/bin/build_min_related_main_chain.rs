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
use tables::subset_absorption::{canonicalize_table, collapse_equal_bitsets, to_tables};
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input_system: PathBuf,
    input_derived_mains: PathBuf,
    origins: PathBuf,
    output_root: PathBuf,
    pipeline_exe: PathBuf,
    iterations: usize,
    exclude_bits: BTreeSet<u32>,
    exclude_bits_file: Option<PathBuf>,
    checkpoint_every: usize,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input_system: PathBuf::from(
                "runs/2026-04-09-origin-aware-min-related-main-step/tables.after_pipeline.json",
            ),
            input_derived_mains: PathBuf::from(
                "runs/2026-04-09-origin-aware-min-related-main-step/derived_mains.after_step.json",
            ),
            origins: PathBuf::from("data/raw/origins.json"),
            output_root: PathBuf::from("runs/2026-04-09-min-related-main-chain-next10"),
            pipeline_exe: PathBuf::from("target/release/tables.exe"),
            iterations: 10,
            exclude_bits: BTreeSet::from([10437]),
            exclude_bits_file: None,
            checkpoint_every: 50,
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
                "--input-system" => {
                    args.input_system = PathBuf::from(expect_value(&mut iter, "--input-system")?)
                }
                "--input-derived-mains" => {
                    args.input_derived_mains =
                        PathBuf::from(expect_value(&mut iter, "--input-derived-mains")?)
                }
                "--origins" => args.origins = PathBuf::from(expect_value(&mut iter, "--origins")?),
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--pipeline-exe" => {
                    args.pipeline_exe = PathBuf::from(expect_value(&mut iter, "--pipeline-exe")?)
                }
                "--iterations" => {
                    args.iterations = expect_value(&mut iter, "--iterations")?
                        .parse()
                        .with_context(|| "invalid value for --iterations")?;
                }
                "--checkpoint-every" => {
                    args.checkpoint_every = expect_value(&mut iter, "--checkpoint-every")?
                        .parse()
                        .with_context(|| "invalid value for --checkpoint-every")?;
                }
                "--exclude-bit" => {
                    args.exclude_bits.insert(
                        expect_value(&mut iter, "--exclude-bit")?
                            .parse()
                            .with_context(|| "invalid value for --exclude-bit")?,
                    );
                }
                "--exclude-bits-file" => {
                    args.exclude_bits_file = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--exclude-bits-file",
                    )?))
                }
                "--clear-exclude-bits" => args.exclude_bits.clear(),
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

        if let Some(path) = &args.exclude_bits_file {
            let file_bits = read_bits_file(path)?;
            args.exclude_bits.extend(file_bits);
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
struct MainSummary {
    bit_count: usize,
    row_count: usize,
    bits: Vec<u32>,
    is_tautology: bool,
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
struct IterationSummary {
    iteration: usize,
    central_bit: u32,
    related_bit_count_before: usize,
    donor_main_count: usize,
    donor_subtables_merged: usize,
    incident_regular_table_count: usize,
    incident_regular_row_count: usize,
    merged_main_bit_count_before_pipeline: usize,
    merged_main_row_count_before_pipeline: usize,
    pipeline_productive_round_count: usize,
    pipeline_round_count_including_final_check: usize,
    regular_table_count_after: usize,
    regular_bit_count_after: usize,
    regular_row_count_after: usize,
    main_table_count_after: usize,
    main_bit_count_after: usize,
    main_row_count_after: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ChainReport {
    method: String,
    input_system: String,
    input_derived_mains: String,
    origins_input: String,
    iterations_requested: usize,
    iterations_completed: usize,
    checkpoint_every: usize,
    selection_metric: String,
    m2_update_policy: String,
    donor_projection_rule: String,
    central_bits_excluded_initially: Vec<u32>,
    central_bits_processed: Vec<u32>,
    initial_regular_metrics: SystemMetrics,
    initial_main_metrics: SystemMetrics,
    final_regular_metrics: SystemMetrics,
    final_main_metrics: SystemMetrics,
    final_combined_metrics: SystemMetrics,
    output_checkpoint_regular_tables: String,
    output_checkpoint_derived_mains: String,
    output_checkpoint_combined_system: String,
    output_checkpoint_exclude_bits: String,
    output_iteration_summaries: String,
    output_final_regular_tables: String,
    output_final_derived_mains: String,
    output_final_combined_system: String,
    iterations: Vec<IterationSummary>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;
    let work_dir = args.output_root.join("work");
    fs::create_dir_all(&work_dir)
        .with_context(|| format!("failed to create {}", work_dir.display()))?;

    let combined_system = canonicalize_tables(read_tables(&args.input_system)?);
    let input_derived_mains = canonicalize_tables(read_tables(&args.input_derived_mains)?);
    let origins = read_origins(&args.origins)?;

    let (mut regular_tables, mut old_mains) =
        separate_regular_and_mains(&combined_system, &input_derived_mains)?;
    let initial_regular_metrics = collect_metrics(&regular_tables);
    let initial_main_metrics = collect_metrics(&old_mains);
    let mut processed_bits = args.exclude_bits.clone();
    let mut iteration_reports: Vec<IterationSummary> = Vec::new();
    let mut processed_central_bits = Vec::new();
    let checkpoint_regular_path = args.output_root.join("checkpoint.regular.json");
    let checkpoint_mains_path = args.output_root.join("checkpoint.derived_mains.json");
    let checkpoint_combined_path = args.output_root.join("checkpoint.combined.json");
    let checkpoint_exclude_bits_path = args.output_root.join("checkpoint.exclude_bits.json");
    let checkpoint_meta_path = args.output_root.join("checkpoint.meta.json");
    let iteration_summaries_path = args.output_root.join("iterations.summary.json");
    let pipeline_input_path = work_dir.join("tables.pipeline_input.json");
    let pipeline_output_path = work_dir.join("tables.after_pipeline.json");
    let pipeline_report_path = work_dir.join("report.after_pipeline.json");
    let pipeline_forced_path = work_dir.join("bits.after_pipeline.forced.json");
    let pipeline_mapping_path = work_dir.join("bits.after_pipeline.rewrite_map.json");
    let pipeline_components_path = work_dir.join("bits.after_pipeline.components.json");
    let pipeline_dropped_path = work_dir.join("tables.after_pipeline.dropped_included.json");
    let pipeline_relations_path = work_dir.join("pairs.after_pipeline.relations.json");
    let pipeline_nodes_path = work_dir.join("nodes.after_pipeline.json");

    for iteration in 1..=args.iterations {
        let m1_before = build_m1(&regular_tables);
        let m2_before_raw = build_m2(&regular_tables);
        let m2_before = remove_processed_bits_from_m2(m2_before_raw, &processed_bits);
        let m3_before = build_m3(&old_mains);

        let (central_bit, related_bits_before) =
            select_next_central_bit(&m1_before, &m2_before, &origins)?;
        let related_bits_set: BTreeSet<u32> = related_bits_before.iter().copied().collect();
        let incident_regular_table_ids: Vec<usize> = m1_before
            .get(&central_bit)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let incident_regular_tables = collect_tables(&regular_tables, &incident_regular_table_ids)?;
        let incident_regular_row_count: usize = incident_regular_tables
            .iter()
            .map(|table| table.rows.len())
            .sum();

        let mut new_main = merge_all_tables(&incident_regular_tables, central_bit)?;

        let removed_ids: BTreeSet<usize> = incident_regular_table_ids.iter().copied().collect();
        let regular_after_removal = remove_table_ids(&regular_tables, &removed_ids);
        let donor_main_ids = donor_main_ids(&m3_before, &related_bits_before);
        let mut donor_subtables_merged = 0usize;
        for &main_id in &donor_main_ids {
            let donor_main = old_mains
                .get(main_id)
                .with_context(|| format!("missing donor main {main_id}"))?;
            if let Some(projected) = project_table_to_allowed_bits(donor_main, &related_bits_set)? {
                new_main = merge_two_tables(&new_main, &projected, central_bit)?;
                donor_subtables_merged += 1;
            }
        }
        let merged_main_before_pipeline = summarize_main(&new_main);

        let mut pipeline_input_tables = regular_after_removal.clone();
        pipeline_input_tables.extend(old_mains.clone());
        pipeline_input_tables.push(new_main.clone());
        write_tables_atomically_validated(&pipeline_input_path, &pipeline_input_tables)?;

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
        let pipeline_output_tables = canonicalize_tables(read_tables(&pipeline_output_path)?);
        let pipeline_summary = parse_pipeline_summary(&pipeline_report_path)?;

        let mut main_targets_before_pipeline = old_mains.clone();
        main_targets_before_pipeline.push(new_main.clone());
        let resolved_mains_after_pipeline =
            resolve_main_targets_in_system(&pipeline_output_tables, &main_targets_before_pipeline)?;
        let regular_after_pipeline =
            remove_unique_exact_tables(&pipeline_output_tables, &resolved_mains_after_pipeline)?;
        let regular_after_pipeline_metrics = collect_metrics(&regular_after_pipeline);

        let mains_after_drop =
            drop_bit_from_all_mains(&resolved_mains_after_pipeline, central_bit)?;
        let mains_after_drop_metrics = collect_metrics(&mains_after_drop);

        processed_bits.insert(central_bit);
        processed_central_bits.push(central_bit);

        let iteration_report = IterationSummary {
            iteration,
            central_bit,
            related_bit_count_before: related_bits_before.len(),
            donor_main_count: donor_main_ids.len(),
            donor_subtables_merged,
            incident_regular_table_count: removed_ids.len(),
            incident_regular_row_count,
            merged_main_bit_count_before_pipeline: merged_main_before_pipeline.bit_count,
            merged_main_row_count_before_pipeline: merged_main_before_pipeline.row_count,
            pipeline_productive_round_count: pipeline_summary.productive_round_count,
            pipeline_round_count_including_final_check: pipeline_summary
                .round_count_including_final_check,
            regular_table_count_after: regular_after_pipeline_metrics.table_count,
            regular_bit_count_after: regular_after_pipeline_metrics.bit_count,
            regular_row_count_after: regular_after_pipeline_metrics.row_count,
            main_table_count_after: mains_after_drop_metrics.table_count,
            main_bit_count_after: mains_after_drop_metrics.bit_count,
            main_row_count_after: mains_after_drop_metrics.row_count,
        };
        iteration_reports.push(iteration_report);

        if iteration % args.checkpoint_every == 0 || iteration == args.iterations {
            write_json(&iteration_summaries_path, &iteration_reports)?;
            write_checkpoint(
                &checkpoint_regular_path,
                &checkpoint_mains_path,
                &checkpoint_combined_path,
                &checkpoint_exclude_bits_path,
                &checkpoint_meta_path,
                &regular_after_pipeline,
                &mains_after_drop,
                &processed_bits,
                &processed_central_bits,
                iteration_reports.len(),
            )?;
        }

        regular_tables = regular_after_pipeline;
        old_mains = mains_after_drop;
    }

    let mut final_combined = regular_tables.clone();
    final_combined.extend(old_mains.clone());
    let final_regular_path = args.output_root.join("final.regular.json");
    let final_mains_path = args.output_root.join("final.derived_mains.json");
    let final_combined_path = args.output_root.join("final.combined.json");
    let report_path = args.output_root.join("report.json");
    write_json(&final_regular_path, &regular_tables)?;
    write_json(&final_mains_path, &old_mains)?;
    write_json(&final_combined_path, &final_combined)?;

    let report = ChainReport {
        method: "Iteratively build a main-table chain from the current regular system. On each step: choose the non-origin, non-excluded bit with minimum current m2 related-bit count over regular tables; merge all incident regular tables into a new main; remove those regular tables; use m3 to collect donor mains sharing at least one related bit; project each donor main to donor.bits ∩ m2[central_bit] without mutating the donor and merge the projected subtable into the new main; run the standard pipeline on regular tables plus old mains plus the new main; resolve the mains back out of the pipeline output; remove the current central bit from every resolved main; drop empty or tautological mains after that projection; rebuild m1/m2/m3 from the new state; and continue.".to_string(),
        input_system: args.input_system.display().to_string(),
        input_derived_mains: args.input_derived_mains.display().to_string(),
        origins_input: args.origins.display().to_string(),
        iterations_requested: args.iterations,
        iterations_completed: iteration_reports.len(),
        checkpoint_every: args.checkpoint_every,
        selection_metric: "minimum m2 related-bit count over current regular tables, excluding origin bits and explicitly excluded/processed bits".to_string(),
        m2_update_policy: "after each pipeline run, m2 is recomputed from the resulting regular tables and then all processed central bits are removed from both keys and neighbor sets".to_string(),
        donor_projection_rule: "for each donor main selected through m3 over bits in m2[central_bit], project the donor to donor.bits ∩ m2[central_bit]; skip empty or tautological projections".to_string(),
        central_bits_excluded_initially: args.exclude_bits.iter().copied().collect(),
        central_bits_processed: processed_central_bits,
        initial_regular_metrics,
        initial_main_metrics,
        final_regular_metrics: collect_metrics(&regular_tables),
        final_main_metrics: collect_metrics(&old_mains),
        final_combined_metrics: collect_metrics(&final_combined),
        output_checkpoint_regular_tables: checkpoint_regular_path.display().to_string(),
        output_checkpoint_derived_mains: checkpoint_mains_path.display().to_string(),
        output_checkpoint_combined_system: checkpoint_combined_path.display().to_string(),
        output_checkpoint_exclude_bits: checkpoint_exclude_bits_path.display().to_string(),
        output_iteration_summaries: iteration_summaries_path.display().to_string(),
        output_final_regular_tables: final_regular_path.display().to_string(),
        output_final_derived_mains: final_mains_path.display().to_string(),
        output_final_combined_system: final_combined_path.display().to_string(),
        iterations: iteration_reports,
    };
    write_json(&report_path, &report)?;

    println!("iterations_completed={}", report.iterations_completed);
    println!(
        "central_bits_processed={}",
        report
            .central_bits_processed
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "final_regular_tables={}",
        report.final_regular_metrics.table_count
    );
    println!(
        "final_regular_bits={}",
        report.final_regular_metrics.bit_count
    );
    println!(
        "final_regular_rows={}",
        report.final_regular_metrics.row_count
    );
    println!(
        "final_main_tables={}",
        report.final_main_metrics.table_count
    );
    println!("final_main_bits={}", report.final_main_metrics.bit_count);
    println!("final_main_rows={}", report.final_main_metrics.row_count);
    println!("output_root={}", args.output_root.display());

    if work_dir.exists() {
        fs::remove_dir_all(&work_dir)
            .with_context(|| format!("failed to remove {}", work_dir.display()))?;
    }

    Ok(())
}

fn canonicalize_tables(tables: Vec<Table>) -> Vec<Table> {
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

fn read_bits_file(path: &Path) -> Result<BTreeSet<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let bits = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(bits)
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

fn summarize_main(table: &Table) -> MainSummary {
    MainSummary {
        bit_count: table.bits.len(),
        row_count: table.rows.len(),
        bits: table.bits.clone(),
        is_tautology: is_full_row_set(table.rows.len(), table.bits.len()),
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

fn remove_processed_bits_from_m2(
    m2: BTreeMap<u32, BTreeSet<u32>>,
    processed_bits: &BTreeSet<u32>,
) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut filtered = BTreeMap::new();
    for (bit, related_bits) in m2 {
        if processed_bits.contains(&bit) {
            continue;
        }
        let filtered_related: BTreeSet<u32> = related_bits
            .into_iter()
            .filter(|related_bit| !processed_bits.contains(related_bit))
            .collect();
        filtered.insert(bit, filtered_related);
    }
    filtered
}

fn select_next_central_bit(
    m1: &BTreeMap<u32, BTreeSet<usize>>,
    m2: &BTreeMap<u32, BTreeSet<u32>>,
    origins: &BTreeSet<u32>,
) -> Result<(u32, Vec<u32>)> {
    m2.iter()
        .filter(|(bit, related_bits)| {
            !origins.contains(bit)
                && m1
                    .get(bit)
                    .map(|tables| !tables.is_empty())
                    .unwrap_or(false)
                && !related_bits.is_empty()
        })
        .min_by(|(left_bit, left_related), (right_bit, right_related)| {
            left_related
                .len()
                .cmp(&right_related.len())
                .then_with(|| left_bit.cmp(right_bit))
        })
        .map(|(&bit, related_bits)| (bit, related_bits.iter().copied().collect()))
        .context("no eligible central bit found")
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

fn remove_table_ids(tables: &[Table], removed_ids: &BTreeSet<usize>) -> Vec<Table> {
    tables
        .iter()
        .enumerate()
        .filter(|(table_id, _)| !removed_ids.contains(table_id))
        .map(|(_, table)| table.clone())
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

fn donor_main_ids(m3: &BTreeMap<u32, BTreeSet<usize>>, related_bits: &[u32]) -> Vec<usize> {
    let mut donors = BTreeSet::new();
    for bit in related_bits {
        if let Some(main_ids) = m3.get(bit) {
            donors.extend(main_ids.iter().copied());
        }
    }
    donors.into_iter().collect()
}

fn project_table_to_allowed_bits(
    table: &Table,
    allowed_bits: &BTreeSet<u32>,
) -> Result<Option<Table>> {
    let mut projected_bits = Vec::new();
    let mut projected_indices = Vec::new();
    for (index, &bit) in table.bits.iter().enumerate() {
        if allowed_bits.contains(&bit) {
            projected_bits.push(bit);
            projected_indices.push(index);
        }
    }

    if projected_bits.is_empty() {
        return Ok(None);
    }

    let mut projected_rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    if projected_rows.is_empty() {
        bail!(
            "projection to bits {:?} produced an empty subtable",
            projected_bits
        );
    }
    if is_full_row_set(projected_rows.len(), projected_bits.len()) {
        return Ok(None);
    }

    Ok(Some(Table {
        bits: projected_bits,
        rows: projected_rows,
    }))
}

fn separate_regular_and_mains(
    combined_system: &[Table],
    input_mains: &[Table],
) -> Result<(Vec<Table>, Vec<Table>)> {
    let resolved_mains = resolve_main_targets_in_system(combined_system, input_mains)?;
    let regular_tables = remove_unique_exact_tables(combined_system, &resolved_mains)?;
    Ok((regular_tables, resolved_mains))
}

fn resolve_main_targets_in_system(system: &[Table], targets: &[Table]) -> Result<Vec<Table>> {
    let mut resolved = Vec::with_capacity(targets.len());
    for target in targets {
        if let Ok(table) = resolve_main_in_tables(system, &target.bits) {
            resolved.push(table);
        }
    }
    Ok(resolved)
}

fn resolve_main_in_tables(system: &[Table], target_bits: &[u32]) -> Result<Table> {
    match resolve_main_from_tables(system, target_bits) {
        Ok(table) => Ok(table),
        Err(exact_error) => {
            let system_bits: BTreeSet<u32> = system
                .iter()
                .flat_map(|table| table.bits.iter().copied())
                .collect();
            let surviving_bits = target_bits
                .iter()
                .copied()
                .filter(|bit| system_bits.contains(bit))
                .collect::<Vec<_>>();
            if surviving_bits.len() < target_bits.len() {
                if let Ok(table) = resolve_main_from_tables(system, &surviving_bits) {
                    return Ok(table);
                }
            }

            resolve_main_by_overlap(system, target_bits).with_context(|| {
                format!(
                    "failed exact resolution for {:?}; also failed after retrying with surviving bits {:?}: {exact_error}",
                    target_bits, surviving_bits
                )
            })
        }
    }
}

fn resolve_main_from_tables(system: &[Table], target_bits: &[u32]) -> Result<Table> {
    let mut matches = system
        .iter()
        .filter(|table| table.bits == target_bits)
        .cloned()
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => bail!("no table with bits {:?}", target_bits),
        count => bail!(
            "expected exactly one table with bits {:?}, found {}",
            target_bits,
            count
        ),
    }
}

fn resolve_main_by_overlap(system: &[Table], target_bits: &[u32]) -> Result<Table> {
    let target_set: BTreeSet<u32> = target_bits.iter().copied().collect();
    let mut best: Option<(usize, usize, usize, usize, Table)> = None;

    for table in system {
        let shared = table
            .bits
            .iter()
            .filter(|bit| target_set.contains(bit))
            .count();
        if shared == 0 {
            continue;
        }

        let outside = table.bits.len().saturating_sub(shared);
        let missing = target_set.len().saturating_sub(shared);
        let subset_preference = usize::from(outside == 0);
        let candidate = (
            subset_preference,
            shared,
            usize::MAX - outside,
            table.rows.len(),
            table.clone(),
        );

        let replace = match &best {
            None => true,
            Some(current) => {
                let current_missing = target_set.len().saturating_sub(current.1);
                candidate.0 > current.0
                    || (candidate.0 == current.0 && candidate.1 > current.1)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 > current.2)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 == current.2
                        && missing < current_missing)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 == current.2
                        && missing == current_missing
                        && candidate.3 > current.3)
            }
        };

        if replace {
            best = Some(candidate);
        }
    }

    best.map(|(_, _, _, _, table)| table).ok_or_else(|| {
        anyhow!(
            "no overlap-based main candidate found for bits {:?}",
            target_bits
        )
    })
}

fn remove_unique_exact_tables(system: &[Table], resolved_mains: &[Table]) -> Result<Vec<Table>> {
    let unique_mains = unique_exact_tables(resolved_mains);
    let mut remaining = system.to_vec();
    for main in unique_mains {
        let Some(index) = remaining.iter().position(|table| table == &main) else {
            bail!("resolved main {:?} is absent from the system", main.bits);
        };
        remaining.remove(index);
    }
    Ok(remaining)
}

fn unique_exact_tables(tables: &[Table]) -> Vec<Table> {
    let mut unique = Vec::new();
    for table in tables {
        if !unique.iter().any(|existing: &Table| existing == table) {
            unique.push(table.clone());
        }
    }
    unique
}

fn drop_bit_from_all_mains(mains: &[Table], central_bit: u32) -> Result<Vec<Table>> {
    let mut dropped = Vec::new();
    for main in mains {
        if let Some(projected) = drop_bit_from_main(main.clone(), central_bit)? {
            dropped.push(projected);
        }
    }

    let canonicalized = canonicalize_tables(dropped);
    let (by_bits, _) = collapse_equal_bitsets(&canonicalized);
    Ok(to_tables(&by_bits))
}

fn drop_bit_from_main(main: Table, central_bit: u32) -> Result<Option<Table>> {
    let mut projected_bits = Vec::with_capacity(main.bits.len().saturating_sub(1));
    let mut projected_indices = Vec::with_capacity(main.bits.len().saturating_sub(1));

    for (index, &bit) in main.bits.iter().enumerate() {
        if bit == central_bit {
            continue;
        }
        projected_bits.push(bit);
        projected_indices.push(index);
    }

    if projected_bits.len() == main.bits.len() {
        return Ok(Some(main));
    }

    let mut projected_rows: Vec<u32> = main
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    if projected_bits.is_empty() {
        return Ok(None);
    }
    if projected_rows.is_empty() || is_full_row_set(projected_rows.len(), projected_bits.len()) {
        return Ok(None);
    }

    Ok(Some(Table {
        bits: projected_bits,
        rows: projected_rows,
    }))
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
    for attempt in 1..=3 {
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
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if !stderr.is_empty() { stderr } else { stdout };
            bail!("pipeline failed: {detail}");
        }

        let output_ok = validate_tables_file(output_path).is_ok();
        let report_ok = validate_json_file(report_path).is_ok();
        if output_ok && report_ok {
            return Ok(());
        }

        if attempt == 3 {
            validate_tables_file(output_path)?;
            validate_json_file(report_path)?;
        }
    }

    bail!("unreachable pipeline validation failure")
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
        "usage: cargo run --release --bin build_min_related_main_chain -- [--input-system <path>] [--input-derived-mains <path>] [--origins <path>] [--output-root <dir>] [--pipeline-exe <path>] [--iterations <n>] [--checkpoint-every <n>] [--exclude-bit <bit>]... [--exclude-bits-file <path>] [--clear-exclude-bits] [--max-rounds <n>] [--disable-zero-collapse-bit-filter]"
    );
}

fn validate_tables_file(path: &Path) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.contains(&0) {
        bail!("NUL byte found in {}", path.display());
    }
    let _: Vec<Table> = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(())
}

fn validate_json_file(path: &Path) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.contains(&0) {
        bail!("NUL byte found in {}", path.display());
    }
    let _: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(())
}

fn write_checkpoint(
    regular_path: &Path,
    mains_path: &Path,
    combined_path: &Path,
    exclude_bits_path: &Path,
    meta_path: &Path,
    regular_tables: &[Table],
    mains: &[Table],
    processed_bits: &BTreeSet<u32>,
    processed_central_bits: &[u32],
    iterations_completed: usize,
) -> Result<()> {
    let mut combined = regular_tables.to_vec();
    combined.extend_from_slice(mains);

    write_json(regular_path, regular_tables)?;
    write_json(mains_path, mains)?;
    write_json(combined_path, &combined)?;
    write_json(exclude_bits_path, processed_bits)?;

    let meta = serde_json::json!({
        "iterations_completed": iterations_completed,
        "processed_bit_count": processed_bits.len(),
        "processed_central_bits": processed_central_bits,
        "regular_metrics": collect_metrics(regular_tables),
        "main_metrics": collect_metrics(mains),
        "combined_metrics": collect_metrics(&combined),
    });
    write_json(meta_path, &meta)?;
    Ok(())
}

fn write_tables_atomically_validated(path: &Path, tables: &[Table]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut bytes = serde_json::to_vec_pretty(tables)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    bytes.push(b'\n');
    let tmp_path = path.with_extension("json.tmp");

    for attempt in 1..=3 {
        fs::write(&tmp_path, &bytes)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        let roundtrip = fs::read(&tmp_path)
            .with_context(|| format!("failed to read {}", tmp_path.display()))?;
        if roundtrip == bytes && !roundtrip.contains(&0) {
            let _: Vec<Table> = serde_json::from_slice(&roundtrip)
                .with_context(|| format!("failed to reparse {}", tmp_path.display()))?;
            fs::rename(&tmp_path, path).with_context(|| {
                format!(
                    "failed to rename {} to {}",
                    tmp_path.display(),
                    path.display()
                )
            })?;
            return Ok(());
        }
        if attempt == 3 {
            bail!(
                "failed to persist a valid JSON table file after {} attempts: {}",
                attempt,
                path.display()
            );
        }
    }

    bail!(
        "unreachable write validation failure for {}",
        path.display()
    )
}
