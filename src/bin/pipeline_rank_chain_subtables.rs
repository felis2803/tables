use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tables::common::{
    for_each_combination, is_full_row_set, project_row, read_tables, write_json, write_tables,
    Table,
};
use tables::rank_stats::compute_rank;
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    output_root: PathBuf,
    pipeline_exe: PathBuf,
    target_arity: usize,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/originals.tables"),
            output_root: PathBuf::from("runs/2026-04-22-pipeline-rank-chain-subtables"),
            pipeline_exe: PathBuf::from("target/release/tables.exe"),
            target_arity: 16,
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
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--pipeline-exe" => {
                    args.pipeline_exe = PathBuf::from(expect_value(&mut iter, "--pipeline-exe")?)
                }
                "--target-arity" => {
                    args.target_arity = expect_value(&mut iter, "--target-arity")?
                        .parse()
                        .with_context(|| "invalid value for --target-arity")?;
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

        if args.target_arity == 0 || args.target_arity > 32 {
            bail!(
                "--target-arity must be in 1..=32 for the current u32 table representation, got {}",
                args.target_arity
            );
        }

        Ok(args)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PipelineSummary {
    productive_round_count: usize,
    round_count_including_final_check: usize,
    final_table_count: usize,
    final_bit_count: usize,
    final_row_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct PipelineArtifacts {
    output_tables: String,
    report: String,
    forced: String,
    mapping: String,
    components: String,
    dropped: String,
    relations: String,
    nodes: String,
    summary: PipelineSummary,
}

#[derive(Clone, Debug, Serialize)]
struct TableSummary {
    bits: Vec<u32>,
    bit_count: usize,
    row_count: usize,
    rank: f64,
}

#[derive(Clone, Debug, Serialize)]
struct BaseTableReport {
    table_index: usize,
    summary: TableSummary,
}

#[derive(Clone, Debug, Serialize)]
struct ChainStepReport {
    step: usize,
    partner_table_index: usize,
    partner_summary: TableSummary,
    shared_bits: Vec<u32>,
    disjoint: bool,
    before: TableSummary,
    after: TableSummary,
}

#[derive(Clone, Debug, Serialize)]
struct ChainReport {
    pool_tables_path: String,
    path_tables_path: String,
    final_table_path: String,
    table_count: usize,
    base: BaseTableReport,
    step_count: usize,
    stop_reason: String,
    used_table_indices: Vec<usize>,
    final_summary: TableSummary,
    steps: Vec<ChainStepReport>,
}

#[derive(Clone, Debug, Serialize)]
struct SubtableArtifacts {
    two_bit_all_path: String,
    two_bit_non_taut_path: String,
    three_bit_all_path: Option<String>,
    three_bit_non_taut_path: Option<String>,
    four_bit_all_path: Option<String>,
    four_bit_non_taut_path: Option<String>,
    two_bit_all_count: usize,
    two_bit_tautology_count: usize,
    two_bit_non_taut_count: usize,
    three_bit_all_count: Option<usize>,
    three_bit_tautology_count: Option<usize>,
    three_bit_non_taut_count: Option<usize>,
    four_bit_all_count: Option<usize>,
    four_bit_tautology_count: Option<usize>,
    four_bit_non_taut_count: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct ReconstructionCheck {
    name: String,
    factors_path: String,
    reconstructed_path: String,
    factor_count: usize,
    factor_arity_distribution: BTreeMap<String, usize>,
    factor_tautology_count: usize,
    reconstructed_summary: Option<TableSummary>,
    matches_source: bool,
}

#[derive(Clone, Debug, Serialize)]
struct RunReport {
    method: String,
    input: String,
    output_root: String,
    pipeline_exe: String,
    target_arity: usize,
    pipeline: PipelineArtifacts,
    chain: ChainReport,
    subtables: SubtableArtifacts,
    reconstruction_checks: Vec<ReconstructionCheck>,
}

#[derive(Clone)]
struct PartnerChoice {
    partner_index: usize,
    merged: Table,
    shared_bits: Vec<u32>,
    disjoint: bool,
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    std::fs::create_dir_all(&args.output_root)
        .with_context(|| format!("failed to create {}", args.output_root.display()))?;

    let pipeline = run_pipeline(&args)?;
    let pool_tables = load_canonical_tables(Path::new(&pipeline.output_tables))?;
    if pool_tables.is_empty() {
        bail!("pipeline produced no tables");
    }

    let chain_dir = args.output_root.join("rank-chain");
    std::fs::create_dir_all(&chain_dir)
        .with_context(|| format!("failed to create {}", chain_dir.display()))?;
    let path_tables_path = chain_dir.join("path.tables");
    let final_table_path = chain_dir.join("final.tables");
    let canonical_pool_path = chain_dir.join("pool.after_pipeline.tables");

    write_tables(&canonical_pool_path, &pool_tables)?;

    let (final_table, path_tables, chain) = run_rank_chain(
        &pool_tables,
        args.target_arity,
        &canonical_pool_path,
        &path_tables_path,
        &final_table_path,
    )?;

    write_tables(&path_tables_path, &path_tables)?;
    write_tables(&final_table_path, std::slice::from_ref(&final_table))?;

    let subtables_dir = args.output_root.join("subtables");
    std::fs::create_dir_all(&subtables_dir)
        .with_context(|| format!("failed to create {}", subtables_dir.display()))?;
    let reconstruction_dir = args.output_root.join("reconstruction");
    std::fs::create_dir_all(&reconstruction_dir)
        .with_context(|| format!("failed to create {}", reconstruction_dir.display()))?;

    let two_bit_all = extract_subtables(&final_table, 2);
    let two_bit_all_path = subtables_dir.join("two_bit.all.tables");
    write_tables(&two_bit_all_path, &two_bit_all)?;

    let two_bit_non_taut: Vec<Table> = two_bit_all
        .iter()
        .filter(|table| !is_full_row_set(table.rows.len(), table.bits.len()))
        .cloned()
        .collect();
    let two_bit_non_taut_path = subtables_dir.join("two_bit.non_taut.tables");
    write_tables(&two_bit_non_taut_path, &two_bit_non_taut)?;

    let mut reconstruction_checks = Vec::new();
    let mut three_bit_all_path = None;
    let mut three_bit_non_taut_path = None;
    let mut four_bit_all_path = None;
    let mut four_bit_non_taut_path = None;
    let mut three_bit_all_count = None;
    let mut three_bit_tautology_count = None;
    let mut three_bit_non_taut_count = None;
    let mut four_bit_all_count = None;
    let mut four_bit_tautology_count = None;
    let mut four_bit_non_taut_count = None;

    let pool_2_path = subtables_dir.join("pool.2.tables");
    write_tables(&pool_2_path, &two_bit_non_taut)?;
    let recon_2_path = reconstruction_dir.join("reconstructed.from_2.tables");
    let recon_2 = reconstruct_from_factors(
        "2",
        &final_table,
        &two_bit_non_taut,
        &pool_2_path,
        &recon_2_path,
    )?;
    let mut reconstructed = recon_2.matches_source;
    reconstruction_checks.push(recon_2);

    if !reconstructed {
        let three_bit_all = extract_subtables(&final_table, 3);
        let path = subtables_dir.join("three_bit.all.tables");
        write_tables(&path, &three_bit_all)?;
        three_bit_all_count = Some(three_bit_all.len());
        three_bit_all_path = Some(path.to_string_lossy().into_owned());
        let three_bit_non_taut: Vec<Table> = three_bit_all
            .iter()
            .filter(|table| !is_full_row_set(table.rows.len(), table.bits.len()))
            .cloned()
            .collect();
        let non_taut_path = subtables_dir.join("three_bit.non_taut.tables");
        write_tables(&non_taut_path, &three_bit_non_taut)?;
        three_bit_non_taut_path = Some(non_taut_path.to_string_lossy().into_owned());
        three_bit_tautology_count =
            Some(three_bit_all.len().saturating_sub(three_bit_non_taut.len()));
        three_bit_non_taut_count = Some(three_bit_non_taut.len());

        let mut pool_2_3 = two_bit_non_taut.clone();
        pool_2_3.extend(three_bit_non_taut.iter().cloned());
        let pool_2_3_path = subtables_dir.join("pool.2_3.tables");
        write_tables(&pool_2_3_path, &pool_2_3)?;
        let recon_2_3_path = reconstruction_dir.join("reconstructed.from_2_3.tables");
        let recon_2_3 = reconstruct_from_factors(
            "2+3",
            &final_table,
            &pool_2_3,
            &pool_2_3_path,
            &recon_2_3_path,
        )?;
        reconstructed = recon_2_3.matches_source;
        reconstruction_checks.push(recon_2_3);

        if !reconstructed {
            let four_bit_all = extract_subtables(&final_table, 4);
            let path = subtables_dir.join("four_bit.all.tables");
            write_tables(&path, &four_bit_all)?;
            four_bit_all_count = Some(four_bit_all.len());
            four_bit_all_path = Some(path.to_string_lossy().into_owned());
            let four_bit_non_taut: Vec<Table> = four_bit_all
                .iter()
                .filter(|table| !is_full_row_set(table.rows.len(), table.bits.len()))
                .cloned()
                .collect();
            let non_taut_path = subtables_dir.join("four_bit.non_taut.tables");
            write_tables(&non_taut_path, &four_bit_non_taut)?;
            four_bit_non_taut_path = Some(non_taut_path.to_string_lossy().into_owned());
            four_bit_tautology_count =
                Some(four_bit_all.len().saturating_sub(four_bit_non_taut.len()));
            four_bit_non_taut_count = Some(four_bit_non_taut.len());

            let mut pool_2_3_4 = pool_2_3;
            pool_2_3_4.extend(four_bit_non_taut.iter().cloned());
            let pool_2_3_4_path = subtables_dir.join("pool.2_3_4.tables");
            write_tables(&pool_2_3_4_path, &pool_2_3_4)?;
            let recon_2_3_4_path = reconstruction_dir.join("reconstructed.from_2_3_4.tables");
            let recon_2_3_4 = reconstruct_from_factors(
                "2+3+4",
                &final_table,
                &pool_2_3_4,
                &pool_2_3_4_path,
                &recon_2_3_4_path,
            )?;
            reconstruction_checks.push(recon_2_3_4);
        }
    }

    let subtables = SubtableArtifacts {
        two_bit_all_path: two_bit_all_path.to_string_lossy().into_owned(),
        two_bit_non_taut_path: two_bit_non_taut_path.to_string_lossy().into_owned(),
        three_bit_all_path,
        three_bit_non_taut_path,
        four_bit_all_path,
        four_bit_non_taut_path,
        two_bit_all_count: two_bit_all.len(),
        two_bit_tautology_count: two_bit_all.len().saturating_sub(two_bit_non_taut.len()),
        two_bit_non_taut_count: two_bit_non_taut.len(),
        three_bit_all_count,
        three_bit_tautology_count,
        three_bit_non_taut_count,
        four_bit_all_count,
        four_bit_tautology_count,
        four_bit_non_taut_count,
    };

    let report = RunReport {
        method: "Run the baseline fixed-point pipeline on the input .tables system, choose the pipeline table with minimum rank as the seed, repeatedly merge it with the distinct unused pipeline table whose exact merge yields the minimum rank until the merged table reaches arity >= target_arity, extract exact 2-bit projections, drop tautological projections before each join stage, then test exact reconstruction by natural-joining pools 2, 2+3, and finally 2+3+4 when needed.".to_string(),
        input: path_string(&args.input),
        output_root: path_string(&args.output_root),
        pipeline_exe: path_string(&args.pipeline_exe),
        target_arity: args.target_arity,
        pipeline,
        chain,
        subtables,
        reconstruction_checks,
    };
    let report_path = args.output_root.join("report.json");
    write_json(&report_path, &report)?;

    println!(
        "pipeline_tables={} pipeline_rows={} pipeline_rounds={}",
        report.pipeline.summary.final_table_count,
        report.pipeline.summary.final_row_count,
        report.pipeline.summary.productive_round_count
    );
    println!(
        "chain_final_arity={} chain_final_rows={} chain_final_rank={:.12}",
        report.chain.final_summary.bit_count,
        report.chain.final_summary.row_count,
        report.chain.final_summary.rank
    );
    for check in &report.reconstruction_checks {
        println!(
            "reconstruction[{}]: factors={} matches_source={}",
            check.name, check.factor_count, check.matches_source
        );
    }
    println!("report={}", report_path.display());
    Ok(())
}

fn run_pipeline(args: &Args) -> Result<PipelineArtifacts> {
    let pipeline_dir = args.output_root.join("after-pipeline");
    std::fs::create_dir_all(&pipeline_dir)
        .with_context(|| format!("failed to create {}", pipeline_dir.display()))?;

    let output_tables = pipeline_dir.join("system.after_pipeline.tables");
    let report = pipeline_dir.join("report.after_pipeline.json");
    let forced = pipeline_dir.join("bits.after_pipeline.forced.json");
    let mapping = pipeline_dir.join("bits.after_pipeline.rewrite_map.json");
    let components = pipeline_dir.join("bits.after_pipeline.components.json");
    let dropped = pipeline_dir.join("tables.after_pipeline.dropped_included.json");
    let relations = pipeline_dir.join("pairs.after_pipeline.relations.json");
    let nodes = pipeline_dir.join("nodes.after_pipeline.json");

    let mut command = Command::new(&args.pipeline_exe);
    command
        .arg("--input")
        .arg(&args.input)
        .arg("--output")
        .arg(&output_tables)
        .arg("--report")
        .arg(&report)
        .arg("--forced")
        .arg(&forced)
        .arg("--mapping")
        .arg(&mapping)
        .arg("--components")
        .arg(&components)
        .arg("--dropped")
        .arg(&dropped)
        .arg("--relations")
        .arg(&relations)
        .arg("--nodes")
        .arg(&nodes);

    if let Some(max_rounds) = args.max_rounds {
        command.arg("--max-rounds").arg(max_rounds.to_string());
    }
    if args.disable_zero_collapse_bit_filter {
        command.arg("--disable-zero-collapse-bit-filter");
    }

    let output = command
        .output()
        .with_context(|| format!("failed to run {}", args.pipeline_exe.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        bail!("pipeline failed: {detail}");
    }

    let summary = read_json::<PipelineSummary>(&report)?;
    Ok(PipelineArtifacts {
        output_tables: path_string(&output_tables),
        report: path_string(&report),
        forced: path_string(&forced),
        mapping: path_string(&mapping),
        components: path_string(&components),
        dropped: path_string(&dropped),
        relations: path_string(&relations),
        nodes: path_string(&nodes),
        summary,
    })
}

fn run_rank_chain(
    tables: &[Table],
    target_arity: usize,
    canonical_pool_path: &Path,
    path_tables_path: &Path,
    final_table_path: &Path,
) -> Result<(Table, Vec<Table>, ChainReport)> {
    if tables.is_empty() {
        bail!("rank chain requires at least one table");
    }

    let base_index = choose_best_base_table(tables)
        .context("failed to choose the minimum-rank base table for the chain")?;
    let bit_to_tables = build_bit_to_tables(tables);

    let mut used = vec![false; tables.len()];
    used[base_index] = true;
    let mut used_indices = vec![base_index];
    let mut current = tables[base_index].clone();
    let mut path_tables = vec![current.clone()];
    let mut steps = Vec::new();
    let stop_reason: String;

    loop {
        if current.bits.len() >= target_arity {
            stop_reason = format!("reached target arity {}", current.bits.len());
            break;
        }

        let before = current.clone();
        let Some(choice) = find_best_partner(&current, tables, &bit_to_tables, &used)? else {
            stop_reason = format!(
                "no distinct unused table yields a non-empty exact merge above arity {}",
                current.bits.len()
            );
            break;
        };

        used[choice.partner_index] = true;
        used_indices.push(choice.partner_index);
        current = choice.merged.clone();
        path_tables.push(current.clone());

        steps.push(ChainStepReport {
            step: steps.len() + 1,
            partner_table_index: choice.partner_index,
            partner_summary: summarize_table(&tables[choice.partner_index]),
            shared_bits: choice.shared_bits,
            disjoint: choice.disjoint,
            before: summarize_table(&before),
            after: summarize_table(&current),
        });
    }

    used_indices.sort_unstable();
    let chain = ChainReport {
        pool_tables_path: path_string(canonical_pool_path),
        path_tables_path: path_string(path_tables_path),
        final_table_path: path_string(final_table_path),
        table_count: tables.len(),
        base: BaseTableReport {
            table_index: base_index,
            summary: summarize_table(&tables[base_index]),
        },
        step_count: steps.len(),
        stop_reason,
        used_table_indices: used_indices,
        final_summary: summarize_table(&current),
        steps,
    };

    Ok((current, path_tables, chain))
}

fn choose_best_base_table(tables: &[Table]) -> Option<usize> {
    tables.iter().enumerate().min_by(|(left_index, left), (right_index, right)| {
        let left_summary = summarize_table(left);
        let right_summary = summarize_table(right);
        left_summary
            .rank
            .total_cmp(&right_summary.rank)
            .then_with(|| left_summary.row_count.cmp(&right_summary.row_count))
            .then_with(|| left_summary.bit_count.cmp(&right_summary.bit_count))
            .then_with(|| left_index.cmp(right_index))
    })
    .map(|(index, _)| index)
}

fn build_bit_to_tables(tables: &[Table]) -> HashMap<u32, Vec<usize>> {
    let mut bit_to_tables: HashMap<u32, Vec<usize>> = HashMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }
    bit_to_tables
}

fn find_best_partner(
    current: &Table,
    tables: &[Table],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    used: &[bool],
) -> Result<Option<PartnerChoice>> {
    let mut overlapping = HashSet::new();
    for &bit in &current.bits {
        if let Some(indices) = bit_to_tables.get(&bit) {
            for &index in indices {
                if !used[index] {
                    overlapping.insert(index);
                }
            }
        }
    }

    let mut best: Option<(f64, usize, usize, usize, Table, Vec<u32>, bool)> = None;

    for candidate_index in overlapping.iter().copied() {
        let merged = merge_exact(current, &tables[candidate_index])?;
        if merged.rows.is_empty() {
            continue;
        }
        let rank = compute_rank(merged.rows.len(), merged.bits.len());
        let shared = shared_bits(&current.bits, &tables[candidate_index].bits);
        let candidate = (
            rank,
            merged.rows.len(),
            merged.bits.len(),
            candidate_index,
            merged,
            shared,
            false,
        );
        if best
            .as_ref()
            .is_none_or(|best_candidate| compare_partner_candidate(&candidate, best_candidate).is_lt())
        {
            best = Some(candidate);
        }
    }

    for (candidate_index, table) in tables.iter().enumerate() {
        if used[candidate_index] || overlapping.contains(&candidate_index) {
            continue;
        }
        let merged_rows = current
            .rows
            .len()
            .checked_mul(table.rows.len())
            .with_context(|| {
                format!(
                    "disjoint row-count overflow while evaluating partner {}",
                    candidate_index
                )
            })?;
        let merged_arity = current.bits.len() + table.bits.len();
        let rank = compute_rank(merged_rows, merged_arity);
        let merged = merge_exact(current, table)?;
        let candidate = (
            rank,
            merged_rows,
            merged_arity,
            candidate_index,
            merged,
            Vec::new(),
            true,
        );
        if best
            .as_ref()
            .is_none_or(|best_candidate| compare_partner_candidate(&candidate, best_candidate).is_lt())
        {
            best = Some(candidate);
        }
    }

    Ok(best.map(
        |(_, _, _, partner_index, merged, shared_bits, disjoint)| PartnerChoice {
            partner_index,
            merged,
            shared_bits,
            disjoint,
        },
    ))
}

fn compare_partner_candidate(
    left: &(f64, usize, usize, usize, Table, Vec<u32>, bool),
    right: &(f64, usize, usize, usize, Table, Vec<u32>, bool),
) -> std::cmp::Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(&right.2))
        .then_with(|| left.3.cmp(&right.3))
}

fn extract_subtables(source: &Table, subtable_arity: usize) -> Vec<Table> {
    if subtable_arity == 0 || subtable_arity > source.bits.len() {
        return Vec::new();
    }

    let mut subtables = Vec::new();
    for_each_combination(source.bits.len(), subtable_arity, |indices| {
        let bits: Vec<u32> = indices.iter().map(|&index| source.bits[index]).collect();
        let mut rows: Vec<u32> = source
            .rows
            .iter()
            .copied()
            .map(|row| project_row(row, indices))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        subtables.push(Table { bits, rows });
    });
    subtables
}

fn reconstruct_from_factors(
    name: &str,
    source: &Table,
    factors: &[Table],
    factors_path: &Path,
    output_path: &Path,
) -> Result<ReconstructionCheck> {
    let mut factor_arity_distribution = BTreeMap::new();
    let mut factor_tautology_count = 0usize;
    for factor in factors {
        *factor_arity_distribution
            .entry(factor.bits.len().to_string())
            .or_insert(0) += 1;
        if is_full_row_set(factor.rows.len(), factor.bits.len()) {
            factor_tautology_count += 1;
        }
    }

    let reconstructed = reconstruct_join(factors)?;
    if let Some(table) = reconstructed.as_ref() {
        write_tables(output_path, std::slice::from_ref(table))?;
    } else {
        write_tables(output_path, &[])?;
    }

    let matches_source = reconstructed
        .as_ref()
        .is_some_and(|table| table.bits == source.bits && table.rows == source.rows);
    Ok(ReconstructionCheck {
        name: name.to_string(),
        factors_path: path_string(factors_path),
        reconstructed_path: path_string(output_path),
        factor_count: factors.len(),
        factor_arity_distribution,
        factor_tautology_count,
        reconstructed_summary: reconstructed.as_ref().map(summarize_table),
        matches_source,
    })
}

fn reconstruct_join(factors: &[Table]) -> Result<Option<Table>> {
    let Some((first_index, _)) = factors
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| compare_initial_factor(left, right))
    else {
        return Ok(None);
    };

    let mut remaining: Vec<Table> = factors.to_vec();
    let mut current = remaining.swap_remove(first_index);

    while !remaining.is_empty() {
        let next_index = remaining
            .iter()
            .position(|factor| have_shared_bits(&current.bits, &factor.bits))
            .unwrap_or(0);
        let next = remaining.swap_remove(next_index);
        current = merge_exact(&current, &next)?;
        if current.rows.is_empty() {
            bail!("join of source projections became contradictory");
        }
    }

    Ok(Some(current))
}

fn compare_initial_factor(left: &Table, right: &Table) -> std::cmp::Ordering {
    right.bits
        .len()
        .cmp(&left.bits.len())
        .then_with(|| {
            compute_rank(left.rows.len(), left.bits.len())
                .total_cmp(&compute_rank(right.rows.len(), right.bits.len()))
        })
        .then_with(|| left.rows.len().cmp(&right.rows.len()))
        .then_with(|| left.bits.cmp(&right.bits))
}

fn merge_exact(left: &Table, right: &Table) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!(error))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn shared_bits(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut shared = Vec::new();

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    shared
}

fn have_shared_bits(left: &[u32], right: &[u32]) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }

    false
}

fn summarize_table(table: &Table) -> TableSummary {
    TableSummary {
        bits: table.bits.clone(),
        bit_count: table.bits.len(),
        row_count: table.rows.len(),
        rank: compute_rank(table.rows.len(), table.bits.len()),
    }
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

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
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
        "usage:\n  cargo run --release --bin pipeline_rank_chain_subtables -- [--input <system.tables>] [--output-root <dir>] [--pipeline-exe <path>] [--target-arity <n>] [--max-rounds <n>] [--disable-zero-collapse-bit-filter]"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        extract_subtables, reconstruct_join, run_rank_chain, summarize_table, Table, TableSummary,
    };

    fn table(bits: &[u32], rows: &[u32]) -> Table {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
        rows.dedup();
        Table {
            bits: bits.to_vec(),
            rows,
        }
    }

    fn assert_summary(summary: &TableSummary, bit_count: usize, row_count: usize) {
        assert_eq!(summary.bit_count, bit_count);
        assert_eq!(summary.row_count, row_count);
    }

    #[test]
    fn extracts_all_two_bit_subtables() {
        let source = table(&[10, 20, 30], &[0b000, 0b011, 0b101, 0b110]);
        let subtables = extract_subtables(&source, 2);
        assert_eq!(subtables.len(), 3);
        assert_eq!(subtables[0].bits, vec![10, 20]);
        assert_eq!(subtables[1].bits, vec![10, 30]);
        assert_eq!(subtables[2].bits, vec![20, 30]);
    }

    #[test]
    fn reconstruct_join_from_exact_projection_factors() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b100, 0b111]);
        let factors = vec![
            table(&[1, 2], &[0b00, 0b11]),
            table(&[2, 3], &[0b00, 0b01, 0b10, 0b11]),
        ];
        let reconstructed = reconstruct_join(&factors).unwrap().unwrap();
        assert_eq!(reconstructed, source);
    }

    #[test]
    fn parity_needs_three_bit_factor_after_two_bit_tautologies_are_removed() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let two_bit_non_taut: Vec<Table> = extract_subtables(&source, 2)
            .into_iter()
            .filter(|factor| factor.rows.len() != 4)
            .collect();
        assert!(two_bit_non_taut.is_empty());

        let three_bit = extract_subtables(&source, 3);
        let reconstructed = reconstruct_join(&three_bit).unwrap().unwrap();
        assert_eq!(reconstructed, source);
    }

    #[test]
    fn rank_chain_starts_from_lowest_rank_table() {
        let tables = vec![
            table(&[1, 2, 3], &[0b000, 0b111]),
            table(&[4, 5], &[0b00, 0b01, 0b10]),
            table(&[2, 6], &[0b00, 0b11]),
        ];
        let temp = std::env::temp_dir();
        let pool = temp.join("rank-chain-pool.tables");
        let path = temp.join("rank-chain-path.tables");
        let final_path = temp.join("rank-chain-final.tables");
        let (_final_table, _path_tables, report) =
            run_rank_chain(&tables, 4, &pool, &path, &final_path).unwrap();
        assert_eq!(report.base.table_index, 0);
        assert_summary(&report.base.summary, 3, 2);
    }

    #[test]
    fn summarize_table_reports_rank_inputs() {
        let summary = summarize_table(&table(&[1, 2], &[0b00, 0b11]));
        assert_summary(&summary, 2, 2);
    }
}
