#![recursion_limit = "256"]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{collect_bits, read_tables, total_rows, write_json, Table};
use tables::forced_bits::{
    collect_forced_bits_bitwise, propagate_forced_bits, ForcedBitsInfo, ForcedPropagationStats,
};
use tables::node_filter::{build_nodes, filter_tables_with_nodes, NodeFilterInfo};
use tables::pair_reduction::{
    build_rewrite_map, extract_relations, rewrite_tables, PairReductionInfo,
    PairReductionIterationInfo,
};
use tables::rank_stats::{summarize_table_ranks, RankSummary};
use tables::subset_absorption::{
    collapse_equal_bitsets, merge_subsets, prune_included_tables, to_tables, SubsetAbsorptionInfo,
};
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    target_arity: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from(
                "runs/2026-04-04-greedy-rank-no-pairwise-cycles/tables.fixed_point.json",
            ),
            report: PathBuf::from(
                "runs/2026-04-04-greedy-rank-no-pairwise-cycles/report.fixed_point.json",
            ),
            target_arity: 16,
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
                "--target-arity" => {
                    args.target_arity = expect_value(&mut iter, "--target-arity")?
                        .parse()
                        .with_context(|| "invalid value for --target-arity")?;
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

#[derive(Clone)]
struct TableEntry {
    table: Table,
    active: bool,
}

#[derive(Clone, Copy)]
struct PairCandidate {
    left_index: usize,
    right_index: usize,
    predicted_rows: usize,
    union_arity: usize,
    predicted_rank: f64,
}

impl Ord for PairCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .predicted_rank
            .total_cmp(&self.predicted_rank)
            .then_with(|| other.predicted_rows.cmp(&self.predicted_rows))
            .then_with(|| other.union_arity.cmp(&self.union_arity))
            .then_with(|| other.left_index.cmp(&self.left_index))
            .then_with(|| other.right_index.cmp(&self.right_index))
    }
}

impl PartialOrd for PairCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PairCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.left_index == other.left_index
            && self.right_index == other.right_index
            && self.predicted_rows == other.predicted_rows
            && self.union_arity == other.union_arity
            && self.predicted_rank.to_bits() == other.predicted_rank.to_bits()
    }
}

impl Eq for PairCandidate {}

#[derive(Clone, Serialize)]
struct GreedyStageInfo {
    candidate_pair_count: usize,
    build_queue_seconds: f64,
    stage_seconds: f64,
    step_count: usize,
    reached_target_arity: bool,
    max_created_arity: usize,
    final_created_table_arity: usize,
    final_created_table_rows: usize,
    final_created_table_rank: f64,
    final_created_table_bits: Vec<u32>,
}

#[derive(Clone, Serialize)]
struct NoPairwiseStageInfo {
    stage_seconds: f64,
    productive_rounds: usize,
    round_count_including_final_check: usize,
    output_table_count: usize,
    output_row_count: usize,
    output_rank_summary: RankSummary,
    total_subset_row_deletions: usize,
    total_dropped_included_tables: usize,
    total_forced_bits: usize,
    total_forced_occurrences: usize,
    total_forced_removed_rows: usize,
    total_pair_relations: usize,
    total_pair_replaced_bits: usize,
    total_node_row_deletions: usize,
    total_node_tightenings: usize,
}

#[derive(Clone, Serialize)]
struct CycleInfo {
    cycle: usize,
    input_table_count: usize,
    input_row_count: usize,
    greedy: GreedyStageInfo,
    no_pairwise: NoPairwiseStageInfo,
    output_table_count: usize,
    output_row_count: usize,
    changed: bool,
}

#[derive(Clone, Serialize)]
struct Report {
    method: String,
    input: String,
    output: String,
    target_arity: usize,
    cycle_count: usize,
    fixed_point_reached: bool,
    initial_table_count: usize,
    initial_row_count: usize,
    final_table_count: usize,
    final_row_count: usize,
    final_rank_summary: RankSummary,
    total_seconds: f64,
    cycles: Vec<CycleInfo>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let initial_tables = normalize_tables(&read_tables(&args.input)?);
    let initial_table_count = initial_tables.len();
    let initial_row_count = total_rows(&initial_tables);

    let started = Instant::now();
    let mut current_tables = initial_tables;
    let mut cycles = Vec::new();

    loop {
        let cycle_index = cycles.len() + 1;
        if cycle_index == 1 || cycle_index % 10 == 0 {
            println!(
                "cycle {} start: tables={} rows={}",
                cycle_index,
                current_tables.len(),
                total_rows(&current_tables)
            );
        }

        let cycle_input = current_tables.clone();
        let greedy = run_greedy_stage(current_tables, args.target_arity)?;
        let no_pairwise = run_no_pairwise_pipeline(greedy.output_tables)?;
        let next_tables = normalize_tables(&no_pairwise.output_tables);
        let changed = next_tables != cycle_input;

        let cycle_info = CycleInfo {
            cycle: cycle_index,
            input_table_count: cycle_input.len(),
            input_row_count: total_rows(&cycle_input),
            greedy: greedy.info,
            no_pairwise: no_pairwise.info,
            output_table_count: next_tables.len(),
            output_row_count: total_rows(&next_tables),
            changed,
        };
        cycles.push(cycle_info);

        if !changed {
            current_tables = next_tables;
            break;
        }

        current_tables = next_tables;
    }

    let total_seconds = started.elapsed().as_secs_f64();
    let report = Report {
        method: "Repeat outer cycles of greedy merge-by-minimum-predicted-rank under shared_assignment_histogram up to target arity, then run the reduction pipeline with pairwise_merge disabled until its own fixed point; stop when a full outer cycle no longer changes the active table set".to_string(),
        input: path_string(&args.input),
        output: path_string(&args.output),
        target_arity: args.target_arity,
        cycle_count: cycles.len(),
        fixed_point_reached: cycles.last().is_some_and(|cycle| !cycle.changed),
        initial_table_count,
        initial_row_count,
        final_table_count: current_tables.len(),
        final_row_count: total_rows(&current_tables),
        final_rank_summary: summarize_table_ranks(&current_tables, 10),
        total_seconds,
        cycles,
    };

    write_json(&args.output, &current_tables)?;
    write_json(&args.report, &report)?;

    println!("cycles: {}", report.cycle_count);
    println!("final tables: {}", report.final_table_count);
    println!("final rows: {}", report.final_row_count);
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

struct GreedyStageResult {
    output_tables: Vec<Table>,
    info: GreedyStageInfo,
}

fn run_greedy_stage(input_tables: Vec<Table>, target_arity: usize) -> Result<GreedyStageResult> {
    let mut entries: Vec<TableEntry> = input_tables
        .into_iter()
        .map(|table| TableEntry { table, active: true })
        .collect();

    let mut bit_to_tables: HashMap<u32, Vec<usize>> = HashMap::new();
    for (table_index, entry) in entries.iter().enumerate() {
        for &bit in &entry.table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }

    let queue_started = Instant::now();
    let pair_keys = generate_shared_pairs_from_bit_map(&bit_to_tables);
    let candidate_pair_count = pair_keys.len();
    let mut queue = BinaryHeap::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        if let Some(candidate) = build_candidate(
            &entries[left_index].table,
            &entries[right_index].table,
            left_index,
            right_index,
            target_arity,
        ) {
            queue.push(candidate);
        }

        let _ = pair_index;
    }
    let build_queue_seconds = queue_started.elapsed().as_secs_f64();

    let stage_started = Instant::now();
    let mut step_count = 0usize;
    let mut max_created_arity = 0usize;
    let mut final_created_bits = Vec::new();
    let mut final_created_rows = 0usize;
    let mut final_created_rank = 0.0;
    let mut reached_target_arity = false;

    while let Some(candidate) = pop_best_active_candidate(&mut queue, &entries) {
        let left = &entries[candidate.left_index].table;
        let right = &entries[candidate.right_index].table;

        let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
            .map_err(|error| anyhow!(error))
            .with_context(|| {
                format!(
                    "failed to merge greedy pair ({}, {})",
                    candidate.left_index, candidate.right_index
                )
            })?;

        if merged.rows.is_empty() {
            continue;
        }

        let actual_rows = merged.rows.len();
        max_created_arity = max_created_arity.max(merged.bits.len());
        final_created_bits = merged.bits.clone();
        final_created_rows = actual_rows;
        final_created_rank = tables::rank_stats::compute_rank(actual_rows, merged.bits.len());

        entries[candidate.left_index].active = false;
        entries[candidate.right_index].active = false;

        let new_index = entries.len();
        entries.push(TableEntry {
            table: Table {
                bits: merged.bits.clone(),
                rows: merged.rows.clone(),
            },
            active: true,
        });

        for &bit in &merged.bits {
            bit_to_tables.entry(bit).or_default().push(new_index);
        }
        push_new_table_candidates(new_index, &entries, &bit_to_tables, &mut queue, target_arity);

        step_count += 1;

        if merged.bits.len() == target_arity {
            reached_target_arity = true;
            break;
        }
    }

    let output_tables: Vec<Table> = entries
        .into_iter()
        .filter(|entry| entry.active)
        .map(|entry| entry.table)
        .collect();
    let info = GreedyStageInfo {
        candidate_pair_count,
        build_queue_seconds,
        stage_seconds: stage_started.elapsed().as_secs_f64(),
        step_count,
        reached_target_arity,
        max_created_arity,
        final_created_table_arity: final_created_bits.len(),
        final_created_table_rows: final_created_rows,
        final_created_table_rank: final_created_rank,
        final_created_table_bits: final_created_bits,
    };

    Ok(GreedyStageResult { output_tables, info })
}

fn build_candidate(
    left: &Table,
    right: &Table,
    left_index: usize,
    right_index: usize,
    target_arity: usize,
) -> Option<PairCandidate> {
    let shared_indices = shared_indices(left, right);
    if shared_indices.0.is_empty() {
        return None;
    }
    let union_arity = left.bits.len() + right.bits.len() - shared_indices.0.len();
    if union_arity > target_arity {
        return None;
    }

    let predicted_rows = predict_shared_assignment_histogram(left, right, &shared_indices) as usize;
    if predicted_rows == 0 {
        return None;
    }

    Some(PairCandidate {
        left_index,
        right_index,
        predicted_rows,
        union_arity,
        predicted_rank: tables::rank_stats::compute_rank(predicted_rows, union_arity),
    })
}

fn generate_shared_pairs_from_bit_map(bit_to_tables: &HashMap<u32, Vec<usize>>) -> Vec<u64> {
    let mut pair_keys = HashSet::new();
    for table_ids in bit_to_tables.values() {
        for left_offset in 0..table_ids.len() {
            for right_offset in (left_offset + 1)..table_ids.len() {
                pair_keys.insert(pair_key(
                    table_ids[left_offset] as u32,
                    table_ids[right_offset] as u32,
                ));
            }
        }
    }

    let mut pair_keys: Vec<u64> = pair_keys.into_iter().collect();
    pair_keys.sort_unstable();
    pair_keys
}

fn pop_best_active_candidate(
    queue: &mut BinaryHeap<PairCandidate>,
    entries: &[TableEntry],
) -> Option<PairCandidate> {
    while let Some(candidate) = queue.pop() {
        if entries[candidate.left_index].active && entries[candidate.right_index].active {
            return Some(candidate);
        }
    }
    None
}

fn push_new_table_candidates(
    new_index: usize,
    entries: &[TableEntry],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    queue: &mut BinaryHeap<PairCandidate>,
    target_arity: usize,
) {
    let new_table = &entries[new_index].table;
    let mut neighbors = HashSet::new();
    for &bit in &new_table.bits {
        if let Some(table_ids) = bit_to_tables.get(&bit) {
            for &other_index in table_ids {
                if other_index != new_index && entries[other_index].active {
                    neighbors.insert(other_index);
                }
            }
        }
    }

    for other_index in neighbors {
        let left_index = other_index.min(new_index);
        let right_index = other_index.max(new_index);
        if let Some(candidate) = build_candidate(
            &entries[left_index].table,
            &entries[right_index].table,
            left_index,
            right_index,
            target_arity,
        ) {
            queue.push(candidate);
        }
    }
}

struct NoPairwiseStageResult {
    output_tables: Vec<Table>,
    info: NoPairwiseStageInfo,
}

fn run_no_pairwise_pipeline(input_tables: Vec<Table>) -> Result<NoPairwiseStageResult> {
    let stage_started = Instant::now();
    let mut tables = input_tables;
    let mut productive_rounds = 0usize;
    let mut round_count_including_final_check = 0usize;
    let mut total_subset_row_deletions = 0usize;
    let mut total_dropped_included_tables = 0usize;
    let mut total_forced_bits = 0usize;
    let mut total_forced_occurrences = 0usize;
    let mut total_forced_removed_rows = 0usize;
    let mut total_pair_relations = 0usize;
    let mut total_pair_replaced_bits = 0usize;
    let mut total_node_row_deletions = 0usize;
    let mut total_node_tightenings = 0usize;

    loop {
        let (after_subset, subset_info, subset_changed) = step_subset_absorption(&tables);
        let (after_forced, forced_info, forced_changed) = step_forced_bits(after_subset)?;
        let (after_pair_reduction, pair_reduction_info, pair_reduction_changed) =
            step_pair_reduction(after_forced)?;
        let (output_tables, node_filter_info, node_filter_changed) =
            step_node_filter(after_pair_reduction)?;

        let changed =
            subset_changed || forced_changed || pair_reduction_changed || node_filter_changed;

        round_count_including_final_check += 1;
        total_subset_row_deletions += subset_info.subset_row_deletions;
        total_dropped_included_tables += subset_info.dropped_included_tables;
        total_forced_bits += forced_info.forced_bits;
        total_forced_occurrences += forced_info.forced_occurrences;
        total_forced_removed_rows += forced_info.stats.removed_rows;
        total_pair_relations += pair_reduction_info.pair_relation_pairs_total;
        total_pair_replaced_bits += pair_reduction_info.pair_replaced_bits_total;
        total_node_row_deletions += node_filter_info.filter.row_deletions;
        total_node_tightenings += node_filter_info.filter.node_tightenings;
        tables = output_tables;

        if !changed {
            break;
        }

        productive_rounds += 1;
    }

    let info = NoPairwiseStageInfo {
        stage_seconds: stage_started.elapsed().as_secs_f64(),
        productive_rounds,
        round_count_including_final_check,
        output_table_count: tables.len(),
        output_row_count: total_rows(&tables),
        output_rank_summary: summarize_table_ranks(&tables, 10),
        total_subset_row_deletions,
        total_dropped_included_tables,
        total_forced_bits,
        total_forced_occurrences,
        total_forced_removed_rows,
        total_pair_relations,
        total_pair_replaced_bits,
        total_node_row_deletions,
        total_node_tightenings,
    };

    Ok(NoPairwiseStageResult {
        output_tables: tables,
        info,
    })
}

fn step_subset_absorption(tables: &[Table]) -> (Vec<Table>, SubsetAbsorptionInfo, bool) {
    let (mut tables_by_bits, duplicate_count) = collapse_equal_bitsets(tables);
    let (merge_stats, pair_details) = merge_subsets(&mut tables_by_bits);
    let effective_pairs = pair_details
        .iter()
        .filter(|pair| pair.rows_removed > 0)
        .count();
    let (tables_by_bits, dropped_tables) = prune_included_tables(&tables_by_bits, &pair_details);

    let info = SubsetAbsorptionInfo {
        collapsed_duplicate_tables: duplicate_count,
        canonical_table_count: tables_by_bits.len(),
        subset_superset_pairs: merge_stats.pair_count,
        effective_subset_pairs: effective_pairs,
        subset_changed_tables: merge_stats.changed_tables,
        subset_row_deletions: merge_stats.row_deletions,
        emptied_tables_during_subset_merge: merge_stats.emptied_tables,
        dropped_included_tables: dropped_tables.len(),
    };
    let changed = duplicate_count > 0
        || merge_stats.row_deletions > 0
        || merge_stats.changed_tables > 0
        || !dropped_tables.is_empty();
    (to_tables(&tables_by_bits), info, changed)
}

fn step_forced_bits(tables: Vec<Table>) -> Result<(Vec<Table>, ForcedBitsInfo, bool)> {
    let (forced_current, forced_occurrences) = collect_forced_bits_bitwise(&tables)?;
    let (output_tables, forced_stats) = if forced_current.is_empty() {
        (tables, ForcedPropagationStats::default())
    } else {
        propagate_forced_bits(&tables, &forced_current)?
    };

    let info = ForcedBitsInfo {
        forced_bits: forced_current.len(),
        forced_one_bits: forced_current.values().filter(|&&value| value == 1).count(),
        forced_zero_bits: forced_current.values().filter(|&&value| value == 0).count(),
        forced_occurrences,
        stats: forced_stats,
    };
    Ok((output_tables, info, !forced_current.is_empty()))
}

fn step_pair_reduction(mut tables: Vec<Table>) -> Result<(Vec<Table>, PairReductionInfo, bool)> {
    let mut iterations = Vec::new();
    let mut iteration_index = 1usize;
    let mut changed = false;

    loop {
        let relations = extract_relations(&tables)?;
        if relations.is_empty() {
            break;
        }

        let (rewrite_map, component_stats) = build_rewrite_map(&relations)?;
        let (rewritten_tables, rewrite_stats) = rewrite_tables(&tables, &rewrite_map);

        iterations.push(PairReductionIterationInfo {
            iteration: iteration_index,
            relation_pair_count: relations.len(),
            bits_involved: component_stats.bits_involved,
            component_count: component_stats.component_count,
            replaced_bit_count: component_stats.replaced_bit_count,
            changed_tables: rewrite_stats.changed_tables,
            reduced_arity_tables: rewrite_stats.reduced_arity_tables,
            same_arity_changed_tables: rewrite_stats.same_arity_changed_tables,
            removed_rows: rewrite_stats.removed_rows,
            collapsed_duplicate_tables: rewrite_stats.collapsed_duplicate_tables,
            table_count_after_iteration: rewritten_tables.len(),
            bit_count_after_iteration: collect_bits(&rewritten_tables).len(),
        });

        tables = rewritten_tables;
        changed = true;
        iteration_index += 1;
    }

    let info = PairReductionInfo {
        pair_relation_pairs_total: iterations.iter().map(|item| item.relation_pair_count).sum(),
        pair_replaced_bits_total: iterations.iter().map(|item| item.replaced_bit_count).sum(),
        iterations,
    };
    Ok((tables, info, changed))
}

fn step_node_filter(tables: Vec<Table>) -> Result<(Vec<Table>, NodeFilterInfo, bool)> {
    let mut tables = tables;
    let (mut nodes, table_to_nodes, node_build_stats) = build_nodes(&tables)?;
    let filter_stats = filter_tables_with_nodes(&mut tables, &mut nodes, &table_to_nodes)?;
    let info = NodeFilterInfo {
        node_build: node_build_stats,
        filter: filter_stats,
    };
    let changed = info.filter.row_deletions > 0 || info.filter.changed_tables > 0;
    Ok((tables, info, changed))
}

fn normalize_tables(tables: &[Table]) -> Vec<Table> {
    let (collapsed, _) = collapse_equal_bitsets(tables);
    to_tables(&collapsed)
}

fn shared_indices(left: &Table, right: &Table) -> (Vec<u8>, Vec<u8>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut left_shared = Vec::new();
    let mut right_shared = Vec::new();

    while left_index < left.bits.len() && right_index < right.bits.len() {
        match left.bits[left_index].cmp(&right.bits[right_index]) {
            Ordering::Less => left_index += 1,
            Ordering::Greater => right_index += 1,
            Ordering::Equal => {
                left_shared.push(left_index as u8);
                right_shared.push(right_index as u8);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    (left_shared, right_shared)
}

fn predict_shared_assignment_histogram(
    left: &Table,
    right: &Table,
    shared_indices: &(Vec<u8>, Vec<u8>),
) -> u64 {
    let (left_shared_indices, right_shared_indices) = shared_indices;
    let shared_count = left_shared_indices.len();

    let (build_rows, build_indices, probe_rows, probe_indices) = if left.rows.len() <= right.rows.len()
    {
        (&left.rows, left_shared_indices.as_slice(), &right.rows, right_shared_indices.as_slice())
    } else {
        (&right.rows, right_shared_indices.as_slice(), &left.rows, left_shared_indices.as_slice())
    };

    if shared_count <= 16 {
        let mut counts = vec![0u32; 1usize << shared_count];
        for &row in build_rows {
            let key = project_bits(row, build_indices) as usize;
            counts[key] += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices) as usize;
            total += counts[key] as u64;
        }
        total
    } else {
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for &row in build_rows {
            let key = project_bits(row, build_indices);
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices);
            total += counts.get(&key).copied().unwrap_or(0) as u64;
        }
        total
    }
}

fn project_bits(row: u32, indices: &[u8]) -> u32 {
    let mut projected = 0u32;
    for (new_pos, &old_pos) in indices.iter().enumerate() {
        projected |= ((row >> old_pos) & 1) << new_pos;
    }
    projected
}

fn pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = if left < right {
        (left as u64, right as u64)
    } else {
        (right as u64, left as u64)
    };
    (left << 32) | right
}

fn unpack_pair_key(key: u64) -> (usize, usize) {
    ((key >> 32) as usize, key as u32 as usize)
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
        "usage: cargo run --release --bin greedy_rank_no_pairwise_cycles -- --input <path> --output <path> --report <path> [--target-arity <n>]"
    );
}
