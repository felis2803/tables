#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::json;
use tables::common::{
    arity_distribution, collect_bits, read_tables, total_rows, write_json, DroppedTableRecord,
    NodeArtifact, PairRelationRecord, Table,
};
use tables::forced_bits::{
    collect_forced_bits_bitwise, forced_rows, propagate_forced_bits, update_original_forced,
    ForcedBitsInfo, ForcedPropagationStats,
};
use tables::node_filter::{build_nodes, filter_tables_with_nodes, serialize_nodes, NodeFilterInfo};
use tables::pair_reduction::{
    build_final_components, build_rewrite_map, build_rewrite_rows, extract_relations,
    relation_history_rows, rewrite_tables, update_original_mapping, PairReductionInfo,
    PairReductionIterationInfo,
};
use tables::rank_stats::{summarize_table_ranks, RankSummary};
use tables::single_table_bit_filter::{filter_single_table_bits, SingleTableBitFilterInfo};
use tables::subset_absorption::{
    collapse_equal_bitsets, merge_subsets, prune_included_tables, to_tables, SubsetAbsorptionInfo,
};
use tables::tautology_filter::{filter_tautologies, TautologyFilterInfo};

const STAGE_COMMON_NODE_FIXED_POINT: &str = "common_node_fixed_point";

struct Args {
    input: PathBuf,
    max_rounds: Option<usize>,
    output: PathBuf,
    report: PathBuf,
    forced: PathBuf,
    mapping: PathBuf,
    components: PathBuf,
    dropped: PathBuf,
    relations: PathBuf,
    nodes: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            max_rounds: None,
            output: PathBuf::from("data/derived/tables.common_node_fixed_point.json"),
            report: PathBuf::from("data/reports/report.common_node_fixed_point.json"),
            forced: PathBuf::from("data/derived/bits.common_node_fixed_point.forced.json"),
            mapping: PathBuf::from("data/derived/bits.common_node_fixed_point.rewrite_map.json"),
            components: PathBuf::from("data/derived/bits.common_node_fixed_point.components.json"),
            dropped: PathBuf::from(
                "data/derived/tables.common_node_fixed_point.dropped_included.json",
            ),
            relations: PathBuf::from("data/derived/pairs.common_node_fixed_point.relations.json"),
            nodes: PathBuf::from("data/derived/nodes.common_node_fixed_point.json"),
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
                "--max-rounds" => {
                    args.max_rounds = Some(
                        expect_value(&mut iter, "--max-rounds")?
                            .parse()
                            .with_context(|| "invalid value for --max-rounds")?,
                    );
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
                "--forced" => args.forced = PathBuf::from(expect_value(&mut iter, "--forced")?),
                "--mapping" => args.mapping = PathBuf::from(expect_value(&mut iter, "--mapping")?),
                "--components" => {
                    args.components = PathBuf::from(expect_value(&mut iter, "--components")?)
                }
                "--dropped" => args.dropped = PathBuf::from(expect_value(&mut iter, "--dropped")?),
                "--relations" => {
                    args.relations = PathBuf::from(expect_value(&mut iter, "--relations")?)
                }
                "--nodes" => args.nodes = PathBuf::from(expect_value(&mut iter, "--nodes")?),
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

#[derive(Clone, Debug)]
struct PipelineState {
    original_mapping: BTreeMap<u32, (u32, u8)>,
    original_forced: BTreeMap<u32, u8>,
    dropped_tables_history: Vec<DroppedTableRecord>,
    pair_relations_history: Vec<PairRelationRecord>,
    final_nodes: Vec<NodeArtifact>,
}

#[derive(Clone, Debug, Serialize)]
struct RoundInfo {
    round: usize,
    input_table_count: usize,
    input_bit_count: usize,
    input_row_count: usize,
    input_arity_distribution: BTreeMap<String, usize>,
    input_rank_summary: RankSummary,
    subset_absorption: SubsetAbsorptionInfo,
    forced_bits: ForcedBitsInfo,
    single_table_bit_filter: SingleTableBitFilterInfo,
    pair_reduction: PairReductionInfo,
    tautology_filter: TautologyFilterInfo,
    node_filter: NodeFilterInfo,
    output_table_count: usize,
    output_bit_count: usize,
    output_row_count: usize,
    output_arity_distribution: BTreeMap<String, usize>,
    output_rank_summary: RankSummary,
    changed: bool,
}

fn initialize_pipeline_state(tables: &[Table]) -> PipelineState {
    let original_mapping = collect_bits(tables)
        .into_iter()
        .map(|bit| (bit, (bit, 0u8)))
        .collect();
    PipelineState {
        original_mapping,
        original_forced: BTreeMap::new(),
        dropped_tables_history: Vec::new(),
        pair_relations_history: Vec::new(),
        final_nodes: Vec::new(),
    }
}

fn step_subset_absorption(
    tables: &[Table],
    state: &mut PipelineState,
    round_index: usize,
) -> (Vec<Table>, SubsetAbsorptionInfo, bool) {
    let (mut tables_by_bits, duplicate_count) = collapse_equal_bitsets(tables);
    let (merge_stats, pair_details) = merge_subsets(&mut tables_by_bits);
    let effective_pairs = pair_details
        .iter()
        .filter(|pair| pair.rows_removed > 0)
        .count();
    let (tables_by_bits, dropped_tables) = prune_included_tables(&tables_by_bits, &pair_details);

    if !dropped_tables.is_empty() {
        state
            .dropped_tables_history
            .extend(
                dropped_tables
                    .iter()
                    .cloned()
                    .map(|bits| DroppedTableRecord {
                        round: round_index,
                        bits,
                    }),
            );
    }

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

fn step_forced_bits(
    tables: Vec<Table>,
    state: &mut PipelineState,
) -> Result<(Vec<Table>, ForcedBitsInfo, bool)> {
    let (forced_current, forced_occurrences) = collect_forced_bits_bitwise(&tables)?;

    let (output_tables, forced_stats) = if forced_current.is_empty() {
        (tables, ForcedPropagationStats::default())
    } else {
        update_original_forced(
            &state.original_mapping,
            &mut state.original_forced,
            &forced_current,
        )?;
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

fn step_single_table_bit_filter(
    tables: Vec<Table>,
) -> Result<(Vec<Table>, SingleTableBitFilterInfo, bool)> {
    let (output_tables, info) = filter_single_table_bits(&tables)?;
    let changed = info.removed_bits > 0 || info.collapsed_duplicate_tables > 0;
    Ok((output_tables, info, changed))
}

fn step_pair_reduction(
    mut tables: Vec<Table>,
    state: &mut PipelineState,
    round_index: usize,
) -> Result<(Vec<Table>, PairReductionInfo, bool)> {
    let mut iterations = Vec::new();
    let mut iteration_index = 1usize;
    let mut changed = false;

    loop {
        let relations = extract_relations(&tables)?;
        if relations.is_empty() {
            break;
        }

        let (rewrite_map, component_stats) = build_rewrite_map(&relations)?;
        state.original_mapping = update_original_mapping(&state.original_mapping, &rewrite_map);
        let (rewritten_tables, rewrite_stats) = rewrite_tables(&tables, &rewrite_map);

        state.pair_relations_history.extend(relation_history_rows(
            round_index,
            iteration_index,
            &relations,
        ));

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

fn step_tautology_filter(tables: Vec<Table>) -> (Vec<Table>, TautologyFilterInfo, bool) {
    let (output_tables, info) = filter_tautologies(tables);
    let changed = info.removed_tables > 0;
    (output_tables, info, changed)
}

fn step_node_filter(
    mut tables: Vec<Table>,
    state: &mut PipelineState,
) -> Result<(Vec<Table>, NodeFilterInfo, bool)> {
    let (mut nodes, table_to_nodes, node_build_stats) = build_nodes(&tables)?;
    let filter_stats = filter_tables_with_nodes(&mut tables, &mut nodes, &table_to_nodes)?;
    state.final_nodes = serialize_nodes(&nodes);

    let info = NodeFilterInfo {
        node_build: node_build_stats,
        filter: filter_stats,
    };
    let changed = info.filter.row_deletions > 0 || info.filter.changed_tables > 0;
    Ok((tables, info, changed))
}

fn run_reduction_pipeline(
    mut tables: Vec<Table>,
    state: &mut PipelineState,
    max_rounds: Option<usize>,
) -> Result<(Vec<Table>, Vec<RoundInfo>, usize)> {
    let mut rounds = Vec::new();
    let mut productive_rounds = 0usize;
    let mut round_index = 1usize;

    loop {
        let input_table_count = tables.len();
        let input_bit_count = collect_bits(&tables).len();
        let input_row_count = total_rows(&tables);
        let input_arity_distribution = arity_distribution(&tables);
        let input_rank_summary = summarize_table_ranks(&tables, 10);

        let (after_subset, subset_info, subset_changed) =
            step_subset_absorption(&tables, state, round_index);
        let (after_forced, forced_info, forced_changed) = step_forced_bits(after_subset, state)?;
        let (after_single_table_bit_filter, single_table_bit_filter_info, single_table_changed) =
            step_single_table_bit_filter(after_forced)?;
        let (after_pair_reduction, pair_reduction_info, pair_reduction_changed) =
            step_pair_reduction(after_single_table_bit_filter, state, round_index)?;
        let (after_tautology_filter, tautology_info, tautology_changed) =
            step_tautology_filter(after_pair_reduction);
        let (output_tables, node_filter_info, node_filter_changed) =
            step_node_filter(after_tautology_filter, state)?;

        let changed = subset_changed
            || forced_changed
            || single_table_changed
            || pair_reduction_changed
            || tautology_changed
            || node_filter_changed;

        let round_info = RoundInfo {
            round: round_index,
            input_table_count,
            input_bit_count,
            input_row_count,
            input_arity_distribution,
            input_rank_summary,
            subset_absorption: subset_info,
            forced_bits: forced_info,
            single_table_bit_filter: single_table_bit_filter_info,
            pair_reduction: pair_reduction_info,
            tautology_filter: tautology_info,
            node_filter: node_filter_info,
            output_table_count: output_tables.len(),
            output_bit_count: collect_bits(&output_tables).len(),
            output_row_count: total_rows(&output_tables),
            output_arity_distribution: arity_distribution(&output_tables),
            output_rank_summary: summarize_table_ranks(&output_tables, 10),
            changed,
        };
        rounds.push(round_info);
        tables = output_tables;

        if !changed {
            break;
        }

        productive_rounds += 1;
        if max_rounds.is_some_and(|limit| round_index >= limit) {
            break;
        }
        round_index += 1;
    }

    Ok((tables, rounds, productive_rounds))
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    let tables = read_tables(&args.input)?;
    let initial_table_count = tables.len();
    let initial_bits = collect_bits(&tables);
    let initial_row_count = total_rows(&tables);
    let initial_rank_summary = summarize_table_ranks(&tables, 10);

    let mut state = initialize_pipeline_state(&tables);
    let (tables, rounds, productive_rounds) =
        run_reduction_pipeline(tables, &mut state, args.max_rounds)?;

    let forced_rows = forced_rows(&state.original_forced);
    let rewrite_rows = build_rewrite_rows(&state.original_mapping, &state.original_forced);
    let final_components = build_final_components(&state.original_mapping, &state.original_forced);

    let report = json!({
        "method": "repeat subset absorption, AND/OR fixed-bit propagation/removal, single-table bit filtering, equal/opposite pair reduction, tautology filtering, and node-based projection intersection filtering until no further change",
        "steps": [
            "subset_absorption",
            "forced_bits",
            "single_table_bit_filter",
            "pair_reduction",
            "tautology_filter",
            "node_filter"
        ],
        "stage": STAGE_COMMON_NODE_FIXED_POINT,
        "input": path_string(&args.input),
        "output": path_string(&args.output),
        "nodes_output": path_string(&args.nodes),
        "max_rounds": args.max_rounds,
        "initial_table_count": initial_table_count,
        "initial_bit_count": initial_bits.len(),
        "initial_row_count": initial_row_count,
        "initial_rank_summary": initial_rank_summary,
        "final_table_count": tables.len(),
        "final_bit_count": collect_bits(&tables).len(),
        "final_row_count": total_rows(&tables),
        "final_rank_summary": summarize_table_ranks(&tables, 10),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": rounds.len(),
        "total_collapsed_duplicate_tables_in_subset_step": rounds.iter().map(|round| round.subset_absorption.collapsed_duplicate_tables).sum::<usize>(),
        "total_subset_row_deletions": rounds.iter().map(|round| round.subset_absorption.subset_row_deletions).sum::<usize>(),
        "total_dropped_included_tables": state.dropped_tables_history.len(),
        "total_forced_bits_detected_across_rounds": rounds.iter().map(|round| round.forced_bits.forced_bits).sum::<usize>(),
        "total_forced_occurrences": rounds.iter().map(|round| round.forced_bits.forced_occurrences).sum::<usize>(),
        "total_removed_rows_in_forced_step": rounds.iter().map(|round| round.forced_bits.stats.removed_rows).sum::<usize>(),
        "total_removed_single_table_bits": rounds.iter().map(|round| round.single_table_bit_filter.removed_bits).sum::<usize>(),
        "total_changed_tables_in_single_table_bit_filter": rounds.iter().map(|round| round.single_table_bit_filter.changed_tables).sum::<usize>(),
        "total_removed_rows_in_single_table_bit_filter": rounds.iter().map(|round| round.single_table_bit_filter.removed_rows_after_projection_dedup).sum::<usize>(),
        "total_collapsed_duplicate_tables_in_single_table_bit_filter": rounds.iter().map(|round| round.single_table_bit_filter.collapsed_duplicate_tables).sum::<usize>(),
        "total_removed_tautologies": rounds.iter().map(|round| round.tautology_filter.removed_tables).sum::<usize>(),
        "total_removed_tautology_rows": rounds.iter().map(|round| round.tautology_filter.removed_rows).sum::<usize>(),
        "final_forced_original_bits": forced_rows.len(),
        "total_pair_relation_pairs_found": state.pair_relations_history.len(),
        "total_pair_replaced_bits": rounds.iter().map(|round| round.pair_reduction.pair_replaced_bits_total).sum::<usize>(),
        "total_nodes_built": rounds.iter().map(|round| round.node_filter.node_build.node_count).sum::<usize>(),
        "total_initial_restrictive_nodes": rounds.iter().map(|round| round.node_filter.node_build.restrictive_node_count).sum::<usize>(),
        "total_node_changed_tables": rounds.iter().map(|round| round.node_filter.filter.changed_tables).sum::<usize>(),
        "total_node_row_deletions": rounds.iter().map(|round| round.node_filter.filter.row_deletions).sum::<usize>(),
        "total_node_recomputations": rounds.iter().map(|round| round.node_filter.filter.node_recomputations).sum::<usize>(),
        "total_node_tightenings": rounds.iter().map(|round| round.node_filter.filter.node_tightenings).sum::<usize>(),
        "final_rewritten_original_bits": rewrite_rows.len(),
        "final_components_with_rewrites": final_components.len(),
        "final_node_count": state.final_nodes.len(),
        "final_arity_distribution": arity_distribution(&tables),
        "rounds": rounds,
    });

    write_json(&args.output, &tables)?;
    write_json(&args.report, &report)?;
    write_json(&args.forced, &forced_rows)?;
    write_json(&args.mapping, &rewrite_rows)?;
    write_json(&args.components, &final_components)?;
    write_json(&args.dropped, &state.dropped_tables_history)?;
    write_json(&args.relations, &state.pair_relations_history)?;
    write_json(&args.nodes, &state.final_nodes)?;

    println!("productive rounds: {productive_rounds}");
    println!("rounds including final check: {}", rounds.len());
    println!("final tables: {}", tables.len());
    println!("final bits: {}", collect_bits(&tables).len());
    println!("forced original bits: {}", forced_rows.len());
    println!("rewritten original bits: {}", rewrite_rows.len());
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

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
    println!("usage: cargo run --release -- --input <path> [--max-rounds <n>] [--output <path>] [--report <path>] [--forced <path>] [--mapping <path>] [--components <path>] [--dropped <path>] [--relations <path>] [--nodes <path>]");
}
