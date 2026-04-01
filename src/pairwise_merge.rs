use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Result};
use serde::Serialize;

use crate::common::{
    arity_distribution, collect_bits, intersect_sorted, is_full_row_set, tables_from_canonical_map,
    total_rows, Table,
};
use crate::rank_stats::{summarize_table_ranks, RankSummary};
use crate::subset_absorption::collapse_equal_bitsets;
use crate::table_merge_fast::merge_tables_fast_from_slices;

pub type CanonicalTableMap = BTreeMap<Vec<u32>, Vec<u32>>;

#[derive(Clone, Debug, Serialize)]
pub struct PairwiseMergeStats {
    pub max_result_arity: usize,
    pub input_table_count: usize,
    pub input_bit_count: usize,
    pub input_row_count: usize,
    pub input_arity_distribution: BTreeMap<String, usize>,
    pub active_input_table_count: usize,
    pub unchanged_input_table_count: usize,
    pub collapsed_duplicate_tables_before_merge: usize,
    pub bitpair_key_count: usize,
    pub raw_pair_hits_over_bitpairs: usize,
    pub candidate_pair_count: usize,
    pub skipped_by_arity: usize,
    pub empty_merge_count: usize,
    pub skipped_tautology_merges: usize,
    pub produced_nonempty_merges: usize,
    pub merged_table_count: usize,
    pub merged_row_count: usize,
    pub merged_duplicate_tables_collapsed: usize,
    pub dropped_source_tables: usize,
    pub retained_original_tables: usize,
    pub combined_duplicate_tables_collapsed: usize,
    pub final_table_count: usize,
    pub final_bit_count: usize,
    pub final_row_count: usize,
    pub input_rank_summary: RankSummary,
    pub merged_rank_summary: RankSummary,
    pub final_rank_summary: RankSummary,
    pub merged_arity_distribution: BTreeMap<String, usize>,
    pub final_arity_distribution: BTreeMap<String, usize>,
}

fn fits_merge_arity(left_bits: &[u32], right_bits: &[u32], max_result_arity: usize) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut union_count = 0usize;

    while left_index < left_bits.len() || right_index < right_bits.len() {
        if union_count >= max_result_arity {
            return false;
        }

        match (left_bits.get(left_index), right_bits.get(right_index)) {
            (Some(left_bit), Some(right_bit)) if left_bit == right_bit => {
                left_index += 1;
                right_index += 1;
            }
            (Some(left_bit), Some(right_bit)) if left_bit < right_bit => {
                left_index += 1;
            }
            (Some(_), Some(_)) => {
                right_index += 1;
            }
            (Some(_), None) => left_index += 1,
            (None, Some(_)) => right_index += 1,
            (None, None) => break,
        }
        union_count += 1;
    }

    true
}

fn pair_key(left: usize, right: usize) -> u64 {
    let (left, right) = if left < right {
        (left as u64, right as u64)
    } else {
        (right as u64, left as u64)
    };
    (left << 32) | right
}

fn generate_bitpair_to_tables(tables: &[Table]) -> (HashMap<(u32, u32), Vec<usize>>, usize) {
    let mut bitpair_to_tables: HashMap<(u32, u32), Vec<usize>> = HashMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for left in 0..table.bits.len() {
            for right in (left + 1)..table.bits.len() {
                bitpair_to_tables
                    .entry((table.bits[left], table.bits[right]))
                    .or_default()
                    .push(table_index);
            }
        }
    }
    let bitpair_key_count = bitpair_to_tables.len();
    (bitpair_to_tables, bitpair_key_count)
}

fn active_table_flags(
    canonical_tables: &[Table],
    previous_canonical_input: Option<&CanonicalTableMap>,
) -> (Vec<bool>, usize) {
    let Some(previous_canonical_input) = previous_canonical_input else {
        return (vec![true; canonical_tables.len()], canonical_tables.len());
    };

    let active_flags: Vec<bool> = canonical_tables
        .iter()
        .map(|table| {
            previous_canonical_input
                .get(&table.bits)
                .is_none_or(|previous_rows| previous_rows != &table.rows)
        })
        .collect();
    let active_table_count = active_flags.iter().filter(|&&flag| flag).count();
    (active_flags, active_table_count)
}

pub fn run_pairwise_merge_incremental(
    tables: &[Table],
    max_result_arity: usize,
) -> Result<(Vec<Table>, PairwiseMergeStats, CanonicalTableMap)> {
    run_pairwise_merge_with_previous_input(tables, max_result_arity, None)
}

pub fn run_pairwise_merge_with_previous_input(
    tables: &[Table],
    max_result_arity: usize,
    previous_canonical_input: Option<&CanonicalTableMap>,
) -> Result<(Vec<Table>, PairwiseMergeStats, CanonicalTableMap)> {
    let (canonical_by_bits, collapsed_duplicate_tables) = collapse_equal_bitsets(tables);
    let canonical_tables = tables_from_canonical_map(&canonical_by_bits);
    let input_bit_count = collect_bits(&canonical_tables).len();
    let input_row_count = total_rows(&canonical_tables);
    let (active_flags, active_input_table_count) =
        active_table_flags(&canonical_tables, previous_canonical_input);
    let unchanged_input_table_count = canonical_tables.len() - active_input_table_count;

    let (bitpair_to_tables, bitpair_key_count) = generate_bitpair_to_tables(&canonical_tables);
    let mut raw_pair_hits = 0usize;
    let mut candidate_pair_count = 0usize;
    let mut seen_pairs = HashSet::new();
    let mut merged_by_bits: BTreeMap<Vec<u32>, Vec<u32>> = BTreeMap::new();
    let mut merged_duplicate_tables = 0usize;
    let mut merged_table_input_count = 0usize;
    let mut merged_source_flags = vec![false; canonical_tables.len()];
    let mut skipped_by_arity = 0usize;
    let mut empty_merges = 0usize;
    let mut tautology_merges = 0usize;

    let mut process_pair = |left_index: usize, right_index: usize| -> Result<()> {
        if !seen_pairs.insert(pair_key(left_index, right_index)) {
            return Ok(());
        }
        candidate_pair_count += 1;

        let left = &canonical_tables[left_index];
        let right = &canonical_tables[right_index];
        if !fits_merge_arity(&left.bits, &right.bits, max_result_arity) {
            skipped_by_arity += 1;
            return Ok(());
        }

        let merged =
            merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
                .map_err(|error| anyhow!(error))?;
        if merged.rows.is_empty() {
            empty_merges += 1;
            return Ok(());
        }
        if is_full_row_set(merged.rows.len(), merged.bits.len()) {
            tautology_merges += 1;
            return Ok(());
        }

        merged_table_input_count += 1;
        merged_source_flags[left_index] = true;
        merged_source_flags[right_index] = true;

        if let Some(existing_rows) = merged_by_bits.get_mut(&merged.bits) {
            *existing_rows = intersect_sorted(existing_rows, &merged.rows);
            merged_duplicate_tables += 1;
        } else {
            merged_by_bits.insert(merged.bits, merged.rows);
        }

        Ok(())
    };

    for table_ids in bitpair_to_tables.values() {
        raw_pair_hits += table_ids.len() * table_ids.len().saturating_sub(1) / 2;
        let mut active_ids = Vec::new();
        let mut inactive_ids = Vec::new();
        for &table_index in table_ids {
            if active_flags[table_index] {
                active_ids.push(table_index);
            } else {
                inactive_ids.push(table_index);
            }
        }
        if active_ids.is_empty() {
            continue;
        }

        for left_offset in 0..active_ids.len() {
            for right_offset in (left_offset + 1)..active_ids.len() {
                process_pair(active_ids[left_offset], active_ids[right_offset])?;
            }
        }
        for &active_index in &active_ids {
            for &inactive_index in &inactive_ids {
                process_pair(active_index, inactive_index)?;
            }
        }
    }

    let merged_tables_canonical = tables_from_canonical_map(&merged_by_bits);
    let dropped_source_tables = merged_source_flags.iter().filter(|&&flag| flag).count();
    let retained_original_tables = canonical_tables.len() - dropped_source_tables;

    let mut combined_by_bits: BTreeMap<Vec<u32>, Vec<u32>> = canonical_tables
        .iter()
        .enumerate()
        .filter(|(table_index, _)| !merged_source_flags[*table_index])
        .map(|(_, table)| (table.bits.clone(), table.rows.clone()))
        .collect();
    let mut combined_duplicate_tables = 0usize;
    for (bits, rows) in merged_by_bits {
        if let Some(existing_rows) = combined_by_bits.get_mut(&bits) {
            *existing_rows = intersect_sorted(existing_rows, &rows);
            combined_duplicate_tables += 1;
        } else {
            combined_by_bits.insert(bits, rows);
        }
    }
    let output_tables = tables_from_canonical_map(&combined_by_bits);

    let stats = PairwiseMergeStats {
        max_result_arity,
        input_table_count: canonical_tables.len(),
        input_bit_count,
        input_row_count,
        input_arity_distribution: arity_distribution(&canonical_tables),
        active_input_table_count,
        unchanged_input_table_count,
        collapsed_duplicate_tables_before_merge: collapsed_duplicate_tables,
        bitpair_key_count,
        raw_pair_hits_over_bitpairs: raw_pair_hits,
        candidate_pair_count,
        skipped_by_arity,
        empty_merge_count: empty_merges,
        skipped_tautology_merges: tautology_merges,
        produced_nonempty_merges: merged_table_input_count,
        merged_table_count: merged_tables_canonical.len(),
        merged_row_count: total_rows(&merged_tables_canonical),
        merged_duplicate_tables_collapsed: merged_duplicate_tables,
        dropped_source_tables,
        retained_original_tables,
        combined_duplicate_tables_collapsed: combined_duplicate_tables,
        final_table_count: output_tables.len(),
        final_bit_count: collect_bits(&output_tables).len(),
        final_row_count: total_rows(&output_tables),
        input_rank_summary: summarize_table_ranks(&canonical_tables, 10),
        merged_rank_summary: summarize_table_ranks(&merged_tables_canonical, 10),
        final_rank_summary: summarize_table_ranks(&output_tables, 10),
        merged_arity_distribution: arity_distribution(&merged_tables_canonical),
        final_arity_distribution: arity_distribution(&output_tables),
    };

    Ok((output_tables, stats, canonical_by_bits))
}

pub fn run_pairwise_merge(
    tables: &[Table],
    max_result_arity: usize,
) -> Result<(Vec<Table>, PairwiseMergeStats)> {
    let (output_tables, stats, _) = run_pairwise_merge_incremental(tables, max_result_arity)?;
    Ok((output_tables, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_pairwise_merge_drops_merged_sources_and_keeps_unrelated_tables() {
        let tables = vec![
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![2, 3, 4],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![10, 11],
                rows: vec![0b00, 0b01],
            },
        ];

        let (output, stats) = run_pairwise_merge(&tables, 16).unwrap();

        assert_eq!(stats.candidate_pair_count, 1);
        assert_eq!(stats.produced_nonempty_merges, 1);
        assert_eq!(stats.dropped_source_tables, 2);
        assert_eq!(stats.retained_original_tables, 1);
        assert_eq!(output.len(), 2);
        assert_eq!(
            output,
            vec![
                Table {
                    bits: vec![10, 11],
                    rows: vec![0b00, 0b01],
                },
                Table {
                    bits: vec![1, 2, 3, 4],
                    rows: vec![0b0000, 0b1111],
                },
            ]
        );
    }

    #[test]
    fn run_pairwise_merge_respects_max_result_arity() {
        let tables = vec![
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![2, 3, 4],
                rows: vec![0b000, 0b111],
            },
        ];

        let (output, stats) = run_pairwise_merge(&tables, 3).unwrap();

        assert_eq!(stats.candidate_pair_count, 1);
        assert_eq!(stats.skipped_by_arity, 1);
        assert_eq!(stats.produced_nonempty_merges, 0);
        assert_eq!(stats.dropped_source_tables, 0);
        assert_eq!(output, tables);
    }

    #[test]
    fn run_pairwise_merge_incremental_matches_full_output_on_followup_round() {
        let round_one_input = vec![
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![2, 3, 4],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![1, 4, 5],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![20, 21, 22],
                rows: vec![0b000],
            },
            Table {
                bits: vec![20, 21, 23],
                rows: vec![0b011],
            },
        ];

        let (round_one_output, _, round_one_snapshot) =
            run_pairwise_merge_incremental(&round_one_input, 16).unwrap();
        let (incremental_output, incremental_stats, _) = run_pairwise_merge_with_previous_input(
            &round_one_output,
            16,
            Some(&round_one_snapshot),
        )
        .unwrap();
        let (full_output, full_stats) = run_pairwise_merge(&round_one_output, 16).unwrap();

        assert_eq!(incremental_output, full_output);
        assert_eq!(incremental_stats.produced_nonempty_merges, 1);
        assert_eq!(incremental_stats.candidate_pair_count, 1);
        assert_eq!(incremental_stats.active_input_table_count, 1);
        assert_eq!(incremental_stats.unchanged_input_table_count, 3);
        assert!(full_stats.candidate_pair_count > incremental_stats.candidate_pair_count);
    }
}
