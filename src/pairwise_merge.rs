use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{anyhow, Result};
use serde::Serialize;

use crate::common::{
    arity_distribution, collect_bits, is_full_row_set, tables_from_canonical_map, total_rows, Table,
};
use crate::rank_stats::{summarize_table_ranks, RankSummary};
use crate::subset_absorption::collapse_equal_bitsets;
use crate::table_merge_fast::{merge_tables_fast, Table32};

#[derive(Clone, Debug, Serialize)]
pub struct PairwiseMergeStats {
    pub max_result_arity: usize,
    pub input_table_count: usize,
    pub input_bit_count: usize,
    pub input_row_count: usize,
    pub input_arity_distribution: BTreeMap<String, usize>,
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

fn generate_candidate_pairs(tables: &[Table]) -> (Vec<(usize, usize)>, usize, usize) {
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

    let mut raw_pair_hits = 0usize;
    let mut candidate_pairs = BTreeSet::new();
    for table_ids in bitpair_to_tables.values() {
        raw_pair_hits += table_ids.len() * table_ids.len().saturating_sub(1) / 2;
        for left in 0..table_ids.len() {
            for right in (left + 1)..table_ids.len() {
                let a = table_ids[left];
                let b = table_ids[right];
                candidate_pairs.insert(if a < b { (a, b) } else { (b, a) });
            }
        }
    }

    (
        candidate_pairs.into_iter().collect(),
        bitpair_to_tables.len(),
        raw_pair_hits,
    )
}

pub fn run_pairwise_merge(
    tables: &[Table],
    max_result_arity: usize,
) -> Result<(Vec<Table>, PairwiseMergeStats)> {
    let (canonical_by_bits, collapsed_duplicate_tables) = collapse_equal_bitsets(tables);
    let canonical_tables = tables_from_canonical_map(&canonical_by_bits);
    let input_bit_count = collect_bits(&canonical_tables).len();
    let input_row_count = total_rows(&canonical_tables);

    let (candidate_pairs, bitpair_key_count, raw_pair_hits) =
        generate_candidate_pairs(&canonical_tables);
    let mut merged_tables = Vec::new();
    let mut merged_source_indices = BTreeSet::new();
    let mut skipped_by_arity = 0usize;
    let mut empty_merges = 0usize;
    let mut tautology_merges = 0usize;

    for (left_index, right_index) in candidate_pairs.iter().copied() {
        let left = &canonical_tables[left_index];
        let right = &canonical_tables[right_index];
        if !fits_merge_arity(&left.bits, &right.bits, max_result_arity) {
            skipped_by_arity += 1;
            continue;
        }

        let merged = merge_tables_fast(
            &Table32 {
                bits: left.bits.clone(),
                rows: left.rows.clone(),
            },
            &Table32 {
                bits: right.bits.clone(),
                rows: right.rows.clone(),
            },
        )
        .map_err(|error| anyhow!(error))?;
        if merged.rows.is_empty() {
            empty_merges += 1;
            continue;
        }
        if is_full_row_set(merged.rows.len(), merged.bits.len()) {
            tautology_merges += 1;
            continue;
        }

        merged_tables.push(Table {
            bits: merged.bits,
            rows: merged.rows,
        });
        merged_source_indices.insert(left_index);
        merged_source_indices.insert(right_index);
    }

    let (merged_by_bits, merged_duplicate_tables) = collapse_equal_bitsets(&merged_tables);
    let merged_tables_canonical = tables_from_canonical_map(&merged_by_bits);

    let retained_originals: Vec<Table> = canonical_tables
        .iter()
        .enumerate()
        .filter(|(table_index, _)| !merged_source_indices.contains(table_index))
        .map(|(_, table)| table.clone())
        .collect();
    let mut combined_source = retained_originals.clone();
    combined_source.extend(merged_tables_canonical.clone());
    let (combined_by_bits, combined_duplicate_tables) = collapse_equal_bitsets(&combined_source);
    let output_tables = tables_from_canonical_map(&combined_by_bits);

    let stats = PairwiseMergeStats {
        max_result_arity,
        input_table_count: canonical_tables.len(),
        input_bit_count,
        input_row_count,
        input_arity_distribution: arity_distribution(&canonical_tables),
        collapsed_duplicate_tables_before_merge: collapsed_duplicate_tables,
        bitpair_key_count,
        raw_pair_hits_over_bitpairs: raw_pair_hits,
        candidate_pair_count: candidate_pairs.len(),
        skipped_by_arity,
        empty_merge_count: empty_merges,
        skipped_tautology_merges: tautology_merges,
        produced_nonempty_merges: merged_tables.len(),
        merged_table_count: merged_tables_canonical.len(),
        merged_row_count: total_rows(&merged_tables_canonical),
        merged_duplicate_tables_collapsed: merged_duplicate_tables,
        dropped_source_tables: merged_source_indices.len(),
        retained_original_tables: retained_originals.len(),
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
}
