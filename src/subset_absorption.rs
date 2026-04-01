use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::common::{
    for_each_combination, intersect_sorted, project_row, sort_dedup_rows,
    tables_from_canonical_map, Table,
};

#[derive(Clone, Debug, Serialize)]
pub struct PairDetail {
    pub subset_bits: Vec<u32>,
    pub superset_bits: Vec<u32>,
    pub rows_removed: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MergeStats {
    pub pair_count: usize,
    pub changed_tables: usize,
    pub row_deletions: usize,
    pub emptied_tables: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubsetAbsorptionInfo {
    pub collapsed_duplicate_tables: usize,
    pub canonical_table_count: usize,
    pub subset_superset_pairs: usize,
    pub effective_subset_pairs: usize,
    pub subset_changed_tables: usize,
    pub subset_row_deletions: usize,
    pub emptied_tables_during_subset_merge: usize,
    pub dropped_included_tables: usize,
}

pub fn canonicalize_table(table: &Table) -> (Vec<u32>, Vec<u32>) {
    let mut order: Vec<usize> = (0..table.bits.len()).collect();
    order.sort_by_key(|&index| table.bits[index]);

    let sorted_bits: Vec<u32> = order.iter().map(|&index| table.bits[index]).collect();
    let mut rows = Vec::with_capacity(table.rows.len());
    for &row in &table.rows {
        let mut mapped = 0u32;
        for (new_offset, &old_offset) in order.iter().enumerate() {
            if ((row >> old_offset) & 1) != 0 {
                mapped |= 1u32 << new_offset;
            }
        }
        rows.push(mapped);
    }
    sort_dedup_rows(&mut rows);

    (sorted_bits, rows)
}

pub fn collapse_equal_bitsets(tables: &[Table]) -> (BTreeMap<Vec<u32>, Vec<u32>>, usize) {
    let mut merged: BTreeMap<Vec<u32>, Vec<u32>> = BTreeMap::new();
    let mut duplicate_count = 0usize;

    for table in tables {
        let (bits, rows) = canonicalize_table(table);
        if let Some(existing_rows) = merged.get_mut(&bits) {
            let intersection = intersect_sorted(existing_rows, &rows);
            *existing_rows = intersection;
            duplicate_count += 1;
        } else {
            merged.insert(bits, rows);
        }
    }

    (merged, duplicate_count)
}

pub fn merge_subsets(
    tables_by_bits: &mut BTreeMap<Vec<u32>, Vec<u32>>,
) -> (MergeStats, Vec<PairDetail>) {
    let lengths_present: BTreeSet<usize> = tables_by_bits.keys().map(|bits| bits.len()).collect();
    let mut stats = MergeStats::default();
    let mut pair_details = Vec::new();

    let mut sup_keys: Vec<Vec<u32>> = tables_by_bits.keys().cloned().collect();
    sup_keys.sort_by(|left, right| left.len().cmp(&right.len()).then_with(|| left.cmp(right)));

    for sup_bits in sup_keys {
        let Some(mut sup_rows) = tables_by_bits.get(&sup_bits).cloned() else {
            continue;
        };
        let mut changed_here = false;
        let bit_count = sup_bits.len();

        let subset_sizes: Vec<usize> = lengths_present
            .iter()
            .copied()
            .filter(|&length| length > 0 && length < bit_count)
            .collect();

        for subset_size in subset_sizes {
            let mut stop = false;
            for_each_combination(bit_count, subset_size, |subset_indices| {
                if stop {
                    return;
                }

                let subset_bits: Vec<u32> = subset_indices
                    .iter()
                    .map(|&index| sup_bits[index])
                    .collect();
                let Some(subset_rows) = tables_by_bits.get(&subset_bits) else {
                    return;
                };

                stats.pair_count += 1;
                let before_count = sup_rows.len();
                let filtered_rows: Vec<u32> = sup_rows
                    .iter()
                    .copied()
                    .filter(|&row| {
                        subset_rows
                            .binary_search(&project_row(row, subset_indices))
                            .is_ok()
                    })
                    .collect();
                let removed = before_count - filtered_rows.len();
                pair_details.push(PairDetail {
                    subset_bits,
                    superset_bits: sup_bits.clone(),
                    rows_removed: removed,
                });

                if filtered_rows.len() != sup_rows.len() {
                    stats.row_deletions += removed;
                    sup_rows = filtered_rows;
                    changed_here = true;
                    if sup_rows.is_empty() {
                        stats.emptied_tables += 1;
                        stop = true;
                    }
                }
            });

            if sup_rows.is_empty() {
                break;
            }
        }

        if changed_here {
            stats.changed_tables += 1;
            tables_by_bits.insert(sup_bits, sup_rows);
        }
    }

    (stats, pair_details)
}

pub fn prune_included_tables(
    tables_by_bits: &BTreeMap<Vec<u32>, Vec<u32>>,
    pair_details: &[PairDetail],
) -> (BTreeMap<Vec<u32>, Vec<u32>>, Vec<Vec<u32>>) {
    let subset_keys: BTreeSet<Vec<u32>> = pair_details
        .iter()
        .map(|pair| pair.subset_bits.clone())
        .collect();

    let pruned = tables_by_bits
        .iter()
        .filter(|(bits, _)| !subset_keys.contains(*bits))
        .map(|(bits, rows)| (bits.clone(), rows.clone()))
        .collect();

    let dropped = subset_keys
        .into_iter()
        .filter(|bits| tables_by_bits.contains_key(bits))
        .collect();

    (pruned, dropped)
}

pub fn to_tables(tables_by_bits: &BTreeMap<Vec<u32>, Vec<u32>>) -> Vec<Table> {
    tables_from_canonical_map(tables_by_bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_equal_bitsets_intersects_duplicate_tables() {
        let tables = vec![
            Table {
                bits: vec![2, 1],
                rows: vec![0, 1, 2],
            },
            Table {
                bits: vec![1, 2],
                rows: vec![1, 2, 3],
            },
        ];

        let (collapsed, duplicate_count) = collapse_equal_bitsets(&tables);
        assert_eq!(duplicate_count, 1);
        assert_eq!(collapsed.get(&vec![1, 2]).unwrap(), &vec![1, 2]);
    }

    #[test]
    fn merge_subsets_filters_superset_rows_and_prunes_subset() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b001, 0b011, 0b110, 0b111],
            },
        ];

        let (mut collapsed, duplicate_count) = collapse_equal_bitsets(&tables);
        assert_eq!(duplicate_count, 0);

        let (stats, pair_details) = merge_subsets(&mut collapsed);
        assert_eq!(stats.pair_count, 1);
        assert_eq!(stats.changed_tables, 1);
        assert_eq!(stats.row_deletions, 2);
        assert_eq!(pair_details[0].rows_removed, 2);
        assert_eq!(
            collapsed.get(&vec![1, 2, 3]).unwrap(),
            &vec![0b000, 0b011, 0b111]
        );

        let (pruned, dropped) = prune_included_tables(&collapsed, &pair_details);
        assert_eq!(dropped, vec![vec![1, 2]]);
        assert_eq!(to_tables(&pruned).len(), 1);
    }
}
