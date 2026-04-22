use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::bit_zero_collapse::compute_bit_zero_collapse_metrics;
use crate::common::{sort_dedup_rows, Table};
use crate::subset_absorption::{collapse_equal_bitsets, to_tables};

#[derive(Clone, Debug, Default, Serialize)]
pub struct ZeroCollapseBitFilterInfo {
    pub removed_bits: usize,
    pub changed_tables: usize,
    pub removed_rows_after_projection_dedup: usize,
    pub collapsed_duplicate_tables: usize,
    pub zero_bit_tables_created: usize,
    pub projection_iterations: usize,
    pub removed_bits_by_input_arity: BTreeMap<String, usize>,
}

pub fn filter_zero_collapse_bits(
    tables: &[Table],
) -> Result<(Vec<Table>, ZeroCollapseBitFilterInfo)> {
    let protected_bits = BTreeSet::new();
    filter_zero_collapse_bits_with_protected_bits(tables, &protected_bits)
}

pub fn filter_zero_collapse_bits_with_protected_bits(
    tables: &[Table],
    protected_bits: &BTreeSet<u32>,
) -> Result<(Vec<Table>, ZeroCollapseBitFilterInfo)> {
    let mut projected = Vec::with_capacity(tables.len());
    let mut info = ZeroCollapseBitFilterInfo::default();

    for table in tables {
        let input_arity = table.bits.len();
        let mut current = table.clone();
        let mut removed_here = 0usize;

        while let Some(bit_index) = removable_bit_index(&current, protected_bits) {
            let previous_row_count = current.rows.len();
            current = project_away_bit(&current, bit_index);
            info.removed_rows_after_projection_dedup += previous_row_count - current.rows.len();
            info.projection_iterations += 1;
            removed_here += 1;
        }

        if removed_here > 0 {
            info.removed_bits += removed_here;
            info.changed_tables += 1;
            *info
                .removed_bits_by_input_arity
                .entry(input_arity.to_string())
                .or_insert(0) += removed_here;
            if current.bits.is_empty() {
                info.zero_bit_tables_created += 1;
            }
        }

        projected.push(current);
    }

    let (canonical, duplicate_count) = collapse_equal_bitsets(&projected);
    info.collapsed_duplicate_tables = duplicate_count;
    if let Some((bits, _)) = canonical.iter().find(|(_, rows)| rows.is_empty()) {
        bail!(
            "zero-collapse bit filtering introduced contradiction on schema {:?}",
            bits
        );
    }

    Ok((to_tables(&canonical), info))
}

fn removable_bit_index(table: &Table, protected_bits: &BTreeSet<u32>) -> Option<usize> {
    compute_bit_zero_collapse_metrics(table)
        .into_iter()
        .find(|metric| {
            metric.row_count_after_zeroing * 2 == metric.row_count_before
                && !protected_bits.contains(&metric.bit)
        })
        .map(|metric| metric.bit_index)
}

fn project_away_bit(table: &Table, removed_bit_index: usize) -> Table {
    let mut kept_bits = Vec::with_capacity(table.bits.len().saturating_sub(1));
    let mut keep_indices = Vec::with_capacity(table.bits.len().saturating_sub(1));

    for (index, &bit) in table.bits.iter().enumerate() {
        if index != removed_bit_index {
            kept_bits.push(bit);
            keep_indices.push(index);
        }
    }

    let mut new_rows = Vec::with_capacity(table.rows.len());
    for &row in &table.rows {
        let mut new_row = 0u32;
        for (new_index, &old_index) in keep_indices.iter().enumerate() {
            if ((row >> old_index) & 1) != 0 {
                new_row |= 1u32 << new_index;
            }
        }
        new_rows.push(new_row);
    }
    sort_dedup_rows(&mut new_rows);

    Table {
        bits: kept_bits,
        rows: new_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_zero_collapse_bits_removes_locally_unrestricted_bit() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b01, 0b10, 0b11],
            },
            Table {
                bits: vec![3],
                rows: vec![0],
            },
        ];

        let (filtered, info) = filter_zero_collapse_bits(&tables).unwrap();

        assert_eq!(info.removed_bits, 2);
        assert_eq!(info.changed_tables, 1);
        assert_eq!(info.removed_rows_after_projection_dedup, 3);
        assert_eq!(info.collapsed_duplicate_tables, 0);
        assert_eq!(info.zero_bit_tables_created, 1);
        assert_eq!(info.projection_iterations, 2);
        assert_eq!(info.removed_bits_by_input_arity.get("2"), Some(&2));
        assert_eq!(
            filtered,
            vec![
                Table {
                    bits: vec![],
                    rows: vec![0],
                },
                Table {
                    bits: vec![3],
                    rows: vec![0],
                },
            ]
        );
    }

    #[test]
    fn filter_zero_collapse_bits_preserves_significant_bit() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0b00, 0b01, 0b10],
        }];

        let (filtered, info) = filter_zero_collapse_bits(&tables).unwrap();

        assert_eq!(info.removed_bits, 0);
        assert_eq!(info.changed_tables, 0);
        assert_eq!(info.projection_iterations, 0);
        assert_eq!(filtered, tables);
    }

    #[test]
    fn filter_zero_collapse_bits_collapses_duplicate_projected_schemas() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b01],
            },
            Table {
                bits: vec![2],
                rows: vec![0],
            },
        ];

        let (filtered, info) = filter_zero_collapse_bits(&tables).unwrap();

        assert_eq!(info.removed_bits, 1);
        assert_eq!(info.changed_tables, 1);
        assert_eq!(info.removed_rows_after_projection_dedup, 1);
        assert_eq!(info.collapsed_duplicate_tables, 1);
        assert_eq!(
            filtered,
            vec![Table {
                bits: vec![2],
                rows: vec![0],
            }]
        );
    }

    #[test]
    fn filter_zero_collapse_bits_keeps_protected_bits() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0b00, 0b01, 0b10, 0b11],
        }];
        let protected = BTreeSet::from([2u32]);

        let (filtered, info) =
            filter_zero_collapse_bits_with_protected_bits(&tables, &protected).unwrap();

        assert_eq!(info.removed_bits, 1);
        assert_eq!(
            filtered,
            vec![Table {
                bits: vec![2],
                rows: vec![0, 1],
            }]
        );
    }
}
