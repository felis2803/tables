use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{sort_dedup_rows, Table};
use crate::subset_absorption::{collapse_equal_bitsets, to_tables};

#[derive(Clone, Debug, Default, Serialize)]
pub struct SingleTableBitFilterInfo {
    pub removed_bits: usize,
    pub changed_tables: usize,
    pub removed_rows_after_projection_dedup: usize,
    pub collapsed_duplicate_tables: usize,
    pub zero_bit_tables_created: usize,
    pub removed_bits_by_input_arity: BTreeMap<String, usize>,
}

pub fn filter_single_table_bits(
    tables: &[Table],
) -> Result<(Vec<Table>, SingleTableBitFilterInfo)> {
    let protected_bits = BTreeSet::new();
    filter_single_table_bits_with_protected_bits(tables, &protected_bits)
}

pub fn filter_single_table_bits_with_protected_bits(
    tables: &[Table],
    protected_bits: &BTreeSet<u32>,
) -> Result<(Vec<Table>, SingleTableBitFilterInfo)> {
    let mut bit_counts: HashMap<u32, usize> = HashMap::new();
    for table in tables {
        for &bit in &table.bits {
            *bit_counts.entry(bit).or_insert(0) += 1;
        }
    }

    let mut projected = Vec::with_capacity(tables.len());
    let mut info = SingleTableBitFilterInfo::default();

    for table in tables {
        let mut keep_indices = Vec::with_capacity(table.bits.len());
        let mut kept_bits = Vec::with_capacity(table.bits.len());
        let mut removed_here = 0usize;

        for (index, &bit) in table.bits.iter().enumerate() {
            if bit_counts.get(&bit).copied().unwrap_or(0) == 1 && !protected_bits.contains(&bit) {
                removed_here += 1;
            } else {
                keep_indices.push(index);
                kept_bits.push(bit);
            }
        }

        if removed_here == 0 {
            projected.push(table.clone());
            continue;
        }

        info.removed_bits += removed_here;
        info.changed_tables += 1;
        *info
            .removed_bits_by_input_arity
            .entry(table.bits.len().to_string())
            .or_insert(0) += removed_here;

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
        let original_row_count = new_rows.len();
        sort_dedup_rows(&mut new_rows);
        info.removed_rows_after_projection_dedup += original_row_count - new_rows.len();

        if kept_bits.is_empty() {
            info.zero_bit_tables_created += 1;
        }

        projected.push(Table {
            bits: kept_bits,
            rows: new_rows,
        });
    }

    let (canonical, duplicate_count) = collapse_equal_bitsets(&projected);
    info.collapsed_duplicate_tables = duplicate_count;
    if let Some((bits, _)) = canonical.iter().find(|(_, rows)| rows.is_empty()) {
        bail!(
            "single-table bit filtering introduced contradiction on schema {:?}",
            bits
        );
    }

    Ok((to_tables(&canonical), info))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_single_table_bits_projects_unique_bits_and_dedups_rows() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b10],
            },
            Table {
                bits: vec![1, 3],
                rows: vec![0b00],
            },
            Table {
                bits: vec![3],
                rows: vec![0b0],
            },
        ];

        let (filtered, info) = filter_single_table_bits(&tables).unwrap();

        assert_eq!(info.removed_bits, 1);
        assert_eq!(info.changed_tables, 1);
        assert_eq!(info.removed_rows_after_projection_dedup, 1);
        assert_eq!(info.collapsed_duplicate_tables, 0);
        assert_eq!(info.zero_bit_tables_created, 0);
        assert_eq!(info.removed_bits_by_input_arity.get("2"), Some(&1));
        assert_eq!(
            filtered,
            vec![
                Table {
                    bits: vec![1],
                    rows: vec![0],
                },
                Table {
                    bits: vec![3],
                    rows: vec![0],
                },
                Table {
                    bits: vec![1, 3],
                    rows: vec![0],
                },
            ]
        );
    }

    #[test]
    fn filter_single_table_bits_collapses_duplicate_projected_schemas() {
        let tables = vec![
            Table {
                bits: vec![1, 2, 4],
                rows: vec![0b000, 0b010, 0b100, 0b110],
            },
            Table {
                bits: vec![1],
                rows: vec![0b0],
            },
        ];

        let (filtered, info) = filter_single_table_bits(&tables).unwrap();

        assert_eq!(info.removed_bits, 2);
        assert_eq!(info.changed_tables, 1);
        assert_eq!(info.removed_rows_after_projection_dedup, 3);
        assert_eq!(info.collapsed_duplicate_tables, 1);
        assert_eq!(
            filtered,
            vec![Table {
                bits: vec![1],
                rows: vec![0],
            }]
        );
    }

    #[test]
    fn filter_single_table_bits_bails_on_projected_schema_contradiction() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00],
            },
            Table {
                bits: vec![1, 3],
                rows: vec![0b01],
            },
        ];

        let error = filter_single_table_bits(&tables).unwrap_err();
        assert!(error
            .to_string()
            .contains("single-table bit filtering introduced contradiction"));
    }

    #[test]
    fn filter_single_table_bits_keeps_protected_unique_bits() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0b00, 0b10],
        }];
        let protected = BTreeSet::from([2u32]);

        let (filtered, info) =
            filter_single_table_bits_with_protected_bits(&tables, &protected).unwrap();

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
