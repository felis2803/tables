use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{ForcedRow, Table};
use crate::subset_absorption::{collapse_equal_bitsets, to_tables};

#[derive(Clone, Debug, Default, Serialize)]
pub struct ForcedPropagationStats {
    pub affected_tables: usize,
    pub changed_tables: usize,
    pub removed_rows: usize,
    pub collapsed_duplicate_tables: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ForcedBitsInfo {
    pub forced_bits: usize,
    pub forced_one_bits: usize,
    pub forced_zero_bits: usize,
    pub forced_occurrences: usize,
    pub stats: ForcedPropagationStats,
}

pub fn collect_forced_bits_bitwise(tables: &[Table]) -> Result<(BTreeMap<u32, u8>, usize)> {
    let mut forced = BTreeMap::new();
    let mut occurrences = 0usize;

    for table in tables {
        let full_mask = if table.bits.len() == 32 {
            u32::MAX
        } else {
            (1u32 << table.bits.len()) - 1
        };
        let mut and_mask = full_mask;
        let mut or_mask = 0u32;

        for &row in &table.rows {
            and_mask &= row;
            or_mask |= row;
        }

        let zero_mask = full_mask & !or_mask;
        for (offset, &bit) in table.bits.iter().enumerate() {
            let mask = 1u32 << offset;
            let value = if (and_mask & mask) != 0 {
                Some(1u8)
            } else if (zero_mask & mask) != 0 {
                Some(0u8)
            } else {
                None
            };

            let Some(value) = value else {
                continue;
            };

            if let Some(current) = forced.get(&bit) {
                if *current != value {
                    bail!("conflicting forced values for bit {bit}");
                }
            }

            forced.insert(bit, value);
            occurrences += 1;
        }
    }

    Ok((forced, occurrences))
}

pub fn propagate_forced_bits(
    tables: &[Table],
    forced: &BTreeMap<u32, u8>,
) -> Result<(Vec<Table>, ForcedPropagationStats)> {
    let mut projected = Vec::with_capacity(tables.len());
    let mut stats = ForcedPropagationStats::default();

    for table in tables {
        let touches_forced = table.bits.iter().any(|bit| forced.contains_key(bit));
        if touches_forced {
            stats.affected_tables += 1;
        }

        let mut kept_bits = Vec::with_capacity(table.bits.len());
        let mut kept_indices = Vec::with_capacity(table.bits.len());
        for (index, &bit) in table.bits.iter().enumerate() {
            if !forced.contains_key(&bit) {
                kept_bits.push(bit);
                kept_indices.push(index);
            }
        }

        let mut new_rows = Vec::with_capacity(table.rows.len());
        for &row in &table.rows {
            let mut consistent = true;
            for (index, &bit) in table.bits.iter().enumerate() {
                if let Some(&forced_value) = forced.get(&bit) {
                    if (((row >> index) & 1) as u8) != forced_value {
                        consistent = false;
                        break;
                    }
                }
            }

            if consistent {
                let mut new_row = 0u32;
                for (new_index, &old_index) in kept_indices.iter().enumerate() {
                    if ((row >> old_index) & 1) != 0 {
                        new_row |= 1u32 << new_index;
                    }
                }
                new_rows.push(new_row);
            }
        }

        new_rows.sort_unstable();
        new_rows.dedup();

        stats.removed_rows += table.rows.len() - new_rows.len();
        if touches_forced && (kept_bits != table.bits || new_rows != table.rows) {
            stats.changed_tables += 1;
        }

        if kept_bits.is_empty() {
            if new_rows == vec![0] {
                projected.push(Table {
                    bits: kept_bits,
                    rows: new_rows,
                });
                continue;
            }
            bail!("contradiction after forcing table {:?}", table.bits);
        }

        projected.push(Table {
            bits: kept_bits,
            rows: new_rows,
        });
    }

    let (canonical, duplicate_count) = collapse_equal_bitsets(&projected);
    stats.collapsed_duplicate_tables = duplicate_count;
    Ok((to_tables(&canonical), stats))
}

pub fn update_original_forced(
    original_mapping: &BTreeMap<u32, (u32, u8)>,
    original_forced: &mut BTreeMap<u32, u8>,
    forced_current: &BTreeMap<u32, u8>,
) -> Result<()> {
    for (&bit, &(current, inverted)) in original_mapping {
        let Some(&current_value) = forced_current.get(&current) else {
            continue;
        };
        let value = current_value ^ inverted;
        if let Some(existing) = original_forced.get(&bit) {
            if *existing != value {
                bail!("conflicting final value for original bit {bit}");
            }
        }
        original_forced.insert(bit, value);
    }
    Ok(())
}

pub fn forced_rows(original_forced: &BTreeMap<u32, u8>) -> Vec<ForcedRow> {
    original_forced
        .iter()
        .map(|(&bit, &value)| ForcedRow { bit, value })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_and_propagate_forced_bits() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0b10, 0b11],
        }];

        let (forced, occurrences) = collect_forced_bits_bitwise(&tables).unwrap();
        assert_eq!(occurrences, 1);
        assert_eq!(forced.get(&2), Some(&1));

        let (projected, stats) = propagate_forced_bits(&tables, &forced).unwrap();
        assert_eq!(stats.affected_tables, 1);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].bits, vec![1]);
        assert_eq!(projected[0].rows, vec![0, 1]);
    }
}
