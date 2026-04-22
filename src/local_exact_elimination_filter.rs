use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::common::{project_row, sort_dedup_rows, Table};
use crate::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug, Serialize)]
pub struct LocalExactEliminationSettings {
    pub max_union_bits: usize,
    pub max_tables_per_component: usize,
    pub min_tables_per_component: usize,
}

impl Default for LocalExactEliminationSettings {
    fn default() -> Self {
        Self {
            max_union_bits: 32,
            max_tables_per_component: 12,
            min_tables_per_component: 4,
        }
    }
}

impl LocalExactEliminationSettings {
    pub fn validate(&self) -> Result<()> {
        if self.max_union_bits == 0 || self.max_union_bits > 32 {
            bail!(
                "local exact elimination max_union_bits must be in 1..=32, got {}",
                self.max_union_bits
            );
        }
        if self.max_tables_per_component < 2 {
            bail!(
                "local exact elimination max_tables_per_component must be at least 2, got {}",
                self.max_tables_per_component
            );
        }
        if self.min_tables_per_component < 2 {
            bail!(
                "local exact elimination min_tables_per_component must be at least 2, got {}",
                self.min_tables_per_component
            );
        }
        if self.min_tables_per_component > self.max_tables_per_component {
            bail!(
                "local exact elimination min_tables_per_component {} exceeds max_tables_per_component {}",
                self.min_tables_per_component,
                self.max_tables_per_component
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LocalExactEliminationInfo {
    pub max_union_bits: usize,
    pub max_tables_per_component: usize,
    pub min_tables_per_component: usize,
    pub candidate_anchor_tables: usize,
    pub exact_component_anchor_tables: usize,
    pub changed_tables: usize,
    pub removed_rows: usize,
    pub max_selected_union_bits: usize,
    pub max_selected_table_count: usize,
    pub max_intermediate_row_count: usize,
}

#[derive(Clone, Debug)]
struct ComponentSelection {
    table_indices: Vec<usize>,
    union_bits: usize,
}

#[derive(Clone, Debug, Default)]
struct FrontierScore {
    shared_with_selected: usize,
}

pub fn filter_tables_by_local_exact_elimination(
    tables: &[Table],
) -> Result<(Vec<Table>, LocalExactEliminationInfo)> {
    filter_tables_by_local_exact_elimination_with_settings(
        tables,
        &LocalExactEliminationSettings::default(),
    )
}

pub fn filter_tables_by_local_exact_elimination_with_settings(
    tables: &[Table],
    settings: &LocalExactEliminationSettings,
) -> Result<(Vec<Table>, LocalExactEliminationInfo)> {
    settings.validate()?;
    let bit_to_tables = build_bit_to_tables(tables);
    let anchor_overlap_counts = build_anchor_overlap_counts(tables, &bit_to_tables);
    let mut output = Vec::with_capacity(tables.len());
    let mut info = LocalExactEliminationInfo {
        max_union_bits: settings.max_union_bits,
        max_tables_per_component: settings.max_tables_per_component,
        min_tables_per_component: settings.min_tables_per_component,
        ..LocalExactEliminationInfo::default()
    };

    for (anchor_index, anchor) in tables.iter().enumerate() {
        let Some(selection) = select_component(
            anchor_index,
            tables,
            &bit_to_tables,
            &anchor_overlap_counts[anchor_index],
            settings,
        ) else {
            output.push(anchor.clone());
            continue;
        };

        info.candidate_anchor_tables += 1;
        info.exact_component_anchor_tables += 1;
        info.max_selected_union_bits = info.max_selected_union_bits.max(selection.union_bits);
        info.max_selected_table_count = info
            .max_selected_table_count
            .max(selection.table_indices.len());

        let (projected_rows, max_intermediate_row_count) =
            exact_join_project_anchor_rows(anchor_index, &selection.table_indices, tables)
                .with_context(|| {
                    format!(
                        "local exact elimination failed for anchor schema {:?}",
                        anchor.bits
                    )
                })?;
        info.max_intermediate_row_count = info
            .max_intermediate_row_count
            .max(max_intermediate_row_count);

        if projected_rows.is_empty() {
            bail!(
                "local exact elimination introduced contradiction on schema {:?}",
                anchor.bits
            );
        }

        if projected_rows.len() != anchor.rows.len() {
            info.changed_tables += 1;
            info.removed_rows += anchor.rows.len() - projected_rows.len();
            output.push(Table {
                bits: anchor.bits.clone(),
                rows: projected_rows,
            });
        } else {
            output.push(anchor.clone());
        }
    }

    Ok((output, info))
}

fn exact_join_project_anchor_rows(
    anchor_index: usize,
    selection_table_indices: &[usize],
    tables: &[Table],
) -> Result<(Vec<u32>, usize)> {
    let anchor = &tables[anchor_index];
    let mut merged_bits = anchor.bits.clone();
    let mut merged_rows = anchor.rows.clone();
    let mut max_intermediate_row_count = merged_rows.len();
    let mut remaining: Vec<usize> = selection_table_indices
        .iter()
        .copied()
        .filter(|&table_index| table_index != anchor_index)
        .collect();

    while !remaining.is_empty() {
        let best_pos = remaining
            .iter()
            .enumerate()
            .max_by_key(|(_, table_index)| {
                let table = &tables[**table_index];
                let shared = count_shared_bits(&merged_bits, &table.bits);
                let added_bits = table.bits.len() - shared;
                (
                    shared,
                    usize::MAX - added_bits,
                    usize::MAX - table.rows.len(),
                    usize::MAX - table.bits.len(),
                    usize::MAX - **table_index,
                )
            })
            .map(|(pos, _)| pos)
            .unwrap();
        let table_index = remaining.swap_remove(best_pos);
        let table = &tables[table_index];
        let merged =
            merge_tables_fast_from_slices(&merged_bits, &merged_rows, &table.bits, &table.rows)
                .map_err(anyhow::Error::msg)?;
        merged_bits = merged.bits;
        merged_rows = merged.rows;
        max_intermediate_row_count = max_intermediate_row_count.max(merged_rows.len());
        if merged_rows.is_empty() {
            break;
        }
    }

    let anchor_projection_indices: Vec<usize> = anchor
        .bits
        .iter()
        .map(|bit| merged_bits.binary_search(bit).unwrap())
        .collect();
    let mut projected_rows: Vec<u32> = merged_rows
        .iter()
        .copied()
        .map(|row| project_row(row, &anchor_projection_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);
    Ok((projected_rows, max_intermediate_row_count))
}

fn select_component(
    anchor_index: usize,
    tables: &[Table],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    anchor_overlap_counts: &HashMap<usize, usize>,
    settings: &LocalExactEliminationSettings,
) -> Option<ComponentSelection> {
    let anchor = &tables[anchor_index];
    if anchor.bits.len() > settings.max_union_bits {
        return None;
    }

    let mut selected_indices = vec![anchor_index];
    let mut selected_set = HashSet::from([anchor_index]);
    let mut selected_bits: HashSet<u32> = anchor.bits.iter().copied().collect();

    loop {
        if selected_indices.len() >= settings.max_tables_per_component {
            break;
        }

        let frontier_scores = build_frontier_scores(&selected_bits, &selected_set, bit_to_tables);
        let Some(next_index) = frontier_scores
            .iter()
            .filter_map(|(&table_index, score)| {
                let table = &tables[table_index];
                let added_bits = table
                    .bits
                    .iter()
                    .filter(|bit| !selected_bits.contains(bit))
                    .count();
                let next_union_bits = selected_bits.len() + added_bits;
                if next_union_bits > settings.max_union_bits {
                    return None;
                }
                let shared_with_anchor = anchor_overlap_counts
                    .get(&table_index)
                    .copied()
                    .unwrap_or_default();
                Some((
                    table_index,
                    (
                        score.shared_with_selected,
                        shared_with_anchor,
                        usize::MAX - added_bits,
                        usize::MAX - table.rows.len(),
                        usize::MAX - table.bits.len(),
                        usize::MAX - table_index,
                    ),
                ))
            })
            .max_by_key(|(_, ranking)| *ranking)
            .map(|(table_index, _)| table_index)
        else {
            break;
        };

        selected_set.insert(next_index);
        selected_indices.push(next_index);
        selected_bits.extend(tables[next_index].bits.iter().copied());
    }

    if selected_indices.len() < settings.min_tables_per_component {
        return None;
    }

    Some(ComponentSelection {
        table_indices: selected_indices,
        union_bits: selected_bits.len(),
    })
}

fn build_frontier_scores(
    selected_bits: &HashSet<u32>,
    selected_set: &HashSet<usize>,
    bit_to_tables: &HashMap<u32, Vec<usize>>,
) -> HashMap<usize, FrontierScore> {
    let mut frontier_scores: HashMap<usize, FrontierScore> = HashMap::new();

    for &bit in selected_bits {
        let Some(table_indices) = bit_to_tables.get(&bit) else {
            continue;
        };
        for &table_index in table_indices {
            if selected_set.contains(&table_index) {
                continue;
            }
            frontier_scores
                .entry(table_index)
                .or_default()
                .shared_with_selected += 1;
        }
    }

    frontier_scores
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

fn build_anchor_overlap_counts(
    tables: &[Table],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
) -> Vec<HashMap<usize, usize>> {
    tables
        .iter()
        .enumerate()
        .map(|(anchor_index, anchor)| {
            let mut overlap_counts = HashMap::new();
            for &bit in &anchor.bits {
                let Some(table_indices) = bit_to_tables.get(&bit) else {
                    continue;
                };
                for &table_index in table_indices {
                    if table_index == anchor_index {
                        continue;
                    }
                    *overlap_counts.entry(table_index).or_insert(0) += 1;
                }
            }
            overlap_counts
        })
        .collect()
}

fn count_shared_bits(left_bits: &[u32], right_bits: &[u32]) -> usize {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut shared = 0usize;

    while left_index < left_bits.len() && right_index < right_bits.len() {
        match left_bits[left_index].cmp(&right_bits[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }

    shared
}

#[cfg(test)]
mod tests {
    use super::{
        filter_tables_by_local_exact_elimination_with_settings, LocalExactEliminationSettings,
    };
    use crate::common::Table;

    #[test]
    fn prunes_anchor_rows_using_connected_component() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b10],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b01, 0b10],
            },
            Table {
                bits: vec![3, 4],
                rows: vec![0b01],
            },
        ];
        let settings = LocalExactEliminationSettings {
            max_union_bits: 8,
            max_tables_per_component: 4,
            min_tables_per_component: 3,
        };

        let (filtered, info) =
            filter_tables_by_local_exact_elimination_with_settings(&tables, &settings).unwrap();

        assert!(info.changed_tables >= 1);
        assert!(info.removed_rows >= 1);
        assert_eq!(filtered[0].rows, vec![0b00]);
    }

    #[test]
    fn skips_when_component_too_small() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b10],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b01, 0b10],
            },
            Table {
                bits: vec![8, 9],
                rows: vec![0b00],
            },
        ];
        let settings = LocalExactEliminationSettings {
            max_union_bits: 8,
            max_tables_per_component: 4,
            min_tables_per_component: 3,
        };

        let (filtered, info) =
            filter_tables_by_local_exact_elimination_with_settings(&tables, &settings).unwrap();

        assert_eq!(info.exact_component_anchor_tables, 0);
        assert_eq!(filtered, tables);
    }
}
