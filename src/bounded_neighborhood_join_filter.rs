use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::common::{project_row, sort_dedup_rows, Table};
use crate::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug, Serialize)]
pub struct BoundedNeighborhoodJoinSettings {
    pub max_union_bits: usize,
    pub max_tables_per_neighborhood: usize,
    pub min_tables_per_neighborhood: usize,
}

impl Default for BoundedNeighborhoodJoinSettings {
    fn default() -> Self {
        Self {
            max_union_bits: 32,
            max_tables_per_neighborhood: 10,
            min_tables_per_neighborhood: 3,
        }
    }
}

impl BoundedNeighborhoodJoinSettings {
    pub fn validate(&self) -> Result<()> {
        if self.max_union_bits == 0 || self.max_union_bits > 32 {
            bail!(
                "bounded neighborhood join max_union_bits must be in 1..=32, got {}",
                self.max_union_bits
            );
        }
        if self.max_tables_per_neighborhood < 2 {
            bail!(
                "bounded neighborhood join max_tables_per_neighborhood must be at least 2, got {}",
                self.max_tables_per_neighborhood
            );
        }
        if self.min_tables_per_neighborhood < 2 {
            bail!(
                "bounded neighborhood join min_tables_per_neighborhood must be at least 2, got {}",
                self.min_tables_per_neighborhood
            );
        }
        if self.min_tables_per_neighborhood > self.max_tables_per_neighborhood {
            bail!(
                "bounded neighborhood join min_tables_per_neighborhood {} exceeds max_tables_per_neighborhood {}",
                self.min_tables_per_neighborhood,
                self.max_tables_per_neighborhood
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BoundedNeighborhoodJoinInfo {
    pub max_union_bits: usize,
    pub max_tables_per_neighborhood: usize,
    pub min_tables_per_neighborhood: usize,
    pub candidate_anchor_tables: usize,
    pub joined_anchor_tables: usize,
    pub changed_tables: usize,
    pub removed_rows: usize,
    pub max_selected_union_bits: usize,
    pub max_selected_table_count: usize,
}

#[derive(Clone, Debug)]
struct NeighborhoodSelection {
    table_indices: Vec<usize>,
    union_bits: usize,
}

#[derive(Clone, Debug)]
struct NeighborCandidate {
    table_index: usize,
    shared_with_anchor: usize,
}

pub fn filter_tables_by_bounded_neighborhood_join(
    tables: &[Table],
) -> Result<(Vec<Table>, BoundedNeighborhoodJoinInfo)> {
    filter_tables_by_bounded_neighborhood_join_with_settings(
        tables,
        &BoundedNeighborhoodJoinSettings::default(),
    )
}

pub fn filter_tables_by_bounded_neighborhood_join_with_settings(
    tables: &[Table],
    settings: &BoundedNeighborhoodJoinSettings,
) -> Result<(Vec<Table>, BoundedNeighborhoodJoinInfo)> {
    settings.validate()?;
    let bit_to_tables = build_bit_to_tables(tables);
    let mut output = Vec::with_capacity(tables.len());
    let mut info = BoundedNeighborhoodJoinInfo {
        max_union_bits: settings.max_union_bits,
        max_tables_per_neighborhood: settings.max_tables_per_neighborhood,
        min_tables_per_neighborhood: settings.min_tables_per_neighborhood,
        ..BoundedNeighborhoodJoinInfo::default()
    };

    for (anchor_index, anchor) in tables.iter().enumerate() {
        let Some(selection) = select_neighborhood(anchor_index, tables, &bit_to_tables, settings)
        else {
            output.push(anchor.clone());
            continue;
        };

        info.candidate_anchor_tables += 1;
        info.joined_anchor_tables += 1;
        info.max_selected_union_bits = info.max_selected_union_bits.max(selection.union_bits);
        info.max_selected_table_count = info
            .max_selected_table_count
            .max(selection.table_indices.len());

        let projected_rows = join_and_project_anchor_rows(anchor, &selection.table_indices, tables)
            .with_context(|| {
                format!(
                    "bounded neighborhood join failed for anchor schema {:?}",
                    anchor.bits
                )
            })?;

        if projected_rows.is_empty() {
            bail!(
                "bounded neighborhood join introduced contradiction on schema {:?}",
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

fn join_and_project_anchor_rows(
    anchor: &Table,
    selection_table_indices: &[usize],
    tables: &[Table],
) -> Result<Vec<u32>> {
    let mut merged_bits = anchor.bits.clone();
    let mut merged_rows = anchor.rows.clone();

    for &table_index in selection_table_indices.iter().skip(1) {
        let neighbor = &tables[table_index];
        let merged = merge_tables_fast_from_slices(
            &merged_bits,
            &merged_rows,
            &neighbor.bits,
            &neighbor.rows,
        )
        .map_err(|error| anyhow::anyhow!(error))?;
        merged_bits = merged.bits;
        merged_rows = merged.rows;
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
    Ok(projected_rows)
}

fn select_neighborhood(
    anchor_index: usize,
    tables: &[Table],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    settings: &BoundedNeighborhoodJoinSettings,
) -> Option<NeighborhoodSelection> {
    let anchor = &tables[anchor_index];
    if anchor.bits.len() > settings.max_union_bits {
        return None;
    }

    let mut overlap_counts: HashMap<usize, usize> = HashMap::new();
    for &bit in &anchor.bits {
        let Some(table_ids) = bit_to_tables.get(&bit) else {
            continue;
        };
        for &table_index in table_ids {
            if table_index == anchor_index {
                continue;
            }
            *overlap_counts.entry(table_index).or_insert(0) += 1;
        }
    }

    if overlap_counts.is_empty() {
        return None;
    }

    let mut candidates: Vec<NeighborCandidate> = overlap_counts
        .into_iter()
        .map(|(table_index, shared_with_anchor)| NeighborCandidate {
            table_index,
            shared_with_anchor,
        })
        .collect();

    if candidates.len() + 1 < settings.min_tables_per_neighborhood {
        return None;
    }

    let mut selected_table_indices = vec![anchor_index];
    let mut selected_bits: HashSet<u32> = anchor.bits.iter().copied().collect();

    while selected_table_indices.len() < settings.max_tables_per_neighborhood {
        let mut best_choice: Option<(usize, (usize, usize, usize, usize, usize))> = None;

        for (candidate_pos, candidate) in candidates.iter().enumerate() {
            let neighbor = &tables[candidate.table_index];
            let added_bits = neighbor
                .bits
                .iter()
                .filter(|bit| !selected_bits.contains(bit))
                .count();
            let next_union_bits = selected_bits.len() + added_bits;
            if next_union_bits > settings.max_union_bits {
                continue;
            }

            let ranking = (
                candidate.shared_with_anchor,
                usize::MAX - added_bits,
                usize::MAX - neighbor.bits.len(),
                usize::MAX - neighbor.rows.len(),
                usize::MAX - candidate.table_index,
            );
            if best_choice
                .as_ref()
                .is_none_or(|(_, current_ranking)| ranking > *current_ranking)
            {
                best_choice = Some((candidate_pos, ranking));
            }
        }

        let Some((candidate_pos, _)) = best_choice else {
            break;
        };

        let candidate = candidates.swap_remove(candidate_pos);
        for &bit in &tables[candidate.table_index].bits {
            selected_bits.insert(bit);
        }
        selected_table_indices.push(candidate.table_index);
    }

    if selected_table_indices.len() < settings.min_tables_per_neighborhood {
        return None;
    }

    Some(NeighborhoodSelection {
        table_indices: selected_table_indices,
        union_bits: selected_bits.len(),
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_neighborhood_join_removes_jointly_unsupported_rows() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b01],
            },
            Table {
                bits: vec![1, 3],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b01, 0b10],
            },
        ];

        let (filtered, info) = filter_tables_by_bounded_neighborhood_join(&tables).unwrap();

        assert_eq!(info.joined_anchor_tables, 3);
        assert_eq!(info.changed_tables, 3);
        assert_eq!(info.removed_rows, 3);
        assert_eq!(
            filtered,
            vec![
                Table {
                    bits: vec![1, 2],
                    rows: vec![0b01],
                },
                Table {
                    bits: vec![1, 3],
                    rows: vec![0b11],
                },
                Table {
                    bits: vec![2, 3],
                    rows: vec![0b10],
                },
            ]
        );
    }

    #[test]
    fn bounded_neighborhood_join_skips_when_no_three_table_neighborhood_fits() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b01],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b00, 0b01],
            },
            Table {
                bits: vec![9, 10],
                rows: vec![0b00, 0b01],
            },
        ];

        let (filtered, info) = filter_tables_by_bounded_neighborhood_join(&tables).unwrap();

        assert_eq!(info.joined_anchor_tables, 0);
        assert_eq!(info.changed_tables, 0);
        assert_eq!(filtered, tables);
    }

    #[test]
    fn bounded_neighborhood_join_bails_on_contradiction() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00],
            },
            Table {
                bits: vec![1, 3],
                rows: vec![0b00],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b10],
            },
        ];

        let error = filter_tables_by_bounded_neighborhood_join(&tables).unwrap_err();
        assert!(error
            .to_string()
            .contains("bounded neighborhood join introduced contradiction"));
    }

    #[test]
    fn bounded_neighborhood_join_respects_custom_settings() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0b00, 0b01],
            },
            Table {
                bits: vec![1, 3],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0b01, 0b10],
            },
        ];
        let settings = BoundedNeighborhoodJoinSettings {
            max_union_bits: 2,
            max_tables_per_neighborhood: 2,
            min_tables_per_neighborhood: 2,
        };

        let (filtered, info) =
            filter_tables_by_bounded_neighborhood_join_with_settings(&tables, &settings).unwrap();

        assert_eq!(info.max_union_bits, 2);
        assert_eq!(info.max_tables_per_neighborhood, 2);
        assert_eq!(info.min_tables_per_neighborhood, 2);
        assert_eq!(info.removed_rows, 0);
        assert_eq!(filtered, tables);
    }
}
