use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::common::{project_row, Table};
use crate::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug, Serialize)]
pub struct TriangleConsistencySettings {
    pub max_union_bits: usize,
    pub max_neighbors_considered: usize,
    pub max_triangle_pairs_per_anchor: usize,
    pub min_shared_bits_between_outer_tables: usize,
    pub max_outer_pair_row_product: usize,
}

impl Default for TriangleConsistencySettings {
    fn default() -> Self {
        Self {
            max_union_bits: 32,
            max_neighbors_considered: 8,
            max_triangle_pairs_per_anchor: 8,
            min_shared_bits_between_outer_tables: 1,
            max_outer_pair_row_product: 2_000_000,
        }
    }
}

impl TriangleConsistencySettings {
    pub fn validate(&self) -> Result<()> {
        if self.max_union_bits == 0 || self.max_union_bits > 32 {
            bail!(
                "triangle consistency max_union_bits must be in 1..=32, got {}",
                self.max_union_bits
            );
        }
        if self.max_neighbors_considered < 2 {
            bail!(
                "triangle consistency max_neighbors_considered must be at least 2, got {}",
                self.max_neighbors_considered
            );
        }
        if self.max_triangle_pairs_per_anchor == 0 {
            bail!("triangle consistency max_triangle_pairs_per_anchor must be positive");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TriangleConsistencyInfo {
    pub max_union_bits: usize,
    pub max_neighbors_considered: usize,
    pub max_triangle_pairs_per_anchor: usize,
    pub min_shared_bits_between_outer_tables: usize,
    pub max_outer_pair_row_product: usize,
    pub candidate_anchor_tables: usize,
    pub anchors_with_triangle_pairs: usize,
    pub checked_triangle_pairs: usize,
    pub informative_triangle_pairs: usize,
    pub changed_tables: usize,
    pub removed_rows: usize,
    pub max_selected_neighbors: usize,
    pub max_selected_triangle_pairs: usize,
}

#[derive(Clone, Debug)]
struct NeighborCandidate {
    table_index: usize,
    shared_with_anchor: usize,
}

#[derive(Clone, Debug)]
struct TriangleCandidate {
    left_index: usize,
    right_index: usize,
    union_bits: usize,
    outer_shared_bits: usize,
    shared_with_anchor_sum: usize,
    outer_row_product: usize,
}

pub fn filter_tables_by_triangle_consistency(
    tables: &[Table],
    settings: &TriangleConsistencySettings,
) -> Result<(Vec<Table>, TriangleConsistencyInfo)> {
    settings.validate()?;
    let bit_to_tables = build_bit_to_tables(tables);
    let mut output = Vec::with_capacity(tables.len());
    let mut info = TriangleConsistencyInfo {
        max_union_bits: settings.max_union_bits,
        max_neighbors_considered: settings.max_neighbors_considered,
        max_triangle_pairs_per_anchor: settings.max_triangle_pairs_per_anchor,
        min_shared_bits_between_outer_tables: settings.min_shared_bits_between_outer_tables,
        max_outer_pair_row_product: settings.max_outer_pair_row_product,
        ..TriangleConsistencyInfo::default()
    };

    for (anchor_index, anchor) in tables.iter().enumerate() {
        let selected_neighbors = select_neighbors(
            anchor_index,
            tables,
            &bit_to_tables,
            settings.max_neighbors_considered,
        );
        if selected_neighbors.len() < 2 {
            output.push(anchor.clone());
            continue;
        }
        info.candidate_anchor_tables += 1;
        info.max_selected_neighbors = info.max_selected_neighbors.max(selected_neighbors.len());

        let triangle_pairs =
            select_triangle_pairs(anchor_index, anchor, tables, &selected_neighbors, settings);
        if triangle_pairs.is_empty() {
            output.push(anchor.clone());
            continue;
        }

        info.anchors_with_triangle_pairs += 1;
        info.max_selected_triangle_pairs =
            info.max_selected_triangle_pairs.max(triangle_pairs.len());

        let mut keep_mask = vec![true; anchor.rows.len()];
        for triangle in &triangle_pairs {
            info.checked_triangle_pairs += 1;
            let Some(allowed_projections) = allowed_anchor_projections(anchor, triangle, tables)
                .with_context(|| {
                    format!(
                        "triangle consistency failed for anchor schema {:?}",
                        anchor.bits
                    )
                })?
            else {
                continue;
            };
            info.informative_triangle_pairs += 1;

            let anchor_projection_indices: Vec<usize> = anchor
                .bits
                .iter()
                .enumerate()
                .filter_map(|(index, bit)| {
                    allowed_projections
                        .anchor_bits
                        .binary_search(bit)
                        .ok()
                        .map(|_| index)
                })
                .collect();

            for (&row, keep) in anchor.rows.iter().zip(keep_mask.iter_mut()) {
                if !*keep {
                    continue;
                }
                let projected = project_row(row, &anchor_projection_indices);
                if !allowed_projections.rows.contains(&projected) {
                    *keep = false;
                }
            }
        }

        let kept_rows = keep_mask.iter().filter(|&&keep| keep).count();
        if kept_rows == anchor.rows.len() {
            output.push(anchor.clone());
            continue;
        }
        if kept_rows == 0 {
            bail!(
                "triangle consistency introduced contradiction on schema {:?}",
                anchor.bits
            );
        }

        info.changed_tables += 1;
        info.removed_rows += anchor.rows.len() - kept_rows;
        let rows = anchor
            .rows
            .iter()
            .copied()
            .zip(keep_mask.into_iter())
            .filter_map(|(row, keep)| keep.then_some(row))
            .collect();
        output.push(Table {
            bits: anchor.bits.clone(),
            rows,
        });
    }

    Ok((output, info))
}

struct AllowedAnchorProjections {
    anchor_bits: Vec<u32>,
    rows: HashSet<u32>,
}

fn allowed_anchor_projections(
    anchor: &Table,
    triangle: &TriangleCandidate,
    tables: &[Table],
) -> Result<Option<AllowedAnchorProjections>> {
    let left = &tables[triangle.left_index];
    let right = &tables[triangle.right_index];
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(anyhow::Error::msg)?;
    if merged.rows.is_empty() {
        bail!(
            "triangle consistency found empty outer merge for schemas {:?} and {:?}",
            left.bits,
            right.bits
        );
    }

    let anchor_bits: Vec<u32> = anchor
        .bits
        .iter()
        .copied()
        .filter(|bit| merged.bits.binary_search(bit).is_ok())
        .collect();
    if anchor_bits.is_empty() {
        return Ok(None);
    }

    let merged_indices: Vec<usize> = anchor_bits
        .iter()
        .map(|bit| merged.bits.binary_search(bit).unwrap())
        .collect();
    let rows = merged
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &merged_indices))
        .collect();

    Ok(Some(AllowedAnchorProjections { anchor_bits, rows }))
}

fn select_neighbors(
    anchor_index: usize,
    tables: &[Table],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    max_neighbors_considered: usize,
) -> Vec<NeighborCandidate> {
    let anchor = &tables[anchor_index];
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

    let mut neighbors: Vec<_> = overlap_counts
        .into_iter()
        .map(|(table_index, shared_with_anchor)| NeighborCandidate {
            table_index,
            shared_with_anchor,
        })
        .collect();
    neighbors.sort_by_key(|candidate| {
        (
            Reverse(candidate.shared_with_anchor),
            tables[candidate.table_index].bits.len(),
            tables[candidate.table_index].rows.len(),
            candidate.table_index,
        )
    });
    neighbors.truncate(max_neighbors_considered);
    neighbors
}

fn select_triangle_pairs(
    _anchor_index: usize,
    anchor: &Table,
    tables: &[Table],
    neighbors: &[NeighborCandidate],
    settings: &TriangleConsistencySettings,
) -> Vec<TriangleCandidate> {
    let mut pairs = Vec::new();

    for left_pos in 0..neighbors.len() {
        for right_pos in (left_pos + 1)..neighbors.len() {
            let left = &neighbors[left_pos];
            let right = &neighbors[right_pos];
            let outer_shared_bits = count_shared_bits(
                &tables[left.table_index].bits,
                &tables[right.table_index].bits,
            );
            if outer_shared_bits < settings.min_shared_bits_between_outer_tables {
                continue;
            }

            let outer_row_product = tables[left.table_index]
                .rows
                .len()
                .saturating_mul(tables[right.table_index].rows.len());
            if outer_row_product > settings.max_outer_pair_row_product {
                continue;
            }

            let union_bits = union_len_three(
                &anchor.bits,
                &tables[left.table_index].bits,
                &tables[right.table_index].bits,
            );
            if union_bits > settings.max_union_bits {
                continue;
            }

            pairs.push(TriangleCandidate {
                left_index: left.table_index,
                right_index: right.table_index,
                union_bits,
                outer_shared_bits,
                shared_with_anchor_sum: left.shared_with_anchor + right.shared_with_anchor,
                outer_row_product,
            });
        }
    }

    pairs.sort_by_key(|pair| {
        (
            Reverse(pair.outer_shared_bits),
            Reverse(pair.shared_with_anchor_sum),
            pair.union_bits,
            pair.outer_row_product,
            pair.left_index,
            pair.right_index,
        )
    });
    pairs.truncate(settings.max_triangle_pairs_per_anchor);
    pairs
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

fn count_shared_bits(left: &[u32], right: &[u32]) -> usize {
    let mut count = 0usize;
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                count += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }
    count
}

fn union_len_three(a: &[u32], b: &[u32], c: &[u32]) -> usize {
    let mut union = HashSet::with_capacity(a.len() + b.len() + c.len());
    union.extend(a.iter().copied());
    union.extend(b.iter().copied());
    union.extend(c.iter().copied());
    union.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triangle_consistency_removes_jointly_unsupported_anchor_row() {
        let tables = vec![
            Table {
                bits: vec![1, 4],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b101],
            },
            Table {
                bits: vec![3, 4],
                rows: vec![0b00, 0b01],
            },
        ];
        let settings = TriangleConsistencySettings {
            max_neighbors_considered: 2,
            max_triangle_pairs_per_anchor: 1,
            ..TriangleConsistencySettings::default()
        };

        let (filtered, info) = filter_tables_by_triangle_consistency(&tables, &settings).unwrap();

        assert_eq!(info.changed_tables, 3);
        assert_eq!(info.removed_rows, 3);
        assert_eq!(
            filtered[1],
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000],
            }
        );
    }

    #[test]
    fn triangle_consistency_skips_when_outer_tables_do_not_share_bits() {
        let tables = vec![
            Table {
                bits: vec![1, 4],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b101],
            },
            Table {
                bits: vec![3, 5],
                rows: vec![0b00, 0b01],
            },
        ];

        let (filtered, info) =
            filter_tables_by_triangle_consistency(&tables, &TriangleConsistencySettings::default())
                .unwrap();

        assert_eq!(info.anchors_with_triangle_pairs, 0);
        assert_eq!(info.changed_tables, 0);
        assert_eq!(filtered, tables);
    }

    #[test]
    fn triangle_consistency_respects_outer_pair_product_bound() {
        let tables = vec![
            Table {
                bits: vec![1, 4],
                rows: vec![0b00, 0b11],
            },
            Table {
                bits: vec![1, 2, 3],
                rows: vec![0b000, 0b101],
            },
            Table {
                bits: vec![3, 4],
                rows: vec![0b00, 0b01],
            },
        ];
        let settings = TriangleConsistencySettings {
            max_neighbors_considered: 2,
            max_triangle_pairs_per_anchor: 1,
            max_outer_pair_row_product: 1,
            ..TriangleConsistencySettings::default()
        };

        let (filtered, info) = filter_tables_by_triangle_consistency(&tables, &settings).unwrap();

        assert_eq!(info.anchors_with_triangle_pairs, 0);
        assert_eq!(info.changed_tables, 0);
        assert_eq!(filtered, tables);
    }
}
