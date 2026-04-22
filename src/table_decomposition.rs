use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{project_row, sort_dedup_rows, Table};
use crate::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProjectionDecomposition {
    pub left_factor: Table,
    pub right_factor: Table,
    pub shared_bits: Vec<u32>,
    pub left_only_bits: Vec<u32>,
    pub right_only_bits: Vec<u32>,
    pub max_factor_arity: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct LatentBicliqueDecomposition {
    pub left_factor: Table,
    pub right_factor: Table,
    pub left_original_bits: Vec<u32>,
    pub right_original_bits: Vec<u32>,
    pub latent_bits: Vec<u32>,
    pub biclique_count: usize,
    pub max_factor_arity: usize,
    pub arity_reducing: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct TableDecompositionSearch {
    pub exact_projection: Option<ProjectionDecomposition>,
    pub exact_latent_biclique: Option<LatentBicliqueDecomposition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionScore {
    max_factor_arity: usize,
    total_factor_rows: usize,
    total_factor_arity: usize,
    shared_bits: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LatentScore {
    non_reducing: bool,
    max_factor_arity: usize,
    latent_bit_count: usize,
    biclique_count: usize,
    total_factor_rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Biclique {
    left_mask: u16,
    right_mask: u16,
}

#[derive(Clone, Debug)]
struct CandidateBiclique {
    biclique: Biclique,
    covered_edges_mask: u128,
}

#[derive(Clone, Debug)]
struct EdgeGraph {
    left_values: Vec<u32>,
    right_values: Vec<u32>,
    left_adjacency: Vec<u16>,
    edge_indices: BTreeMap<(usize, usize), usize>,
    edge_count: usize,
}

pub fn find_exact_projection_decomposition(
    table: &Table,
) -> Result<Option<ProjectionDecomposition>> {
    validate_table_for_search(table)?;
    if table.bits.len() < 3 {
        return Ok(None);
    }

    let table = canonicalize_table(table);
    let bit_count = table.bits.len();
    let full_mask = (1u16 << bit_count) - 1;
    let mut best: Option<(ProjectionScore, ProjectionDecomposition)> = None;

    for left_mask in 1..full_mask {
        if left_mask == full_mask {
            continue;
        }
        for right_mask in (left_mask + 1)..full_mask {
            if (left_mask | right_mask) != full_mask {
                continue;
            }

            let left_count = left_mask.count_ones() as usize;
            let right_count = right_mask.count_ones() as usize;
            if left_count == 0
                || right_count == 0
                || left_count == bit_count
                || right_count == bit_count
            {
                continue;
            }

            let left_factor = project_table_by_mask(&table, left_mask);
            let right_factor = project_table_by_mask(&table, right_mask);
            let merged = merge_tables_fast_from_slices(
                &left_factor.bits,
                &left_factor.rows,
                &right_factor.bits,
                &right_factor.rows,
            )
            .map_err(anyhow::Error::msg)?;

            if merged.bits != table.bits || merged.rows != table.rows {
                continue;
            }

            let shared_bits = bits_for_mask(&table.bits, left_mask & right_mask);
            let left_only_bits = bits_for_mask(&table.bits, left_mask & !right_mask);
            let right_only_bits = bits_for_mask(&table.bits, right_mask & !left_mask);
            let decomposition = ProjectionDecomposition {
                left_factor,
                right_factor,
                shared_bits: shared_bits.clone(),
                left_only_bits,
                right_only_bits,
                max_factor_arity: left_count.max(right_count),
            };
            let score = ProjectionScore {
                max_factor_arity: decomposition.max_factor_arity,
                total_factor_rows: decomposition.left_factor.rows.len()
                    + decomposition.right_factor.rows.len(),
                total_factor_arity: decomposition.left_factor.bits.len()
                    + decomposition.right_factor.bits.len(),
                shared_bits: usize::MAX - shared_bits.len(),
            };

            if best
                .as_ref()
                .is_none_or(|(best_score, _)| score < *best_score)
            {
                best = Some((score, decomposition));
            }
        }
    }

    Ok(best.map(|(_, decomposition)| decomposition))
}

pub fn find_exact_latent_biclique_decomposition(
    table: &Table,
    max_small_side_bits: usize,
) -> Result<Option<LatentBicliqueDecomposition>> {
    validate_table_for_search(table)?;
    if table.bits.len() < 4 {
        return Ok(None);
    }
    if max_small_side_bits == 0 || max_small_side_bits > 4 {
        bail!(
            "max_small_side_bits must be in 1..=4 for exact latent biclique search, got {}",
            max_small_side_bits
        );
    }

    let table = canonicalize_table(table);
    let bit_count = table.bits.len();
    let full_mask = (1u16 << bit_count) - 1;
    let mut best: Option<(LatentScore, LatentBicliqueDecomposition)> = None;

    for left_mask in 1..full_mask {
        let left_bit_count = left_mask.count_ones() as usize;
        let right_bit_count = bit_count - left_bit_count;
        if left_bit_count == 0
            || right_bit_count == 0
            || left_bit_count > right_bit_count
            || left_bit_count > max_small_side_bits
        {
            continue;
        }

        let Ok(graph) = build_edge_graph(&table, left_mask) else {
            continue;
        };
        let Some(bicliques) = exact_biclique_cover(&graph) else {
            continue;
        };

        let biclique_count = bicliques.len();
        let latent_bit_count = bits_required_for_states(biclique_count);
        let left_original_bits = bits_for_mask(&table.bits, left_mask);
        let right_original_bits = bits_for_mask(&table.bits, full_mask ^ left_mask);
        let latent_bits = build_latent_bits(&table.bits, latent_bit_count);
        let left_factor = build_latent_factor(
            &left_original_bits,
            &graph.left_values,
            &latent_bits,
            &bicliques,
            true,
        );
        let right_factor = build_latent_factor(
            &right_original_bits,
            &graph.right_values,
            &latent_bits,
            &bicliques,
            false,
        );
        let max_factor_arity = left_factor.bits.len().max(right_factor.bits.len());
        let arity_reducing =
            left_factor.bits.len() < bit_count && right_factor.bits.len() < bit_count;

        let decomposition = LatentBicliqueDecomposition {
            left_factor,
            right_factor,
            left_original_bits,
            right_original_bits,
            latent_bits,
            biclique_count,
            max_factor_arity,
            arity_reducing,
        };
        let score = LatentScore {
            non_reducing: !decomposition.arity_reducing,
            max_factor_arity,
            latent_bit_count,
            biclique_count,
            total_factor_rows: decomposition.left_factor.rows.len()
                + decomposition.right_factor.rows.len(),
        };

        if best
            .as_ref()
            .is_none_or(|(best_score, _)| score < *best_score)
        {
            best = Some((score, decomposition));
        }
    }

    Ok(best.map(|(_, decomposition)| decomposition))
}

pub fn search_table_decompositions(
    table: &Table,
    max_small_side_bits: usize,
) -> Result<TableDecompositionSearch> {
    Ok(TableDecompositionSearch {
        exact_projection: find_exact_projection_decomposition(table)?,
        exact_latent_biclique: find_exact_latent_biclique_decomposition(
            table,
            max_small_side_bits,
        )?,
    })
}

pub fn canonicalize_table_for_decomposition(table: &Table) -> Result<Table> {
    validate_table_for_search(table)?;
    Ok(canonicalize_table(table))
}

pub fn project_away_bits(table: &Table, removed_bits: &[u32]) -> Result<Table> {
    let removed: BTreeSet<u32> = removed_bits.iter().copied().collect();
    let kept_indices: Vec<usize> = table
        .bits
        .iter()
        .enumerate()
        .filter_map(|(index, bit)| (!removed.contains(bit)).then_some(index))
        .collect();
    let kept_bits: Vec<u32> = kept_indices
        .iter()
        .map(|&index| table.bits[index])
        .collect();
    let mut rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &kept_indices))
        .collect();
    sort_dedup_rows(&mut rows);
    Ok(Table {
        bits: kept_bits,
        rows,
    })
}

fn validate_table_for_search(table: &Table) -> Result<()> {
    if table.bits.is_empty() {
        bail!("table decomposition search requires at least one bit");
    }
    if table.bits.len() > 15 {
        bail!(
            "table decomposition search currently supports arity up to 15, got {}",
            table.bits.len()
        );
    }
    let unique_count = table.bits.iter().copied().collect::<BTreeSet<_>>().len();
    if unique_count != table.bits.len() {
        bail!("table bits must be unique for decomposition search");
    }
    Ok(())
}

fn canonicalize_table(table: &Table) -> Table {
    let mut indexed_bits = table.bits.iter().copied().enumerate().collect::<Vec<_>>();
    indexed_bits.sort_by_key(|(_, bit)| *bit);
    let reordered_indices = indexed_bits
        .iter()
        .map(|(old_index, _)| *old_index)
        .collect::<Vec<_>>();
    let bits = indexed_bits.iter().map(|(_, bit)| *bit).collect::<Vec<_>>();
    let mut rows = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &reordered_indices))
        .collect::<Vec<_>>();
    sort_dedup_rows(&mut rows);
    Table { bits, rows }
}

fn project_table_by_mask(table: &Table, mask: u16) -> Table {
    let kept_indices: Vec<usize> = (0..table.bits.len())
        .filter(|index| ((mask >> index) & 1) != 0)
        .collect();
    let kept_bits = kept_indices
        .iter()
        .map(|&index| table.bits[index])
        .collect::<Vec<_>>();
    let mut rows = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &kept_indices))
        .collect::<Vec<_>>();
    sort_dedup_rows(&mut rows);
    Table {
        bits: kept_bits,
        rows,
    }
}

fn bits_for_mask(bits: &[u32], mask: u16) -> Vec<u32> {
    bits.iter()
        .enumerate()
        .filter_map(|(index, &bit)| (((mask >> index) & 1) != 0).then_some(bit))
        .collect()
}

fn build_edge_graph(table: &Table, left_mask: u16) -> Result<EdgeGraph> {
    let left_indices: Vec<usize> = (0..table.bits.len())
        .filter(|index| ((left_mask >> index) & 1) != 0)
        .collect();
    let right_indices: Vec<usize> = (0..table.bits.len())
        .filter(|index| ((left_mask >> index) & 1) == 0)
        .collect();

    let left_values: Vec<u32> = unique_projected_values(&table.rows, &left_indices);
    let right_values: Vec<u32> = unique_projected_values(&table.rows, &right_indices);

    if left_values.len() > 16 || right_values.len() > 16 {
        bail!(
            "exact latent biclique search requires projected assignment counts <= 16, got left={} right={}",
            left_values.len(),
            right_values.len()
        );
    }

    let left_lookup = left_values
        .iter()
        .enumerate()
        .map(|(index, &value)| (value, index))
        .collect::<BTreeMap<_, _>>();
    let right_lookup = right_values
        .iter()
        .enumerate()
        .map(|(index, &value)| (value, index))
        .collect::<BTreeMap<_, _>>();

    let mut left_adjacency = vec![0u16; left_values.len()];
    let mut edge_indices = BTreeMap::new();

    for &row in &table.rows {
        let left_value = project_row(row, &left_indices);
        let right_value = project_row(row, &right_indices);
        let left_index = *left_lookup.get(&left_value).unwrap();
        let right_index = *right_lookup.get(&right_value).unwrap();
        left_adjacency[left_index] |= 1u16 << right_index;
    }

    let mut edge_count = 0usize;
    for (left_index, &neighbors) in left_adjacency.iter().enumerate() {
        for right_index in 0..right_values.len() {
            if ((neighbors >> right_index) & 1) == 0 {
                continue;
            }
            edge_indices.insert((left_index, right_index), edge_count);
            edge_count += 1;
        }
    }

    Ok(EdgeGraph {
        left_values,
        right_values,
        left_adjacency,
        edge_indices,
        edge_count,
    })
}

fn unique_projected_values(rows: &[u32], indices: &[usize]) -> Vec<u32> {
    let mut values = rows
        .iter()
        .copied()
        .map(|row| project_row(row, indices))
        .collect::<Vec<_>>();
    sort_dedup_rows(&mut values);
    values
}

fn exact_biclique_cover(graph: &EdgeGraph) -> Option<Vec<Biclique>> {
    if graph.edge_count == 0 {
        return Some(Vec::new());
    }
    if graph.edge_count > 128 {
        return None;
    }

    let mut unique = BTreeMap::<Biclique, u128>::new();
    let left_count = graph.left_values.len();
    for left_subset in 1u32..(1u32 << left_count) {
        let mut common_neighbors = (1u16 << graph.right_values.len()) - 1;
        for left_index in 0..left_count {
            if ((left_subset >> left_index) & 1) != 0 {
                common_neighbors &= graph.left_adjacency[left_index];
                if common_neighbors == 0 {
                    break;
                }
            }
        }
        if common_neighbors == 0 {
            continue;
        }

        let mut left_closure = 0u16;
        for left_index in 0..left_count {
            if graph.left_adjacency[left_index] & common_neighbors == common_neighbors {
                left_closure |= 1u16 << left_index;
            }
        }

        let biclique = Biclique {
            left_mask: left_closure,
            right_mask: common_neighbors,
        };
        unique
            .entry(biclique)
            .or_insert_with(|| biclique_edge_mask(graph, biclique));
    }

    let candidates = unique
        .into_iter()
        .map(|(biclique, covered_edges_mask)| CandidateBiclique {
            biclique,
            covered_edges_mask,
        })
        .collect::<Vec<_>>();
    let candidates = candidates
        .iter()
        .filter(|candidate| {
            candidates.iter().all(|other| {
                candidate.covered_edges_mask == other.covered_edges_mask
                    || (candidate.covered_edges_mask & !other.covered_edges_mask) != 0
            })
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut edge_to_candidates = vec![Vec::<usize>::new(); graph.edge_count];
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        for edge_index in 0..graph.edge_count {
            if ((candidate.covered_edges_mask >> edge_index) & 1) != 0 {
                edge_to_candidates[edge_index].push(candidate_index);
            }
        }
    }

    let all_edges = if graph.edge_count == 128 {
        u128::MAX
    } else {
        (1u128 << graph.edge_count) - 1
    };
    let mut best: Option<Vec<usize>> = None;
    let mut current = Vec::new();
    search_biclique_cover(
        all_edges,
        &candidates,
        &edge_to_candidates,
        &mut current,
        &mut best,
    );

    best.map(|indices| {
        indices
            .into_iter()
            .map(|index| candidates[index].biclique)
            .collect()
    })
}

fn biclique_edge_mask(graph: &EdgeGraph, biclique: Biclique) -> u128 {
    let mut mask = 0u128;
    for left_index in 0..graph.left_values.len() {
        if ((biclique.left_mask >> left_index) & 1) == 0 {
            continue;
        }
        for right_index in 0..graph.right_values.len() {
            if ((biclique.right_mask >> right_index) & 1) == 0 {
                continue;
            }
            let edge_index = graph.edge_indices[&(left_index, right_index)];
            mask |= 1u128 << edge_index;
        }
    }
    mask
}

fn search_biclique_cover(
    uncovered_edges: u128,
    candidates: &[CandidateBiclique],
    edge_to_candidates: &[Vec<usize>],
    current: &mut Vec<usize>,
    best: &mut Option<Vec<usize>>,
) {
    if uncovered_edges == 0 {
        if best
            .as_ref()
            .is_none_or(|best_cover| current.len() < best_cover.len())
        {
            *best = Some(current.clone());
        }
        return;
    }
    if best
        .as_ref()
        .is_some_and(|best_cover| current.len() >= best_cover.len())
    {
        return;
    }

    let edge_index = choose_branch_edge(uncovered_edges, candidates, edge_to_candidates);
    let mut candidate_indices = edge_to_candidates[edge_index]
        .iter()
        .copied()
        .filter(|&candidate_index| {
            candidates[candidate_index].covered_edges_mask & uncovered_edges != 0
        })
        .collect::<Vec<_>>();
    candidate_indices.sort_by_key(|&candidate_index| {
        usize::MAX
            - (candidates[candidate_index].covered_edges_mask & uncovered_edges).count_ones()
                as usize
    });

    for candidate_index in candidate_indices {
        let covered_edges = candidates[candidate_index].covered_edges_mask & uncovered_edges;
        if covered_edges == 0 {
            continue;
        }
        current.push(candidate_index);
        search_biclique_cover(
            uncovered_edges & !candidates[candidate_index].covered_edges_mask,
            candidates,
            edge_to_candidates,
            current,
            best,
        );
        current.pop();
    }
}

fn choose_branch_edge(
    uncovered_edges: u128,
    candidates: &[CandidateBiclique],
    edge_to_candidates: &[Vec<usize>],
) -> usize {
    let mut best_edge = uncovered_edges.trailing_zeros() as usize;
    let mut best_branching = usize::MAX;
    let mut remaining = uncovered_edges;

    while remaining != 0 {
        let edge_index = remaining.trailing_zeros() as usize;
        let branching = edge_to_candidates[edge_index]
            .iter()
            .filter(|&&candidate_index| {
                candidates[candidate_index].covered_edges_mask & uncovered_edges != 0
            })
            .count();
        if branching < best_branching {
            best_branching = branching;
            best_edge = edge_index;
            if branching <= 1 {
                break;
            }
        }
        remaining &= remaining - 1;
    }

    best_edge
}

fn bits_required_for_states(state_count: usize) -> usize {
    if state_count <= 1 {
        0
    } else {
        (usize::BITS - (state_count - 1).leading_zeros()) as usize
    }
}

fn build_latent_bits(original_bits: &[u32], latent_bit_count: usize) -> Vec<u32> {
    let start = original_bits.last().copied().unwrap_or(0).saturating_add(1);
    (0..latent_bit_count)
        .map(|offset| start + offset as u32)
        .collect()
}

fn build_latent_factor(
    original_bits: &[u32],
    projected_values: &[u32],
    latent_bits: &[u32],
    bicliques: &[Biclique],
    use_left_side: bool,
) -> Table {
    let mut rows = Vec::new();
    let projected_width = original_bits.len();

    for (state, biclique) in bicliques.iter().enumerate() {
        let mask = if use_left_side {
            biclique.left_mask
        } else {
            biclique.right_mask
        };
        for (value_index, &projected_value) in projected_values.iter().enumerate() {
            if ((mask >> value_index) & 1) == 0 {
                continue;
            }
            rows.push(projected_value | ((state as u32) << projected_width));
        }
    }

    sort_dedup_rows(&mut rows);
    let mut bits = original_bits.to_vec();
    bits.extend_from_slice(latent_bits);
    Table { bits, rows }
}

#[cfg(test)]
mod tests {
    use super::{
        find_exact_latent_biclique_decomposition, find_exact_projection_decomposition,
        project_away_bits,
    };
    use crate::common::Table;
    use crate::table_merge_fast::merge_tables_fast_from_slices;

    fn table(bits: &[u32], rows: &[u32]) -> Table {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
        rows.dedup();
        Table {
            bits: bits.to_vec(),
            rows,
        }
    }

    #[test]
    fn finds_lossless_projection_decomposition() {
        let input = table(&[1, 2, 3], &[0b000, 0b001, 0b110, 0b111]);

        let decomposition = find_exact_projection_decomposition(&input)
            .unwrap()
            .expect("expected projection decomposition");

        assert_eq!(decomposition.max_factor_arity, 2);
        let merged = merge_tables_fast_from_slices(
            &decomposition.left_factor.bits,
            &decomposition.left_factor.rows,
            &decomposition.right_factor.bits,
            &decomposition.right_factor.rows,
        )
        .unwrap();
        assert_eq!(merged.bits, input.bits);
        assert_eq!(merged.rows, input.rows);
    }

    #[test]
    fn rejects_projection_decomposition_when_join_would_overgenerate() {
        let input = table(&[1, 2, 3, 4], &[0, 3, 5, 6, 9, 10, 12, 15]);

        let decomposition = find_exact_projection_decomposition(&input).unwrap();

        assert!(decomposition.is_none());
    }

    #[test]
    fn finds_exact_latent_biclique_decomposition() {
        let input = table(&[1, 2, 3, 4], &[0, 3, 5, 6, 9, 10, 12, 15]);

        let decomposition = find_exact_latent_biclique_decomposition(&input, 4)
            .unwrap()
            .expect("expected latent biclique decomposition");

        assert!(decomposition.arity_reducing);
        assert_eq!(decomposition.latent_bits.len(), 1);
        assert_eq!(decomposition.max_factor_arity, 3);

        let merged = merge_tables_fast_from_slices(
            &decomposition.left_factor.bits,
            &decomposition.left_factor.rows,
            &decomposition.right_factor.bits,
            &decomposition.right_factor.rows,
        )
        .unwrap();
        let projected = project_away_bits(
            &Table {
                bits: merged.bits,
                rows: merged.rows,
            },
            &decomposition.latent_bits,
        )
        .unwrap();

        assert_eq!(projected.bits, input.bits);
        assert_eq!(projected.rows, input.rows);
    }
}
