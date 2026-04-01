use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{for_each_combination, project_row, NodeArtifact, Table};

#[derive(Clone, Debug)]
pub struct Node {
    pub bits: Vec<u32>,
    pub members: Vec<usize>,
    pub member_indices: Vec<Vec<usize>>,
    pub rows: Vec<u32>,
    pub full_row_count: usize,
    pub is_restrictive: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NodeBuildStats {
    pub node_count: usize,
    pub restrictive_node_count: usize,
    pub support_histogram: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NodeFilterStats {
    pub changed_tables: usize,
    pub row_deletions: usize,
    pub node_recomputations: usize,
    pub node_tightenings: usize,
    pub final_restrictive_node_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeFilterInfo {
    pub node_build: NodeBuildStats,
    pub filter: NodeFilterStats,
}

fn build_subset_support(tables: &[Table]) -> HashMap<Vec<u32>, Vec<usize>> {
    let mut subset_to_tables: HashMap<Vec<u32>, Vec<usize>> = HashMap::new();

    for (table_index, table) in tables.iter().enumerate() {
        for subset_size in 2..=table.bits.len() {
            for_each_combination(table.bits.len(), subset_size, |subset_indices| {
                let subset_bits: Vec<u32> = subset_indices
                    .iter()
                    .map(|&index| table.bits[index])
                    .collect();
                subset_to_tables
                    .entry(subset_bits)
                    .or_default()
                    .push(table_index);
            });
        }
    }

    subset_to_tables
}

fn sorted_difference(full: &[u32], subset: &[u32]) -> Vec<u32> {
    let mut difference = Vec::with_capacity(full.len().saturating_sub(subset.len()));
    let mut subset_index = 0usize;

    for &bit in full {
        if subset_index < subset.len() && subset[subset_index] == bit {
            subset_index += 1;
        } else {
            difference.push(bit);
        }
    }

    difference
}

fn sorted_slices_are_disjoint(left: &[u32], right: &[u32]) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return false,
        }
    }

    true
}

fn exact_intersection_members(
    subset_bits: &[u32],
    support_tables: &[usize],
    tables: &[Table],
) -> Vec<usize> {
    let mut extras = Vec::with_capacity(support_tables.len());
    let mut has_exact_table = false;

    for &table_index in support_tables {
        let extra_bits = sorted_difference(&tables[table_index].bits, subset_bits);
        if extra_bits.is_empty() {
            has_exact_table = true;
        }
        extras.push((table_index, extra_bits));
    }

    if has_exact_table {
        let mut members = support_tables.to_vec();
        members.sort_unstable();
        members.dedup();
        return members;
    }

    let mut members = BTreeSet::new();
    for left_index in 0..extras.len() {
        let (left_table, left_extra) = &extras[left_index];
        for (right_table, right_extra) in extras.iter().skip(left_index + 1) {
            if sorted_slices_are_disjoint(left_extra, right_extra) {
                members.insert(*left_table);
                members.insert(*right_table);
            }
        }
    }

    members.into_iter().collect()
}

fn projected_rows(table: &Table, subset_indices: &[usize]) -> Vec<u32> {
    let mut rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, subset_indices))
        .collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}

fn compute_allowed_rows(node: &Node, tables: &[Table]) -> Result<Vec<u32>> {
    let mut member_iter = node.members.iter().zip(node.member_indices.iter());
    let Some((&first_table_index, first_subset_indices)) = member_iter.next() else {
        bail!("node without members for bits {:?}", node.bits);
    };

    let mut allowed_rows = projected_rows(&tables[first_table_index], first_subset_indices);
    for (&table_index, subset_indices) in member_iter {
        let projected = projected_rows(&tables[table_index], subset_indices);
        allowed_rows = crate::common::intersect_sorted(&allowed_rows, &projected);
        if allowed_rows.is_empty() {
            bail!("empty node intersection for bits {:?}", node.bits);
        }
    }

    Ok(allowed_rows)
}

pub fn build_nodes(tables: &[Table]) -> Result<(Vec<Node>, Vec<Vec<usize>>, NodeBuildStats)> {
    let subset_to_tables = build_subset_support(tables);
    let mut table_to_nodes = vec![Vec::new(); tables.len()];
    let mut nodes = Vec::new();
    let mut support_histogram: BTreeMap<String, usize> = BTreeMap::new();
    let mut restrictive_nodes = 0usize;

    let mut subset_entries: Vec<_> = subset_to_tables.into_iter().collect();
    subset_entries.sort_by(|(left_bits, _), (right_bits, _)| {
        left_bits
            .len()
            .cmp(&right_bits.len())
            .then_with(|| left_bits.cmp(right_bits))
    });

    for (subset_bits, support_tables) in subset_entries {
        if support_tables.len() < 2 {
            continue;
        }

        let members = exact_intersection_members(&subset_bits, &support_tables, tables);
        if members.len() < 2 {
            continue;
        }

        let member_indices: Vec<Vec<usize>> = members
            .iter()
            .map(|&table_index| {
                subset_bits
                    .iter()
                    .map(|bit| tables[table_index].bits.binary_search(bit).unwrap())
                    .collect()
            })
            .collect();

        let mut node = Node {
            bits: subset_bits,
            members,
            member_indices,
            rows: Vec::new(),
            full_row_count: 0,
            is_restrictive: false,
        };
        node.rows = compute_allowed_rows(&node, tables)?;
        node.full_row_count = 1usize << node.bits.len();
        node.is_restrictive = node.rows.len() < node.full_row_count;
        if node.is_restrictive {
            restrictive_nodes += 1;
        }

        let node_index = nodes.len();
        for &table_index in &node.members {
            table_to_nodes[table_index].push(node_index);
        }
        *support_histogram
            .entry(node.members.len().to_string())
            .or_insert(0) += 1;
        nodes.push(node);
    }

    Ok((
        nodes,
        table_to_nodes,
        NodeBuildStats {
            node_count: support_histogram.values().sum(),
            restrictive_node_count: restrictive_nodes,
            support_histogram,
        },
    ))
}

pub fn filter_tables_with_nodes(
    tables: &mut [Table],
    nodes: &mut [Node],
    table_to_nodes: &[Vec<usize>],
) -> Result<NodeFilterStats> {
    let mut queue = VecDeque::new();
    let mut queued = vec![false; nodes.len()];
    for (node_index, node) in nodes.iter().enumerate() {
        if node.is_restrictive {
            queue.push_back(node_index);
            queued[node_index] = true;
        }
    }

    let mut touched_tables = HashSet::new();
    let mut stats = NodeFilterStats::default();

    while let Some(node_index) = queue.pop_front() {
        queued[node_index] = false;

        let mut changed_here = Vec::new();

        {
            let node = &nodes[node_index];
            for (&table_index, subset_indices) in
                node.members.iter().zip(node.member_indices.iter())
            {
                let original_len = tables[table_index].rows.len();
                let filtered_rows: Vec<u32> = tables[table_index]
                    .rows
                    .iter()
                    .copied()
                    .filter(|&row| {
                        node.rows
                            .binary_search(&project_row(row, subset_indices))
                            .is_ok()
                    })
                    .collect();
                if filtered_rows.is_empty() {
                    bail!(
                        "node filtering emptied table {} for node bits {:?}",
                        table_index,
                        node.bits
                    );
                }
                if filtered_rows.len() != original_len {
                    tables[table_index].rows = filtered_rows;
                    stats.row_deletions += original_len - tables[table_index].rows.len();
                    changed_here.push(table_index);
                    touched_tables.insert(table_index);
                }
            }
        }

        if changed_here.is_empty() {
            continue;
        }

        let mut affected_nodes = BTreeSet::new();
        for table_index in changed_here {
            for &affected_node_index in &table_to_nodes[table_index] {
                affected_nodes.insert(affected_node_index);
            }
        }

        for affected_node_index in affected_nodes {
            let new_rows = compute_allowed_rows(&nodes[affected_node_index], tables)?;
            stats.node_recomputations += 1;
            if new_rows != nodes[affected_node_index].rows {
                let full_row_count = nodes[affected_node_index].full_row_count;
                nodes[affected_node_index].rows = new_rows;
                nodes[affected_node_index].is_restrictive =
                    nodes[affected_node_index].rows.len() < full_row_count;
                stats.node_tightenings += 1;
                if !queued[affected_node_index] {
                    queue.push_back(affected_node_index);
                    queued[affected_node_index] = true;
                }
            }
        }
    }

    stats.changed_tables = touched_tables.len();
    stats.final_restrictive_node_count = nodes.iter().filter(|node| node.is_restrictive).count();
    Ok(stats)
}

pub fn serialize_nodes(nodes: &[Node]) -> Vec<NodeArtifact> {
    nodes
        .iter()
        .map(|node| NodeArtifact {
            bits: node.bits.clone(),
            rows: node.rows.clone(),
            members: node.members.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_filter_builds_shared_node() {
        let tables = vec![
            Table {
                bits: vec![1, 2, 4],
                rows: vec![0b000, 0b111],
            },
            Table {
                bits: vec![1, 2, 5],
                rows: vec![0b000, 0b111],
            },
        ];

        let (nodes, _, stats) = build_nodes(&tables).unwrap();
        assert!(!nodes.is_empty());
        assert!(stats.node_count >= 1);
    }
}
