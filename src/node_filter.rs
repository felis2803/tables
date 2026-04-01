use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{project_row, NodeArtifact, Table};

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

#[derive(Clone, Debug, Default)]
struct ProjectionScratch {
    seen_epoch: Vec<u32>,
    count_epoch: Vec<u32>,
    counts: Vec<u32>,
    touched: Vec<u32>,
    epoch: u32,
}

impl ProjectionScratch {
    fn ensure_capacity(&mut self, full_row_count: usize) {
        if self.seen_epoch.len() < full_row_count {
            self.seen_epoch.resize(full_row_count, 0);
            self.count_epoch.resize(full_row_count, 0);
            self.counts.resize(full_row_count, 0);
        }
        self.touched.clear();
    }

    fn next_epoch(&mut self) -> u32 {
        if self.epoch == u32::MAX {
            self.seen_epoch.fill(0);
            self.count_epoch.fill(0);
            self.epoch = 1;
        } else {
            self.epoch += 1;
        }
        self.epoch
    }
}

fn collect_candidate_subsets(tables: &[Table]) -> Vec<Vec<u32>> {
    let bit_to_tables = build_bit_to_tables(tables);
    let mut candidate_subsets = HashSet::new();
    let mut overlap_counts: HashMap<usize, u8> = HashMap::new();

    for (left_index, left_table) in tables.iter().enumerate() {
        overlap_counts.clear();

        for &bit in &left_table.bits {
            let Some(table_ids) = bit_to_tables.get(&bit) else {
                continue;
            };
            for &right_index in table_ids {
                if right_index <= left_index {
                    continue;
                }
                let entry = overlap_counts.entry(right_index).or_insert(0);
                if *entry < u8::MAX {
                    *entry += 1;
                }
            }
        }

        for (right_index, shared_bit_count) in overlap_counts.drain() {
            if shared_bit_count < 2 {
                continue;
            }

            let shared_bits =
                crate::common::intersect_sorted(&left_table.bits, &tables[right_index].bits);
            debug_assert!(shared_bits.len() >= 2);
            candidate_subsets.insert(shared_bits);
        }
    }

    let mut candidate_subsets: Vec<Vec<u32>> = candidate_subsets.into_iter().collect();
    candidate_subsets.sort_by(|left_bits, right_bits| {
        left_bits
            .len()
            .cmp(&right_bits.len())
            .then_with(|| left_bits.cmp(right_bits))
    });
    candidate_subsets
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

fn intersect_sorted_usize(left: &[usize], right: &[usize]) -> Vec<usize> {
    let mut output = Vec::with_capacity(left.len().min(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                output.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    output
}

fn support_tables_for_subset(
    subset_bits: &[u32],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
) -> Vec<usize> {
    let Some((first_bit, remaining_bits)) = subset_bits.split_first() else {
        return Vec::new();
    };

    let Some(mut support) = bit_to_tables.get(first_bit).cloned() else {
        return Vec::new();
    };

    let mut posting_lists: Vec<&[usize]> = remaining_bits
        .iter()
        .filter_map(|bit| bit_to_tables.get(bit).map(Vec::as_slice))
        .collect();
    if posting_lists.len() != remaining_bits.len() {
        return Vec::new();
    }
    posting_lists.sort_by_key(|tables| tables.len());

    for posting_list in posting_lists {
        support = intersect_sorted_usize(&support, posting_list);
        if support.len() < 2 {
            break;
        }
    }

    support
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

fn compute_allowed_rows_legacy(node: &Node, tables: &[Table]) -> Result<Vec<u32>> {
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

fn compute_allowed_rows_with_scratch(
    node: &Node,
    tables: &[Table],
    scratch: &mut ProjectionScratch,
) -> Result<Vec<u32>> {
    let Some(full_row_count) = 1usize.checked_shl(node.bits.len() as u32) else {
        return compute_allowed_rows_legacy(node, tables);
    };
    if node.bits.len() > 16 {
        return compute_allowed_rows_legacy(node, tables);
    }
    if node.members.is_empty() {
        bail!("node without members for bits {:?}", node.bits);
    }

    scratch.ensure_capacity(full_row_count);
    let call_epoch = scratch.next_epoch();

    for (member_index, (&table_index, subset_indices)) in node
        .members
        .iter()
        .zip(node.member_indices.iter())
        .enumerate()
    {
        let member_epoch = scratch.next_epoch();
        let mut matched_here = 0usize;

        for &row in &tables[table_index].rows {
            let projected = project_row(row, subset_indices) as usize;
            if scratch.seen_epoch[projected] == member_epoch {
                continue;
            }
            scratch.seen_epoch[projected] = member_epoch;

            if member_index == 0 {
                if scratch.count_epoch[projected] != call_epoch {
                    scratch.count_epoch[projected] = call_epoch;
                    scratch.counts[projected] = 1;
                    scratch.touched.push(projected as u32);
                    matched_here += 1;
                }
                continue;
            }

            if scratch.count_epoch[projected] == call_epoch
                && scratch.counts[projected] == member_index as u32
            {
                scratch.counts[projected] += 1;
                matched_here += 1;
            }
        }

        if matched_here == 0 {
            bail!("empty node intersection for bits {:?}", node.bits);
        }
    }

    let target_count = node.members.len() as u32;
    let mut allowed_rows = Vec::with_capacity(scratch.touched.len());
    for &projected in &scratch.touched {
        let projected = projected as usize;
        if scratch.count_epoch[projected] == call_epoch && scratch.counts[projected] == target_count
        {
            allowed_rows.push(projected as u32);
        }
    }
    allowed_rows.sort_unstable();
    Ok(allowed_rows)
}

pub fn build_nodes(tables: &[Table]) -> Result<(Vec<Node>, Vec<Vec<usize>>, NodeBuildStats)> {
    let candidate_subsets = collect_candidate_subsets(tables);
    let bit_to_tables = build_bit_to_tables(tables);
    let mut table_to_nodes = vec![Vec::new(); tables.len()];
    let mut nodes = Vec::new();
    let mut support_histogram: BTreeMap<String, usize> = BTreeMap::new();
    let mut restrictive_nodes = 0usize;
    let mut projection_scratch = ProjectionScratch::default();

    for subset_bits in candidate_subsets {
        let support_tables = support_tables_for_subset(&subset_bits, &bit_to_tables);
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
        node.rows = compute_allowed_rows_with_scratch(&node, tables, &mut projection_scratch)?;
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
    let mut projection_scratch = ProjectionScratch::default();

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
            let new_rows = compute_allowed_rows_with_scratch(
                &nodes[affected_node_index],
                tables,
                &mut projection_scratch,
            )?;
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
    use std::collections::{BTreeSet, HashMap};

    use super::*;

    fn legacy_build_subset_support(tables: &[Table]) -> HashMap<Vec<u32>, Vec<usize>> {
        let mut subset_to_tables: HashMap<Vec<u32>, Vec<usize>> = HashMap::new();

        for (table_index, table) in tables.iter().enumerate() {
            for subset_size in 2..=table.bits.len() {
                crate::common::for_each_combination(
                    table.bits.len(),
                    subset_size,
                    |subset_indices| {
                        let subset_bits: Vec<u32> = subset_indices
                            .iter()
                            .map(|&index| table.bits[index])
                            .collect();
                        subset_to_tables
                            .entry(subset_bits)
                            .or_default()
                            .push(table_index);
                    },
                );
            }
        }

        subset_to_tables
    }

    fn legacy_build_nodes(
        tables: &[Table],
    ) -> Result<(Vec<Node>, Vec<Vec<usize>>, NodeBuildStats)> {
        let subset_to_tables = legacy_build_subset_support(tables);
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
            node.rows = compute_allowed_rows_legacy(&node, tables)?;
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
                ..Default::default()
            },
        ))
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct NodeSnapshot {
        bits: Vec<u32>,
        members: Vec<usize>,
        rows: Vec<u32>,
        is_restrictive: bool,
    }

    fn snapshot_nodes(nodes: &[Node]) -> Vec<NodeSnapshot> {
        let mut snapshots: Vec<_> = nodes
            .iter()
            .map(|node| NodeSnapshot {
                bits: node.bits.clone(),
                members: node.members.clone(),
                rows: node.rows.clone(),
                is_restrictive: node.is_restrictive,
            })
            .collect();
        snapshots.sort();
        snapshots
    }

    struct XorShift64 {
        state: u64,
    }

    impl XorShift64 {
        fn new(seed: u64) -> Self {
            Self { state: seed.max(1) }
        }

        fn next_u32(&mut self) -> u32 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            (x >> 16) as u32
        }

        fn gen_range(&mut self, bound: u32) -> u32 {
            if bound == 0 {
                0
            } else {
                self.next_u32() % bound
            }
        }
    }

    fn random_bits(
        rng: &mut XorShift64,
        universe: usize,
        min_len: usize,
        max_len: usize,
    ) -> Vec<u32> {
        let target_len = min_len + rng.gen_range((max_len - min_len + 1) as u32) as usize;
        let mut set = BTreeSet::new();
        while set.len() < target_len {
            set.insert(rng.gen_range(universe as u32));
        }
        set.into_iter().collect()
    }

    fn random_rows(rng: &mut XorShift64, arity: usize) -> Vec<u32> {
        let full = 1u32 << arity;
        let target_len = 1 + rng.gen_range(full.min(8));
        let mut set = BTreeSet::new();
        while set.len() < target_len as usize {
            set.insert(rng.gen_range(full));
        }
        set.into_iter().collect()
    }

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

    #[test]
    fn build_nodes_matches_legacy_subset_materialization() {
        let mut rng = XorShift64::new(0xBAD5EED);

        for _case in 0..50 {
            let table_count = 2 + rng.gen_range(4) as usize;
            let mut tables = Vec::with_capacity(table_count);
            for _ in 0..table_count {
                let bits = random_bits(&mut rng, 7, 2, 5);
                let rows = random_rows(&mut rng, bits.len());
                tables.push(Table { bits, rows });
            }

            let new_result = build_nodes(&tables);
            let legacy_result = legacy_build_nodes(&tables);
            match (new_result, legacy_result) {
                (Ok((new_nodes, _, new_stats)), Ok((legacy_nodes, _, legacy_stats))) => {
                    assert_eq!(snapshot_nodes(&new_nodes), snapshot_nodes(&legacy_nodes));
                    assert_eq!(new_stats.node_count, legacy_stats.node_count);
                    assert_eq!(
                        new_stats.restrictive_node_count,
                        legacy_stats.restrictive_node_count
                    );
                    assert_eq!(new_stats.support_histogram, legacy_stats.support_histogram);
                }
                (Err(new_error), Err(legacy_error)) => {
                    assert_eq!(new_error.to_string(), legacy_error.to_string());
                }
                (new_result, legacy_result) => {
                    panic!(
                        "new and legacy build_nodes disagree: new={:?}, legacy={:?}",
                        new_result, legacy_result
                    );
                }
            }
        }
    }

    #[test]
    fn compute_allowed_rows_fast_path_matches_legacy() {
        let tables = vec![
            Table {
                bits: vec![1, 2, 3, 4],
                rows: vec![0b0000, 0b0011, 0b1010, 0b1111],
            },
            Table {
                bits: vec![1, 2, 5, 6],
                rows: vec![0b0000, 0b0011, 0b0101, 0b1111],
            },
            Table {
                bits: vec![1, 2, 7, 8],
                rows: vec![0b0000, 0b0011, 0b1001, 0b1111],
            },
        ];

        let node = Node {
            bits: vec![1, 2],
            members: vec![0, 1, 2],
            member_indices: vec![vec![0, 1], vec![0, 1], vec![0, 1]],
            rows: Vec::new(),
            full_row_count: 4,
            is_restrictive: false,
        };

        let mut scratch = ProjectionScratch::default();
        let fast = compute_allowed_rows_with_scratch(&node, &tables, &mut scratch).unwrap();
        let legacy = compute_allowed_rows_legacy(&node, &tables).unwrap();

        assert_eq!(fast, legacy);
    }
}
