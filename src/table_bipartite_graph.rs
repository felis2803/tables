use std::collections::BTreeMap;

use serde::Serialize;

use crate::common::{project_row, total_rows, Table};

#[derive(Clone, Debug, Serialize)]
pub struct TableGraphNode {
    pub table_index: usize,
    pub bits: Vec<u32>,
    pub rows: Vec<u32>,
    pub adjacent_table_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct CompatibleRowBlock {
    pub shared_assignment: u32,
    pub left_row_indices: Vec<usize>,
    pub right_row_indices: Vec<usize>,
    pub edge_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TableGraphEdge {
    pub left_table_index: usize,
    pub right_table_index: usize,
    pub shared_bits: Vec<u32>,
    pub left_shared_indices: Vec<usize>,
    pub right_shared_indices: Vec<usize>,
    pub compatible_row_blocks: Vec<CompatibleRowBlock>,
    pub compatible_row_pair_count: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TableBipartiteGraphStats {
    pub table_count: usize,
    pub isolated_table_count: usize,
    pub table_edge_count: usize,
    pub row_node_count: usize,
    pub compatible_row_block_count: usize,
    pub compatible_row_pair_count: u64,
    pub max_shared_bits_per_table_edge: usize,
    pub max_compatible_row_blocks_per_table_edge: usize,
    pub max_compatible_row_pairs_per_table_edge: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TableBipartiteGraph {
    pub stats: TableBipartiteGraphStats,
    pub tables: Vec<TableGraphNode>,
    pub edges: Vec<TableGraphEdge>,
}

pub fn build_table_bipartite_graph(tables: &[Table]) -> TableBipartiteGraph {
    let pair_to_shared_bits = build_table_pair_shared_bits(tables);
    let mut incident_counts = vec![0usize; tables.len()];
    let mut edges = Vec::with_capacity(pair_to_shared_bits.len());
    let mut stats = TableBipartiteGraphStats {
        table_count: tables.len(),
        row_node_count: total_rows(tables),
        ..TableBipartiteGraphStats::default()
    };

    for ((left_table_index, right_table_index), shared_bits) in pair_to_shared_bits {
        let left_shared_indices =
            shared_indices_for_bits(&tables[left_table_index].bits, &shared_bits);
        let right_shared_indices =
            shared_indices_for_bits(&tables[right_table_index].bits, &shared_bits);
        let (compatible_row_blocks, compatible_row_pair_count) = build_compatible_row_blocks(
            &tables[left_table_index].rows,
            &left_shared_indices,
            &tables[right_table_index].rows,
            &right_shared_indices,
        );

        incident_counts[left_table_index] += 1;
        incident_counts[right_table_index] += 1;
        stats.table_edge_count += 1;
        stats.compatible_row_block_count += compatible_row_blocks.len();
        stats.compatible_row_pair_count += compatible_row_pair_count;
        stats.max_shared_bits_per_table_edge =
            stats.max_shared_bits_per_table_edge.max(shared_bits.len());
        stats.max_compatible_row_blocks_per_table_edge = stats
            .max_compatible_row_blocks_per_table_edge
            .max(compatible_row_blocks.len());
        stats.max_compatible_row_pairs_per_table_edge = stats
            .max_compatible_row_pairs_per_table_edge
            .max(compatible_row_pair_count);

        edges.push(TableGraphEdge {
            left_table_index,
            right_table_index,
            shared_bits,
            left_shared_indices,
            right_shared_indices,
            compatible_row_blocks,
            compatible_row_pair_count,
        });
    }

    let tables = tables
        .iter()
        .enumerate()
        .map(|(table_index, table)| TableGraphNode {
            table_index,
            bits: table.bits.clone(),
            rows: table.rows.clone(),
            adjacent_table_count: incident_counts[table_index],
        })
        .collect::<Vec<_>>();
    stats.isolated_table_count = incident_counts.iter().filter(|&&count| count == 0).count();

    TableBipartiteGraph {
        stats,
        tables,
        edges,
    }
}

fn build_table_pair_shared_bits(tables: &[Table]) -> BTreeMap<(usize, usize), Vec<u32>> {
    let mut bit_to_tables: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }

    let mut pair_to_shared_bits: BTreeMap<(usize, usize), Vec<u32>> = BTreeMap::new();
    for (bit, table_indices) in bit_to_tables {
        for left_pos in 0..table_indices.len() {
            for right_pos in (left_pos + 1)..table_indices.len() {
                pair_to_shared_bits
                    .entry((table_indices[left_pos], table_indices[right_pos]))
                    .or_default()
                    .push(bit);
            }
        }
    }

    pair_to_shared_bits
}

fn shared_indices_for_bits(table_bits: &[u32], shared_bits: &[u32]) -> Vec<usize> {
    shared_bits
        .iter()
        .map(|bit| {
            table_bits
                .binary_search(bit)
                .expect("shared bit must exist in table bits")
        })
        .collect()
}

fn build_compatible_row_blocks(
    left_rows: &[u32],
    left_shared_indices: &[usize],
    right_rows: &[u32],
    right_shared_indices: &[usize],
) -> (Vec<CompatibleRowBlock>, u64) {
    if left_shared_indices.len() <= 16 {
        build_dense_compatible_row_blocks(
            left_rows,
            left_shared_indices,
            right_rows,
            right_shared_indices,
        )
    } else {
        build_sparse_compatible_row_blocks(
            left_rows,
            left_shared_indices,
            right_rows,
            right_shared_indices,
        )
    }
}

fn build_dense_compatible_row_blocks(
    left_rows: &[u32],
    left_shared_indices: &[usize],
    right_rows: &[u32],
    right_shared_indices: &[usize],
) -> (Vec<CompatibleRowBlock>, u64) {
    let bucket_count = 1usize << left_shared_indices.len();
    let mut left_buckets = vec![Vec::new(); bucket_count];
    let mut right_buckets = vec![Vec::new(); bucket_count];

    for (row_index, &row) in left_rows.iter().enumerate() {
        let key = project_row(row, left_shared_indices) as usize;
        left_buckets[key].push(row_index);
    }
    for (row_index, &row) in right_rows.iter().enumerate() {
        let key = project_row(row, right_shared_indices) as usize;
        right_buckets[key].push(row_index);
    }

    let mut compatible_row_blocks = Vec::new();
    let mut compatible_row_pair_count = 0u64;
    for shared_assignment in 0..bucket_count {
        if left_buckets[shared_assignment].is_empty() || right_buckets[shared_assignment].is_empty()
        {
            continue;
        }

        let edge_count = (left_buckets[shared_assignment].len() as u64)
            * (right_buckets[shared_assignment].len() as u64);
        compatible_row_pair_count += edge_count;
        compatible_row_blocks.push(CompatibleRowBlock {
            shared_assignment: shared_assignment as u32,
            left_row_indices: left_buckets[shared_assignment].clone(),
            right_row_indices: right_buckets[shared_assignment].clone(),
            edge_count,
        });
    }

    (compatible_row_blocks, compatible_row_pair_count)
}

fn build_sparse_compatible_row_blocks(
    left_rows: &[u32],
    left_shared_indices: &[usize],
    right_rows: &[u32],
    right_shared_indices: &[usize],
) -> (Vec<CompatibleRowBlock>, u64) {
    let left_buckets = build_sparse_projection_buckets(left_rows, left_shared_indices);
    let right_buckets = build_sparse_projection_buckets(right_rows, right_shared_indices);
    let mut compatible_row_blocks = Vec::new();
    let mut compatible_row_pair_count = 0u64;

    for (shared_assignment, left_row_indices) in left_buckets {
        let Some(right_row_indices) = right_buckets.get(&shared_assignment) else {
            continue;
        };
        let edge_count = (left_row_indices.len() as u64) * (right_row_indices.len() as u64);
        compatible_row_pair_count += edge_count;
        compatible_row_blocks.push(CompatibleRowBlock {
            shared_assignment,
            left_row_indices,
            right_row_indices: right_row_indices.clone(),
            edge_count,
        });
    }

    (compatible_row_blocks, compatible_row_pair_count)
}

fn build_sparse_projection_buckets(
    rows: &[u32],
    shared_indices: &[usize],
) -> BTreeMap<u32, Vec<usize>> {
    let mut buckets: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (row_index, &row) in rows.iter().enumerate() {
        let shared_assignment = project_row(row, shared_indices);
        buckets
            .entry(shared_assignment)
            .or_default()
            .push(row_index);
    }
    buckets
}

#[cfg(test)]
mod tests {
    use super::build_table_bipartite_graph;
    use crate::common::Table;

    fn table(bits: &[u32], rows: &[u32]) -> Table {
        Table {
            bits: bits.to_vec(),
            rows: rows.to_vec(),
        }
    }

    #[test]
    fn connects_tables_that_share_bits_and_groups_compatible_rows() {
        let tables = vec![
            table(&[1, 2], &[0b00, 0b10, 0b11]),
            table(&[2, 3], &[0b00, 0b01, 0b11]),
        ];

        let graph = build_table_bipartite_graph(&tables);

        assert_eq!(graph.stats.table_count, 2);
        assert_eq!(graph.stats.table_edge_count, 1);
        assert_eq!(graph.stats.compatible_row_block_count, 2);
        assert_eq!(graph.stats.compatible_row_pair_count, 5);
        assert_eq!(graph.tables[0].adjacent_table_count, 1);
        assert_eq!(graph.tables[1].adjacent_table_count, 1);

        let edge = &graph.edges[0];
        assert_eq!(edge.left_table_index, 0);
        assert_eq!(edge.right_table_index, 1);
        assert_eq!(edge.shared_bits, vec![2]);
        assert_eq!(edge.left_shared_indices, vec![1]);
        assert_eq!(edge.right_shared_indices, vec![0]);
        assert_eq!(edge.compatible_row_pair_count, 5);
        assert_eq!(edge.compatible_row_blocks.len(), 2);
        assert_eq!(edge.compatible_row_blocks[0].shared_assignment, 0);
        assert_eq!(edge.compatible_row_blocks[0].left_row_indices, vec![0]);
        assert_eq!(edge.compatible_row_blocks[0].right_row_indices, vec![0]);
        assert_eq!(edge.compatible_row_blocks[0].edge_count, 1);
        assert_eq!(edge.compatible_row_blocks[1].shared_assignment, 1);
        assert_eq!(edge.compatible_row_blocks[1].left_row_indices, vec![1, 2]);
        assert_eq!(edge.compatible_row_blocks[1].right_row_indices, vec![1, 2]);
        assert_eq!(edge.compatible_row_blocks[1].edge_count, 4);
    }

    #[test]
    fn leaves_disconnected_tables_isolated() {
        let tables = vec![
            table(&[1, 2], &[0b00, 0b01]),
            table(&[3, 4], &[0b00, 0b11]),
            table(&[4, 5], &[0b00, 0b10]),
        ];

        let graph = build_table_bipartite_graph(&tables);

        assert_eq!(graph.stats.table_edge_count, 1);
        assert_eq!(graph.stats.isolated_table_count, 1);
        assert_eq!(graph.tables[0].adjacent_table_count, 0);
        assert_eq!(graph.tables[1].adjacent_table_count, 1);
        assert_eq!(graph.tables[2].adjacent_table_count, 1);
        assert_eq!(graph.edges[0].left_table_index, 1);
        assert_eq!(graph.edges[0].right_table_index, 2);
        assert_eq!(graph.edges[0].shared_bits, vec![4]);
    }

    #[test]
    fn keeps_table_edge_even_when_no_rows_are_compatible() {
        let tables = vec![table(&[1, 2], &[0b00]), table(&[2, 3], &[0b11])];

        let graph = build_table_bipartite_graph(&tables);

        assert_eq!(graph.stats.table_edge_count, 1);
        assert_eq!(graph.stats.compatible_row_block_count, 0);
        assert_eq!(graph.stats.compatible_row_pair_count, 0);
        assert!(graph.edges[0].compatible_row_blocks.is_empty());
    }

    #[test]
    fn handles_sparse_shared_assignments_when_many_bits_are_shared() {
        let mut left_bits: Vec<u32> = (1..=17).collect();
        left_bits.push(100);
        let mut right_bits: Vec<u32> = (1..=17).collect();
        right_bits.push(200);
        let tables = vec![
            table(&left_bits, &[0, 1u32 << 16, (1u32 << 16) | (1u32 << 17)]),
            table(&right_bits, &[0, 1u32 << 16, (1u32 << 16) | (1u32 << 17)]),
        ];

        let graph = build_table_bipartite_graph(&tables);

        assert_eq!(graph.stats.table_edge_count, 1);
        assert_eq!(graph.stats.compatible_row_block_count, 2);
        assert_eq!(graph.stats.compatible_row_pair_count, 5);
        assert_eq!(graph.edges[0].compatible_row_blocks[0].shared_assignment, 0);
        assert_eq!(graph.edges[0].compatible_row_blocks[0].edge_count, 1);
        assert_eq!(
            graph.edges[0].compatible_row_blocks[1].shared_assignment,
            1u32 << 16
        );
        assert_eq!(graph.edges[0].compatible_row_blocks[1].edge_count, 4);
    }
}
