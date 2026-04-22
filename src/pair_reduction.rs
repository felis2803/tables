use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{bail, Result};
use serde::Serialize;

use crate::common::{ComponentMember, ComponentRow, PairRelationRecord, RewriteRow, Table};

const EQUAL_MASKS: [u8; 3] = [1, 8, 9];
const OPPOSITE_MASKS: [u8; 3] = [2, 4, 6];

#[derive(Clone, Debug)]
pub struct Relation {
    pub left: u32,
    pub right: u32,
    pub relation: u8,
    pub support: usize,
    pub sources: Vec<usize>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RewriteMapStats {
    pub bits_involved: usize,
    pub component_count: usize,
    pub replaced_bit_count: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RewriteTablesStats {
    pub changed_tables: usize,
    pub reduced_arity_tables: usize,
    pub same_arity_changed_tables: usize,
    pub removed_rows: usize,
    pub collapsed_duplicate_tables: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct PairReductionIterationInfo {
    pub iteration: usize,
    pub relation_pair_count: usize,
    pub bits_involved: usize,
    pub component_count: usize,
    pub replaced_bit_count: usize,
    pub changed_tables: usize,
    pub reduced_arity_tables: usize,
    pub same_arity_changed_tables: usize,
    pub removed_rows: usize,
    pub collapsed_duplicate_tables: usize,
    pub table_count_after_iteration: usize,
    pub bit_count_after_iteration: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct PairReductionInfo {
    pub iterations: Vec<PairReductionIterationInfo>,
    pub pair_relation_pairs_total: usize,
    pub pair_replaced_bits_total: usize,
}

#[derive(Default)]
struct ParityUnionFind {
    parent: HashMap<u32, u32>,
    rank: HashMap<u32, usize>,
    parity: HashMap<u32, u8>,
}

impl ParityUnionFind {
    fn add(&mut self, bit: u32) {
        self.parent.entry(bit).or_insert(bit);
        self.rank.entry(bit).or_insert(0);
        self.parity.entry(bit).or_insert(0);
    }

    fn find(&mut self, bit: u32) -> (u32, u8) {
        self.add(bit);
        let parent = self.parent[&bit];
        if parent != bit {
            let (root, parent_parity) = self.find(parent);
            let entry = self.parity.entry(bit).or_insert(0);
            *entry ^= parent_parity;
            self.parent.insert(bit, root);
        }
        (self.parent[&bit], self.parity[&bit])
    }

    fn union(&mut self, left: u32, right: u32, relation: u8) -> bool {
        let (mut left_root, mut left_parity) = self.find(left);
        let (mut right_root, mut right_parity) = self.find(right);

        if left_root == right_root {
            return (left_parity ^ right_parity) == relation;
        }

        let left_rank = *self.rank.get(&left_root).unwrap_or(&0);
        let right_rank = *self.rank.get(&right_root).unwrap_or(&0);
        if left_rank < right_rank {
            std::mem::swap(&mut left_root, &mut right_root);
            std::mem::swap(&mut left_parity, &mut right_parity);
        }

        self.parent.insert(right_root, left_root);
        self.parity
            .insert(right_root, left_parity ^ right_parity ^ relation);

        if left_rank == right_rank {
            *self.rank.entry(left_root).or_insert(0) += 1;
        }

        true
    }
}

pub fn extract_relations(tables: &[Table]) -> Result<Vec<Relation>> {
    let mut relation_map: HashMap<(u32, u32), (u8, usize, BTreeSet<usize>)> = HashMap::new();

    for table in tables {
        let bit_count = table.bits.len();
        for left_index in 0..bit_count {
            for right_index in (left_index + 1)..bit_count {
                let mut mask = 0u8;
                for &row in &table.rows {
                    let pair_state =
                        ((row >> left_index) & 1) as u8 | ((((row >> right_index) & 1) as u8) << 1);
                    mask |= 1u8 << pair_state;
                }

                let relation = if EQUAL_MASKS.contains(&mask) {
                    Some(0u8)
                } else if OPPOSITE_MASKS.contains(&mask) {
                    Some(1u8)
                } else {
                    None
                };

                let Some(relation) = relation else {
                    continue;
                };

                let left_bit = table.bits[left_index];
                let right_bit = table.bits[right_index];
                let entry = relation_map
                    .entry((left_bit, right_bit))
                    .or_insert_with(|| (relation, 0usize, BTreeSet::new()));
                if entry.0 != relation {
                    bail!("conflicting direct relations for pair ({left_bit}, {right_bit})");
                }
                entry.1 += 1;
                entry.2.insert(bit_count);
            }
        }
    }

    let mut relations: Vec<_> = relation_map
        .into_iter()
        .map(|((left, right), (relation, support, sources))| Relation {
            left,
            right,
            relation,
            support,
            sources: sources.into_iter().collect(),
        })
        .collect();
    relations.sort_by(|left, right| {
        left.left
            .cmp(&right.left)
            .then_with(|| left.right.cmp(&right.right))
            .then_with(|| left.relation.cmp(&right.relation))
    });
    Ok(relations)
}

pub fn build_rewrite_map(
    relations: &[Relation],
) -> Result<(HashMap<u32, (u32, u8)>, RewriteMapStats)> {
    let mut union_find = ParityUnionFind::default();

    for relation in relations {
        if !union_find.union(relation.left, relation.right, relation.relation) {
            bail!(
                "conflicting transitive relations for pair ({}, {})",
                relation.left,
                relation.right
            );
        }
    }

    let mut components: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    let keys: Vec<u32> = union_find.parent.keys().copied().collect();
    for bit in keys {
        let (root, _) = union_find.find(bit);
        components.entry(root).or_default().push(bit);
    }

    let mut rewrite_map = HashMap::new();
    for bits in components.values() {
        let representative = *bits.iter().min().unwrap();
        let (_, representative_parity) = union_find.find(representative);
        for &bit in bits {
            let (_, bit_parity) = union_find.find(bit);
            rewrite_map.insert(bit, (representative, bit_parity ^ representative_parity));
        }
    }

    let stats = RewriteMapStats {
        bits_involved: union_find.parent.len(),
        component_count: components.len(),
        replaced_bit_count: union_find.parent.len().saturating_sub(components.len()),
    };
    Ok((rewrite_map, stats))
}

pub fn protect_bits_in_rewrite_map(
    rewrite_map: &HashMap<u32, (u32, u8)>,
    protected_bits: &BTreeSet<u32>,
) -> HashMap<u32, (u32, u8)> {
    let mut groups: BTreeMap<u32, Vec<(u32, u8)>> = BTreeMap::new();
    for (&bit, &(rep, inv)) in rewrite_map {
        groups.entry(rep).or_default().push((bit, inv));
    }

    let mut out = HashMap::new();
    for members in groups.values() {
        let preferred = members
            .iter()
            .map(|(bit, _)| *bit)
            .filter(|bit| protected_bits.contains(bit))
            .min()
            .or_else(|| members.iter().map(|(bit, _)| *bit).min())
            .unwrap();
        let preferred_inv = members
            .iter()
            .find(|(bit, _)| *bit == preferred)
            .map(|(_, inv)| *inv)
            .unwrap_or(0);

        for &(bit, inv) in members {
            if protected_bits.contains(&bit) {
                out.insert(bit, (bit, 0));
            } else {
                out.insert(bit, (preferred, inv ^ preferred_inv));
            }
        }
    }

    out
}

pub fn rewrite_tables(
    tables: &[Table],
    rewrite_map: &HashMap<u32, (u32, u8)>,
) -> (Vec<Table>, RewriteTablesStats) {
    let mut merged: BTreeMap<Vec<u32>, Vec<u32>> = BTreeMap::new();
    let mut stats = RewriteTablesStats::default();

    for table in tables {
        let mut new_bits: Vec<u32> = table
            .bits
            .iter()
            .map(|bit| {
                rewrite_map
                    .get(bit)
                    .map_or(*bit, |&(representative, _)| representative)
            })
            .collect();
        new_bits.sort_unstable();
        new_bits.dedup();

        let new_index: HashMap<u32, usize> = new_bits
            .iter()
            .enumerate()
            .map(|(index, &bit)| (bit, index))
            .collect();

        let mut new_rows = Vec::with_capacity(table.rows.len());
        for &row in &table.rows {
            let mut assignments: Vec<(u32, u8)> = Vec::with_capacity(table.bits.len());
            let mut consistent = true;

            for (offset, &bit) in table.bits.iter().enumerate() {
                let (representative, inverted) = rewrite_map.get(&bit).copied().unwrap_or((bit, 0));
                let value = (((row >> offset) & 1) as u8) ^ inverted;
                if let Some((_, previous)) = assignments
                    .iter()
                    .find(|(member_bit, _)| *member_bit == representative)
                {
                    if *previous != value {
                        consistent = false;
                        break;
                    }
                } else {
                    assignments.push((representative, value));
                }
            }

            if !consistent {
                continue;
            }

            let mut new_row = 0u32;
            for (bit, value) in assignments {
                if value != 0 {
                    new_row |= 1u32 << new_index[&bit];
                }
            }
            new_rows.push(new_row);
        }

        new_rows.sort_unstable();
        new_rows.dedup();

        if new_bits != table.bits || new_rows != table.rows {
            stats.changed_tables += 1;
            if new_bits.len() < table.bits.len() {
                stats.reduced_arity_tables += 1;
            } else {
                stats.same_arity_changed_tables += 1;
            }
        }

        stats.removed_rows += table.rows.len() - new_rows.len();

        if let Some(existing_rows) = merged.get_mut(&new_bits) {
            let intersection = crate::common::intersect_sorted(existing_rows, &new_rows);
            *existing_rows = intersection;
        } else {
            merged.insert(new_bits, new_rows);
        }
    }

    let mut output_tables = crate::common::tables_from_canonical_map(&merged);
    output_tables.sort_by(|left, right| {
        left.bits
            .len()
            .cmp(&right.bits.len())
            .then_with(|| left.bits.cmp(&right.bits))
    });
    stats.collapsed_duplicate_tables = tables.len().saturating_sub(output_tables.len());

    (output_tables, stats)
}

pub fn update_original_mapping(
    mapping: &BTreeMap<u32, (u32, u8)>,
    rewrite_map: &HashMap<u32, (u32, u8)>,
) -> BTreeMap<u32, (u32, u8)> {
    let mut updated = BTreeMap::new();
    for (&bit, &(current, inverted)) in mapping {
        if let Some(&(representative, current_inverted)) = rewrite_map.get(&current) {
            updated.insert(bit, (representative, inverted ^ current_inverted));
        } else {
            updated.insert(bit, (current, inverted));
        }
    }
    updated
}

pub fn build_rewrite_rows(
    original_mapping: &BTreeMap<u32, (u32, u8)>,
    original_forced: &BTreeMap<u32, u8>,
) -> Vec<RewriteRow> {
    original_mapping
        .iter()
        .filter_map(|(&bit, &(representative, inverted))| {
            if original_forced.contains_key(&bit) || (bit == representative && inverted == 0) {
                None
            } else {
                Some(RewriteRow {
                    bit,
                    representative,
                    inverted: inverted != 0,
                })
            }
        })
        .collect()
}

pub fn build_final_components(
    original_mapping: &BTreeMap<u32, (u32, u8)>,
    original_forced: &BTreeMap<u32, u8>,
) -> Vec<ComponentRow> {
    let mut grouped: BTreeMap<u32, Vec<ComponentMember>> = BTreeMap::new();
    for (&bit, &(representative, inverted)) in original_mapping {
        if original_forced.contains_key(&bit) {
            continue;
        }
        grouped
            .entry(representative)
            .or_default()
            .push(ComponentMember {
                bit,
                representative,
                inverted: inverted != 0,
            });
    }

    let mut components: Vec<_> = grouped
        .into_iter()
        .filter_map(|(representative, members)| {
            if members.len() > 1 {
                Some(ComponentRow {
                    representative,
                    size: members.len(),
                    members,
                })
            } else {
                None
            }
        })
        .collect();
    components.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.representative.cmp(&right.representative))
    });
    components
}

pub fn relation_history_rows(
    round: usize,
    iteration: usize,
    relations: &[Relation],
) -> Vec<PairRelationRecord> {
    relations
        .iter()
        .map(|relation| PairRelationRecord {
            round,
            iteration,
            left: relation.left,
            right: relation.right,
            equal: relation.relation == 0,
            inverted: relation.relation == 1,
            support: relation.support,
            source_arities: relation.sources.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_reduction_rewrites_equal_bits() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0b00, 0b11],
        }];

        let relations = extract_relations(&tables).unwrap();
        assert_eq!(relations.len(), 1);
        let (rewrite_map, stats) = build_rewrite_map(&relations).unwrap();
        assert_eq!(stats.replaced_bit_count, 1);

        let (rewritten, rewrite_stats) = rewrite_tables(&tables, &rewrite_map);
        assert_eq!(rewrite_stats.changed_tables, 1);
        assert_eq!(rewritten[0].bits, vec![1]);
    }

    #[test]
    fn protect_bits_in_rewrite_map_keeps_protected_bits_identity() {
        let rewrite_map = HashMap::from([
            (10u32, (10u32, 0u8)),
            (11u32, (10u32, 0u8)),
            (12u32, (10u32, 1u8)),
        ]);
        let protected_bits = BTreeSet::from([10u32, 11u32]);

        let protected = protect_bits_in_rewrite_map(&rewrite_map, &protected_bits);

        assert_eq!(protected.get(&10), Some(&(10, 0)));
        assert_eq!(protected.get(&11), Some(&(11, 0)));
        assert_eq!(protected.get(&12), Some(&(10, 1)));
    }
}
