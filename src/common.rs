use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::tables_file::{
    has_tables_extension, read_tables_from_tables_file, write_tables_to_tables_file,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub rows: Vec<u32>,
    pub bits: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForcedRow {
    pub bit: u32,
    pub value: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RewriteRow {
    pub bit: u32,
    pub representative: u32,
    pub inverted: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ComponentMember {
    pub bit: u32,
    pub representative: u32,
    pub inverted: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ComponentRow {
    pub representative: u32,
    pub size: usize,
    pub members: Vec<ComponentMember>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DroppedTableRecord {
    pub round: usize,
    pub bits: Vec<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PairRelationRecord {
    pub round: usize,
    pub iteration: usize,
    pub left: u32,
    pub right: u32,
    pub equal: bool,
    pub inverted: bool,
    pub support: usize,
    pub source_arities: Vec<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeArtifact {
    pub bits: Vec<u32>,
    pub rows: Vec<u32>,
    pub members: Vec<usize>,
}

pub fn read_tables_json(path: &Path) -> Result<Vec<Table>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let tables = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(tables)
}

pub fn read_tables(path: &Path) -> Result<Vec<Table>> {
    if has_tables_extension(path) {
        return read_tables_from_tables_file(path);
    }
    read_tables_json(path)
}

pub fn write_json<T: Serialize + ?Sized>(path: &Path, payload: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut json = serde_json::to_vec_pretty(payload)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    json.push(b'\n');
    fs::write(path, json).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub fn write_tables(path: &Path, tables: &[Table]) -> Result<()> {
    if has_tables_extension(path) {
        return write_tables_to_tables_file(path, tables);
    }
    write_json(path, tables)
}

pub fn sort_dedup_rows(rows: &mut Vec<u32>) {
    rows.sort_unstable();
    rows.dedup();
}

pub fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
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

pub fn tables_from_canonical_map(tables_by_bits: &BTreeMap<Vec<u32>, Vec<u32>>) -> Vec<Table> {
    let mut entries: Vec<_> = tables_by_bits.iter().collect();
    entries.sort_by(|(left_bits, _), (right_bits, _)| {
        left_bits
            .len()
            .cmp(&right_bits.len())
            .then_with(|| left_bits.cmp(right_bits))
    });
    entries
        .into_iter()
        .map(|(bits, rows)| Table {
            bits: bits.clone(),
            rows: rows.clone(),
        })
        .collect()
}

pub fn collect_bits(tables: &[Table]) -> Vec<u32> {
    let mut bits = Vec::new();
    for table in tables {
        bits.extend_from_slice(&table.bits);
    }
    bits.sort_unstable();
    bits.dedup();
    bits
}

pub fn total_rows(tables: &[Table]) -> usize {
    tables.iter().map(|table| table.rows.len()).sum()
}

pub fn arity_distribution(tables: &[Table]) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for table in tables {
        let key = table.bits.len().to_string();
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

pub fn project_row(row: u32, subset_indices: &[usize]) -> u32 {
    let mut projected = 0u32;
    for (new_offset, old_offset) in subset_indices.iter().copied().enumerate() {
        if ((row >> old_offset) & 1) != 0 {
            projected |= 1u32 << new_offset;
        }
    }
    projected
}

pub fn for_each_combination<F>(length: usize, choose: usize, mut f: F)
where
    F: FnMut(&[usize]),
{
    if choose == 0 || choose > length {
        return;
    }

    let mut indices: Vec<usize> = (0..choose).collect();
    loop {
        f(&indices);

        let mut pivot = choose;
        while pivot > 0 && indices[pivot - 1] == length - choose + pivot - 1 {
            pivot -= 1;
        }
        if pivot == 0 {
            break;
        }

        indices[pivot - 1] += 1;
        for index in pivot..choose {
            indices[index] = indices[index - 1] + 1;
        }
    }
}

pub fn is_full_row_set(row_count: usize, bit_count: usize) -> bool {
    bit_count < usize::BITS as usize && row_count == (1usize << bit_count)
}
