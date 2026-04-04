#![recursion_limit = "256"]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, sort_dedup_rows, total_rows, write_json, Table};
use tables::rank_stats::compute_rank;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    target_arity: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from(
                "runs/2026-04-03-origin-greedy-rank-chain/tables.after_target_arity16.json",
            ),
            report: PathBuf::from(
                "runs/2026-04-03-origin-greedy-rank-chain/report.after_target_arity16.json",
            ),
            target_arity: 16,
        }
    }
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self::default();
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => args.input = PathBuf::from(expect_value(&mut iter, "--input")?),
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
                "--target-arity" => {
                    args.target_arity = expect_value(&mut iter, "--target-arity")?
                        .parse()
                        .with_context(|| "invalid value for --target-arity")?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(args)
    }
}

#[derive(Clone)]
struct TableEntry {
    table: Table,
    active: bool,
    parent_left: Option<usize>,
    parent_right: Option<usize>,
}

#[derive(Clone, Copy)]
struct PairCandidate {
    left_index: usize,
    right_index: usize,
    predicted_rows: usize,
    union_arity: usize,
    predicted_rank: f64,
}

impl Ord for PairCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .predicted_rank
            .total_cmp(&self.predicted_rank)
            .then_with(|| other.predicted_rows.cmp(&self.predicted_rows))
            .then_with(|| other.union_arity.cmp(&self.union_arity))
            .then_with(|| other.left_index.cmp(&self.left_index))
            .then_with(|| other.right_index.cmp(&self.right_index))
    }
}

impl PartialOrd for PairCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PairCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.left_index == other.left_index
            && self.right_index == other.right_index
            && self.predicted_rows == other.predicted_rows
            && self.union_arity == other.union_arity
            && self.predicted_rank.to_bits() == other.predicted_rank.to_bits()
    }
}

impl Eq for PairCandidate {}

#[derive(Serialize)]
struct StepReport {
    step: usize,
    left_index: usize,
    right_index: usize,
    new_index: usize,
    left_arity: usize,
    right_arity: usize,
    left_rows: usize,
    right_rows: usize,
    shared_bit_count: usize,
    union_arity: usize,
    predicted_rows: usize,
    predicted_rank: f64,
    actual_rows: usize,
    actual_rank: f64,
    active_table_count_after_step: usize,
}

#[derive(Serialize)]
struct FinalTableReport {
    index: usize,
    arity: usize,
    row_count: usize,
    rank: f64,
    bits: Vec<u32>,
    parent_left: Option<usize>,
    parent_right: Option<usize>,
}

#[derive(Serialize)]
struct Report {
    method: String,
    input: String,
    output: String,
    target_arity: usize,
    reached_target_arity: bool,
    initial_table_count: usize,
    final_active_table_count: usize,
    initial_total_rows: usize,
    final_total_rows: usize,
    initial_candidate_pair_count: usize,
    total_steps: usize,
    build_initial_queue_seconds: f64,
    total_seconds: f64,
    final_table: Option<FinalTableReport>,
    steps: Vec<StepReport>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let mut entries: Vec<TableEntry> = canonicalize_tables(read_tables(&args.input)?)
        .into_iter()
        .map(|table| TableEntry {
            table,
            active: true,
            parent_left: None,
            parent_right: None,
        })
        .collect();

    let initial_table_count = entries.len();
    let initial_total_rows: usize = entries.iter().map(|entry| entry.table.rows.len()).sum();

    let mut bit_to_tables: HashMap<u32, Vec<usize>> = HashMap::new();
    for (table_index, entry) in entries.iter().enumerate() {
        for &bit in &entry.table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }

    let queue_started = Instant::now();
    let initial_pair_keys = generate_shared_pairs_from_bit_map(&bit_to_tables);
    let initial_candidate_pair_count = initial_pair_keys.len();
    let mut queue = BinaryHeap::with_capacity(initial_pair_keys.len());
    for (pair_index, &pair_key) in initial_pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let candidate = build_candidate(&entries[left_index].table, &entries[right_index].table, left_index, right_index);
        queue.push(candidate);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "initial queue: {}/{}",
                pair_index + 1,
                initial_pair_keys.len()
            );
        }
    }
    let build_initial_queue_seconds = queue_started.elapsed().as_secs_f64();

    let total_started = Instant::now();
    let mut steps = Vec::new();
    let mut active_table_count = entries.len();
    let mut reached_target_index = None;

    while let Some(candidate) = pop_best_active_candidate(&mut queue, &entries) {
        let left = &entries[candidate.left_index].table;
        let right = &entries[candidate.right_index].table;
        let left_arity = left.bits.len();
        let right_arity = right.bits.len();
        let left_rows = left.rows.len();
        let right_rows = right.rows.len();

        let shared_bit_count = count_shared_bits(&left.bits, &right.bits);
        let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
            .map_err(|error| anyhow!(error))
            .with_context(|| {
                format!(
                    "failed to merge pair ({}, {})",
                    candidate.left_index, candidate.right_index
                )
            })?;

        let actual_rows = merged.rows.len();
        let actual_rank = compute_rank(actual_rows, merged.bits.len());

        entries[candidate.left_index].active = false;
        entries[candidate.right_index].active = false;
        active_table_count = active_table_count.saturating_sub(2);

        let new_index = entries.len();
        entries.push(TableEntry {
            table: Table {
                bits: merged.bits.clone(),
                rows: merged.rows.clone(),
            },
            active: true,
            parent_left: Some(candidate.left_index),
            parent_right: Some(candidate.right_index),
        });
        active_table_count += 1;

        for &bit in &merged.bits {
            bit_to_tables.entry(bit).or_default().push(new_index);
        }
        push_new_table_candidates(new_index, &entries, &bit_to_tables, &mut queue);

        let step = StepReport {
            step: steps.len() + 1,
            left_index: candidate.left_index,
            right_index: candidate.right_index,
            new_index,
            left_arity,
            right_arity,
            left_rows,
            right_rows,
            shared_bit_count,
            union_arity: candidate.union_arity,
            predicted_rows: candidate.predicted_rows,
            predicted_rank: candidate.predicted_rank,
            actual_rows,
            actual_rank,
            active_table_count_after_step: active_table_count,
        };
        steps.push(step);

        if steps.len() % 1000 == 0 {
            println!(
                "steps={} active_tables={} last_arity={} last_rank={:.12}",
                steps.len(),
                active_table_count,
                candidate.union_arity,
                candidate.predicted_rank
            );
        }

        if merged.bits.len() == args.target_arity {
            reached_target_index = Some(new_index);
            break;
        }
    }

    let final_active_tables: Vec<Table> = entries
        .iter()
        .filter(|entry| entry.active)
        .map(|entry| entry.table.clone())
        .collect();
    let final_total_rows = total_rows(&final_active_tables);
    let total_seconds = total_started.elapsed().as_secs_f64();

    let final_table = reached_target_index.map(|index| {
        let entry = &entries[index];
        FinalTableReport {
            index,
            arity: entry.table.bits.len(),
            row_count: entry.table.rows.len(),
            rank: compute_rank(entry.table.rows.len(), entry.table.bits.len()),
            bits: entry.table.bits.clone(),
            parent_left: entry.parent_left,
            parent_right: entry.parent_right,
        }
    });

    let report = Report {
        method: "Iteratively select the active table pair with minimum predicted rank under shared_assignment_histogram, remove both source tables, add the exact merge, and stop when a newly created table reaches target arity".to_string(),
        input: path_string(&args.input),
        output: path_string(&args.output),
        target_arity: args.target_arity,
        reached_target_arity: reached_target_index.is_some(),
        initial_table_count,
        final_active_table_count: final_active_tables.len(),
        initial_total_rows,
        final_total_rows,
        initial_candidate_pair_count,
        total_steps: steps.len(),
        build_initial_queue_seconds,
        total_seconds,
        final_table,
        steps,
    };

    write_json(&args.output, &final_active_tables)?;
    write_json(&args.report, &report)?;

    if let Some(index) = reached_target_index {
        println!("reached target arity with table {}", index);
    } else {
        println!("target arity was not reached");
    }
    println!("steps: {}", report.total_steps);
    println!("active tables: {}", report.final_active_table_count);
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

fn build_candidate(left: &Table, right: &Table, left_index: usize, right_index: usize) -> PairCandidate {
    let shared_indices = shared_indices(left, right);
    let union_arity = left.bits.len() + right.bits.len() - shared_indices.0.len();
    let predicted_rows = predict_shared_assignment_histogram(left, right, &shared_indices) as usize;
    let predicted_rank = compute_rank(predicted_rows, union_arity);
    PairCandidate {
        left_index,
        right_index,
        predicted_rows,
        union_arity,
        predicted_rank,
    }
}

fn generate_shared_pairs_from_bit_map(bit_to_tables: &HashMap<u32, Vec<usize>>) -> Vec<u64> {
    let mut pair_keys = HashSet::new();
    for table_ids in bit_to_tables.values() {
        for left_offset in 0..table_ids.len() {
            for right_offset in (left_offset + 1)..table_ids.len() {
                pair_keys.insert(pair_key(
                    table_ids[left_offset] as u32,
                    table_ids[right_offset] as u32,
                ));
            }
        }
    }

    let mut pair_keys: Vec<u64> = pair_keys.into_iter().collect();
    pair_keys.sort_unstable();
    pair_keys
}

fn pop_best_active_candidate(
    queue: &mut BinaryHeap<PairCandidate>,
    entries: &[TableEntry],
) -> Option<PairCandidate> {
    while let Some(candidate) = queue.pop() {
        if entries[candidate.left_index].active && entries[candidate.right_index].active {
            return Some(candidate);
        }
    }
    None
}

fn push_new_table_candidates(
    new_index: usize,
    entries: &[TableEntry],
    bit_to_tables: &HashMap<u32, Vec<usize>>,
    queue: &mut BinaryHeap<PairCandidate>,
) {
    let new_table = &entries[new_index].table;
    let mut neighbors = HashSet::new();
    for &bit in &new_table.bits {
        if let Some(table_ids) = bit_to_tables.get(&bit) {
            for &other_index in table_ids {
                if other_index != new_index && entries[other_index].active {
                    neighbors.insert(other_index);
                }
            }
        }
    }

    for other_index in neighbors {
        let left_index = other_index.min(new_index);
        let right_index = other_index.max(new_index);
        let candidate = build_candidate(
            &entries[left_index].table,
            &entries[right_index].table,
            left_index,
            right_index,
        );
        queue.push(candidate);
    }
}

fn shared_indices(left: &Table, right: &Table) -> (Vec<u8>, Vec<u8>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut left_shared = Vec::new();
    let mut right_shared = Vec::new();

    while left_index < left.bits.len() && right_index < right.bits.len() {
        match left.bits[left_index].cmp(&right.bits[right_index]) {
            Ordering::Less => left_index += 1,
            Ordering::Greater => right_index += 1,
            Ordering::Equal => {
                left_shared.push(left_index as u8);
                right_shared.push(right_index as u8);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    (left_shared, right_shared)
}

fn count_shared_bits(left_bits: &[u32], right_bits: &[u32]) -> usize {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut shared = 0usize;

    while left_index < left_bits.len() && right_index < right_bits.len() {
        match left_bits[left_index].cmp(&right_bits[right_index]) {
            Ordering::Less => left_index += 1,
            Ordering::Greater => right_index += 1,
            Ordering::Equal => {
                shared += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }

    shared
}

fn predict_shared_assignment_histogram(
    left: &Table,
    right: &Table,
    shared_indices: &(Vec<u8>, Vec<u8>),
) -> u64 {
    let (left_shared_indices, right_shared_indices) = shared_indices;
    let shared_count = left_shared_indices.len();

    let (build_rows, build_indices, probe_rows, probe_indices) = if left.rows.len() <= right.rows.len()
    {
        (&left.rows, left_shared_indices.as_slice(), &right.rows, right_shared_indices.as_slice())
    } else {
        (&right.rows, right_shared_indices.as_slice(), &left.rows, left_shared_indices.as_slice())
    };

    if shared_count <= 16 {
        let mut counts = vec![0u32; 1usize << shared_count];
        for &row in build_rows {
            let key = project_bits(row, build_indices) as usize;
            counts[key] += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices) as usize;
            total += counts[key] as u64;
        }
        total
    } else {
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for &row in build_rows {
            let key = project_bits(row, build_indices);
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices);
            total += counts.get(&key).copied().unwrap_or(0) as u64;
        }
        total
    }
}

fn project_bits(row: u32, indices: &[u8]) -> u32 {
    let mut projected = 0u32;
    for (new_pos, &old_pos) in indices.iter().enumerate() {
        projected |= ((row >> old_pos) & 1) << new_pos;
    }
    projected
}

fn canonicalize_tables(tables: Vec<Table>) -> Vec<Table> {
    tables.into_iter().map(canonicalize_table).collect()
}

fn canonicalize_table(mut table: Table) -> Table {
    let mut order: Vec<usize> = (0..table.bits.len()).collect();
    order.sort_unstable_by_key(|&index| table.bits[index]);

    if order
        .iter()
        .copied()
        .enumerate()
        .all(|(new_index, old_index)| new_index == old_index)
    {
        sort_dedup_rows(&mut table.rows);
        return table;
    }

    let mut inverse = vec![0usize; order.len()];
    for (new_index, old_index) in order.iter().copied().enumerate() {
        inverse[old_index] = new_index;
    }

    let old_bits = table.bits;
    let old_rows = table.rows;
    let new_bits: Vec<u32> = order.iter().map(|&index| old_bits[index]).collect();
    let mut new_rows = Vec::with_capacity(old_rows.len());
    for row in old_rows {
        let mut remapped = 0u32;
        for (old_index, &new_index) in inverse.iter().enumerate() {
            if ((row >> old_index) & 1) != 0 {
                remapped |= 1u32 << new_index;
            }
        }
        new_rows.push(remapped);
    }
    sort_dedup_rows(&mut new_rows);

    Table {
        bits: new_bits,
        rows: new_rows,
    }
}

fn pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = if left < right {
        (left as u64, right as u64)
    } else {
        (right as u64, left as u64)
    };
    (left << 32) | right
}

fn unpack_pair_key(key: u64) -> (usize, usize) {
    ((key >> 32) as usize, key as u32 as usize)
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin greedy_rank_chain -- --input <path> --output <path> --report <path> [--target-arity <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
