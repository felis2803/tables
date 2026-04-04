#![recursion_limit = "256"]

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, sort_dedup_rows, write_json, Table};
use tables::rank_stats::compute_rank;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from(
                "runs/2026-04-03-origin-best-merge/tables.best_pair_merged.json",
            ),
            report: PathBuf::from(
                "runs/2026-04-03-origin-best-merge/report.best_pair_merged.json",
            ),
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

#[derive(Clone, Copy)]
struct BestCandidate {
    left_index: usize,
    right_index: usize,
    shared_count: usize,
    union_arity: usize,
    predicted_rows: usize,
    predicted_rank: f64,
}

#[derive(Serialize)]
struct Report {
    method: String,
    input: String,
    output: String,
    table_count: usize,
    candidate_pair_count: usize,
    search_seconds: f64,
    selected_pair: SelectedPair,
}

#[derive(Serialize)]
struct SelectedPair {
    left_index: usize,
    right_index: usize,
    left_bits: Vec<u32>,
    right_bits: Vec<u32>,
    left_row_count: usize,
    right_row_count: usize,
    shared_bit_count: usize,
    shared_bits: Vec<u32>,
    union_arity: usize,
    predicted_rows: usize,
    predicted_rank: f64,
    actual_rows: usize,
    actual_rank: f64,
    prediction_matches_actual_rows: bool,
    merged_bits: Vec<u32>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = canonicalize_tables(read_tables(&args.input)?);

    let started = Instant::now();
    let pair_keys = generate_shared_pairs(&tables);
    let mut best: Option<BestCandidate> = None;

    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left = &tables[left_index];
        let right = &tables[right_index];
        let (shared_bits, union_arity) = shared_bits_and_union_arity(&left.bits, &right.bits);
        let predicted_rows =
            predict_shared_assignment_histogram(left, right, &shared_bits_to_indices(left, right, &shared_bits));
        let predicted_rows = predicted_rows as usize;
        let predicted_rank = compute_rank(predicted_rows, union_arity);

        let candidate = BestCandidate {
            left_index,
            right_index,
            shared_count: shared_bits.len(),
            union_arity,
            predicted_rows,
            predicted_rank,
        };

        if best.is_none_or(|current| is_better_candidate(candidate, current)) {
            best = Some(candidate);
        }

        if (pair_index + 1) % 250_000 == 0 {
            println!("scanned pairs: {}/{}", pair_index + 1, pair_keys.len());
        }
    }

    let elapsed_seconds = started.elapsed().as_secs_f64();
    let best = best.context("no table pairs with shared bits found")?;

    let left = &tables[best.left_index];
    let right = &tables[best.right_index];
    let (shared_bits, _) = shared_bits_and_union_arity(&left.bits, &right.bits);
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!(error))
        .with_context(|| {
            format!(
                "failed to merge selected pair ({}, {})",
                best.left_index, best.right_index
            )
        })?;

    let actual_rows = merged.rows.len();
    let actual_rank = compute_rank(actual_rows, merged.bits.len());

    let report = Report {
        method: "Choose the origin-table pair with minimum expected rank under shared_assignment_histogram, then materialize the merge".to_string(),
        input: path_string(&args.input),
        output: path_string(&args.output),
        table_count: tables.len(),
        candidate_pair_count: pair_keys.len(),
        search_seconds: elapsed_seconds,
        selected_pair: SelectedPair {
            left_index: best.left_index,
            right_index: best.right_index,
            left_bits: left.bits.clone(),
            right_bits: right.bits.clone(),
            left_row_count: left.rows.len(),
            right_row_count: right.rows.len(),
            shared_bit_count: best.shared_count,
            shared_bits,
            union_arity: best.union_arity,
            predicted_rows: best.predicted_rows,
            predicted_rank: best.predicted_rank,
            actual_rows,
            actual_rank,
            prediction_matches_actual_rows: best.predicted_rows == actual_rows,
            merged_bits: merged.bits.clone(),
        },
    };

    write_json(&args.output, &vec![Table {
        bits: merged.bits,
        rows: merged.rows,
    }])?;
    write_json(&args.report, &report)?;

    println!("best pair: ({}, {})", best.left_index, best.right_index);
    println!("predicted rows: {}", best.predicted_rows);
    println!("predicted rank: {:.12}", best.predicted_rank);
    println!("actual rows: {}", actual_rows);
    println!("actual rank: {:.12}", actual_rank);
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

fn is_better_candidate(candidate: BestCandidate, current: BestCandidate) -> bool {
    candidate
        .predicted_rank
        .total_cmp(&current.predicted_rank)
        .is_lt()
        || (candidate.predicted_rank == current.predicted_rank
            && (candidate.predicted_rows, candidate.union_arity, candidate.left_index, candidate.right_index)
                < (current.predicted_rows, current.union_arity, current.left_index, current.right_index))
}

fn generate_shared_pairs(tables: &[Table]) -> Vec<u64> {
    let mut bit_to_tables: HashMap<u32, Vec<usize>> = HashMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }

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

fn shared_bits_and_union_arity(left_bits: &[u32], right_bits: &[u32]) -> (Vec<u32>, usize) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut shared_bits = Vec::new();

    while left_index < left_bits.len() && right_index < right_bits.len() {
        match left_bits[left_index].cmp(&right_bits[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared_bits.push(left_bits[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    let union_arity = left_bits.len() + right_bits.len() - shared_bits.len();
    (shared_bits, union_arity)
}

fn shared_bits_to_indices(
    left: &Table,
    right: &Table,
    shared_bits: &[u32],
) -> (Vec<u8>, Vec<u8>) {
    let left_indices = shared_bits
        .iter()
        .map(|bit| left.bits.binary_search(bit).unwrap() as u8)
        .collect();
    let right_indices = shared_bits
        .iter()
        .map(|bit| right.bits.binary_search(bit).unwrap() as u8)
        .collect();
    (left_indices, right_indices)
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
        "usage: cargo run --release --bin best_pair_merge -- --input <path> --output <path> --report <path>"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
