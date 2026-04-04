use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{
    arity_distribution, collect_bits, project_row, read_tables, sort_dedup_rows, total_rows,
    write_json, Table,
};
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Debug)]
struct Args {
    input: PathBuf,
    max_cluster_arity: usize,
    stop_before_bit: Option<u32>,
    output_tables: PathBuf,
    output_report: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = None;
        let mut max_cluster_arity = 16usize;
        let mut stop_before_bit = None;
        let mut output_tables = None;
        let mut output_report = None;
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--max-cluster-arity" => {
                    max_cluster_arity = expect_value(&mut iter, "--max-cluster-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-cluster-arity")?;
                }
                "--stop-before-bit" => {
                    stop_before_bit = Some(
                        expect_value(&mut iter, "--stop-before-bit")?
                            .parse()
                            .with_context(|| "invalid value for --stop-before-bit")?,
                    );
                }
                "--output-tables" => {
                    output_tables = Some(PathBuf::from(expect_value(&mut iter, "--output-tables")?))
                }
                "--output-report" => {
                    output_report = Some(PathBuf::from(expect_value(&mut iter, "--output-report")?))
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            input: input.context("missing --input")?,
            max_cluster_arity,
            stop_before_bit,
            output_tables: output_tables.context("missing --output-tables")?,
            output_report: output_report.context("missing --output-report")?,
        })
    }
}

#[derive(Clone, Debug)]
struct CandidateBit {
    bit: u32,
    source_table_count: usize,
    source_row_count: usize,
    source_union_arity: usize,
    source_merged_row_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct BitEliminationRecord {
    order: usize,
    central_bit: u32,
    source_table_count: usize,
    source_row_count: usize,
    source_union_arity: usize,
    source_merged_row_count: usize,
    current_table_count: usize,
    current_row_count: usize,
    current_union_arity: usize,
    current_merged_row_count: usize,
    removed_unsupported_rows: usize,
    projected_row_collapses: usize,
    changed_tables: usize,
    dropped_empty_tables: usize,
    skipped: bool,
    skip_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct EliminationReport {
    input: String,
    max_cluster_arity: usize,
    stop_before_bit: Option<u32>,
    initial_table_count: usize,
    initial_bit_count: usize,
    initial_row_count: usize,
    initial_arity_distribution: BTreeMap<String, usize>,
    qualifying_central_bit_count: usize,
    processed_central_bit_count: usize,
    skipped_central_bit_count: usize,
    final_table_count: usize,
    final_bit_count: usize,
    final_row_count: usize,
    final_arity_distribution: BTreeMap<String, usize>,
    remaining_qualifying_central_bits: usize,
    total_removed_unsupported_rows: usize,
    total_projected_row_collapses: usize,
    total_changed_tables: usize,
    total_dropped_empty_tables: usize,
    bits: Vec<BitEliminationRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let source_tables = read_tables(&args.input)?;
    let initial_table_count = source_tables.len();
    let initial_bit_count = collect_bits(&source_tables).len();
    let initial_row_count = total_rows(&source_tables);
    let initial_arity_distribution = arity_distribution(&source_tables);

    let candidates = find_central_bits(&source_tables, args.max_cluster_arity)?;
    let qualifying_central_bit_count = candidates.len();

    let mut tables: Vec<Option<Table>> = source_tables.into_iter().map(Some).collect();
    let mut bit_to_tables = build_bit_to_tables(&tables);

    let mut bits = Vec::with_capacity(candidates.len());
    let mut processed_central_bit_count = 0usize;
    let mut skipped_central_bit_count = 0usize;
    let mut total_removed_unsupported_rows = 0usize;
    let mut total_projected_row_collapses = 0usize;
    let mut total_changed_tables = 0usize;
    let mut total_dropped_empty_tables = 0usize;

    for (order, candidate) in candidates.into_iter().enumerate() {
        if args.stop_before_bit == Some(candidate.bit) {
            break;
        }

        let current_table_indices: Vec<usize> = bit_to_tables
            .get(&candidate.bit)
            .map(|indices| indices.iter().copied().collect())
            .unwrap_or_default();
        if current_table_indices.is_empty() {
            bits.push(BitEliminationRecord {
                order: order + 1,
                central_bit: candidate.bit,
                source_table_count: candidate.source_table_count,
                source_row_count: candidate.source_row_count,
                source_union_arity: candidate.source_union_arity,
                source_merged_row_count: candidate.source_merged_row_count,
                current_table_count: 0,
                current_row_count: 0,
                current_union_arity: 0,
                current_merged_row_count: 0,
                removed_unsupported_rows: 0,
                projected_row_collapses: 0,
                changed_tables: 0,
                dropped_empty_tables: 0,
                skipped: true,
                skip_reason: Some("bit no longer present in any table".to_string()),
            });
            skipped_central_bit_count += 1;
            continue;
        }

        let current_tables = collect_cluster_tables(&tables, &current_table_indices)?;
        let current_row_count: usize = current_tables.iter().map(|table| table.rows.len()).sum();
        let current_union_arity = collect_bits(&current_tables).len();
        if current_union_arity > args.max_cluster_arity {
            bits.push(BitEliminationRecord {
                order: order + 1,
                central_bit: candidate.bit,
                source_table_count: candidate.source_table_count,
                source_row_count: candidate.source_row_count,
                source_union_arity: candidate.source_union_arity,
                source_merged_row_count: candidate.source_merged_row_count,
                current_table_count: current_table_indices.len(),
                current_row_count,
                current_union_arity,
                current_merged_row_count: 0,
                removed_unsupported_rows: 0,
                projected_row_collapses: 0,
                changed_tables: 0,
                dropped_empty_tables: 0,
                skipped: true,
                skip_reason: Some(format!(
                    "current cluster arity {} exceeds {}",
                    current_union_arity, args.max_cluster_arity
                )),
            });
            skipped_central_bit_count += 1;
            continue;
        }

        let merged_cluster = merge_cluster_tables(&current_tables, candidate.bit)?;
        if merged_cluster.rows.is_empty() {
            bail!(
                "cluster for central bit {} became empty after sequential elimination",
                candidate.bit
            );
        }

        let mut removed_unsupported_rows = 0usize;
        let mut projected_row_collapses = 0usize;
        let mut changed_tables = 0usize;
        let mut dropped_empty_tables = 0usize;

        for &table_index in &current_table_indices {
            let Some(old_table) = tables[table_index].take() else {
                continue;
            };
            let supported_rows = supported_rows_from_cluster_merge(&old_table, &merged_cluster)?;
            removed_unsupported_rows += old_table.rows.len().saturating_sub(supported_rows.len());

            let (new_bits, new_rows) =
                project_table_without_bit(&old_table.bits, &supported_rows, candidate.bit)?;
            projected_row_collapses += supported_rows.len().saturating_sub(new_rows.len());

            for &bit in &old_table.bits {
                let should_remove_entry = if let Some(indices) = bit_to_tables.get_mut(&bit) {
                    indices.remove(&table_index);
                    indices.is_empty()
                } else {
                    false
                };
                if should_remove_entry {
                    bit_to_tables.remove(&bit);
                }
            }

            if new_bits.is_empty() {
                dropped_empty_tables += 1;
                continue;
            }

            if new_bits != old_table.bits || new_rows != old_table.rows {
                changed_tables += 1;
            }

            for &bit in &new_bits {
                bit_to_tables.entry(bit).or_default().insert(table_index);
            }
            tables[table_index] = Some(Table {
                bits: new_bits,
                rows: new_rows,
            });
        }

        processed_central_bit_count += 1;
        total_removed_unsupported_rows += removed_unsupported_rows;
        total_projected_row_collapses += projected_row_collapses;
        total_changed_tables += changed_tables;
        total_dropped_empty_tables += dropped_empty_tables;

        bits.push(BitEliminationRecord {
            order: order + 1,
            central_bit: candidate.bit,
            source_table_count: candidate.source_table_count,
            source_row_count: candidate.source_row_count,
            source_union_arity: candidate.source_union_arity,
            source_merged_row_count: candidate.source_merged_row_count,
            current_table_count: current_table_indices.len(),
            current_row_count,
            current_union_arity,
            current_merged_row_count: merged_cluster.rows.len(),
            removed_unsupported_rows,
            projected_row_collapses,
            changed_tables,
            dropped_empty_tables,
            skipped: false,
            skip_reason: None,
        });
    }

    let final_tables: Vec<Table> = tables.into_iter().flatten().collect();
    let final_bits = collect_bits(&final_tables);
    let final_rows = total_rows(&final_tables);
    let candidate_bit_set: BTreeSet<u32> = bits.iter().map(|record| record.central_bit).collect();
    let remaining_qualifying_central_bits = final_bits
        .iter()
        .filter(|bit| candidate_bit_set.contains(bit))
        .count();

    let report = EliminationReport {
        input: args.input.display().to_string(),
        max_cluster_arity: args.max_cluster_arity,
        stop_before_bit: args.stop_before_bit,
        initial_table_count,
        initial_bit_count,
        initial_row_count,
        initial_arity_distribution,
        qualifying_central_bit_count,
        processed_central_bit_count,
        skipped_central_bit_count,
        final_table_count: final_tables.len(),
        final_bit_count: final_bits.len(),
        final_row_count: final_rows,
        final_arity_distribution: arity_distribution(&final_tables),
        remaining_qualifying_central_bits,
        total_removed_unsupported_rows,
        total_projected_row_collapses,
        total_changed_tables,
        total_dropped_empty_tables,
        bits,
    };

    write_json(&args.output_tables, &final_tables)?;
    write_json(&args.output_report, &report)?;

    println!(
        "qualifying_central_bits={}",
        report.qualifying_central_bit_count
    );
    println!(
        "processed_central_bits={}",
        report.processed_central_bit_count
    );
    println!("skipped_central_bits={}", report.skipped_central_bit_count);
    println!("final_tables={}", report.final_table_count);
    println!("final_bits={}", report.final_bit_count);
    println!("final_rows={}", report.final_row_count);
    println!(
        "remaining_qualifying_central_bits={}",
        report.remaining_qualifying_central_bits
    );
    if let Some(bit) = report.stop_before_bit {
        println!("stopped_before_bit={bit}");
    }
    println!("output_tables={}", args.output_tables.display());
    println!("output_report={}", args.output_report.display());
    Ok(())
}

fn find_central_bits(tables: &[Table], max_cluster_arity: usize) -> Result<Vec<CandidateBit>> {
    let mut bit_to_table_indices: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_table_indices
                .entry(bit)
                .or_default()
                .push(table_index);
        }
    }

    let mut candidates = Vec::new();
    for (bit, table_indices) in bit_to_table_indices {
        let cluster_tables: Vec<Table> = table_indices
            .iter()
            .map(|&table_index| tables[table_index].clone())
            .collect();
        let union_arity = collect_bits(&cluster_tables).len();
        if union_arity > max_cluster_arity {
            continue;
        }

        let merged_cluster = merge_cluster_tables(&cluster_tables, bit)?;
        if merged_cluster.rows.is_empty() {
            continue;
        }

        candidates.push(CandidateBit {
            bit,
            source_table_count: cluster_tables.len(),
            source_row_count: cluster_tables.iter().map(|table| table.rows.len()).sum(),
            source_union_arity: union_arity,
            source_merged_row_count: merged_cluster.rows.len(),
        });
    }

    candidates.sort_by(|left, right| {
        right
            .source_row_count
            .cmp(&left.source_row_count)
            .then_with(|| right.source_table_count.cmp(&left.source_table_count))
            .then_with(|| left.bit.cmp(&right.bit))
    });
    Ok(candidates)
}

fn collect_cluster_tables(tables: &[Option<Table>], table_indices: &[usize]) -> Result<Vec<Table>> {
    table_indices
        .iter()
        .map(|&table_index| {
            tables[table_index]
                .clone()
                .with_context(|| format!("missing table {table_index} in cluster"))
        })
        .collect()
}

fn build_bit_to_tables(tables: &[Option<Table>]) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut bit_to_tables: BTreeMap<u32, BTreeSet<usize>> = BTreeMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        let Some(table) = table else {
            continue;
        };
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().insert(table_index);
        }
    }
    bit_to_tables
}

fn merge_cluster_tables(cluster_tables: &[Table], central_bit: u32) -> Result<Table> {
    let Some(first_table) = cluster_tables.first() else {
        bail!("central bit {central_bit} has empty cluster");
    };

    let mut merged_bits = first_table.bits.clone();
    let mut merged_rows = first_table.rows.clone();
    for table in &cluster_tables[1..] {
        let merged =
            merge_tables_fast_from_slices(&merged_bits, &merged_rows, &table.bits, &table.rows)
                .map_err(|error| {
                    anyhow!("failed to merge cluster for central bit {central_bit}: {error}")
                })?;
        merged_bits = merged.bits;
        merged_rows = merged.rows;
        if merged_rows.is_empty() {
            break;
        }
    }

    Ok(Table {
        bits: merged_bits,
        rows: merged_rows,
    })
}

fn supported_rows_from_cluster_merge(table: &Table, merged_cluster: &Table) -> Result<Vec<u32>> {
    let merged_indices: Result<Vec<usize>> = table
        .bits
        .iter()
        .map(|bit| {
            merged_cluster
                .bits
                .binary_search(bit)
                .map_err(|_| anyhow!("merged cluster lost bit {bit}"))
        })
        .collect();
    let merged_indices = merged_indices?;

    let mut rows: Vec<u32> = merged_cluster
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &merged_indices))
        .collect();
    sort_dedup_rows(&mut rows);
    Ok(rows)
}

fn project_table_without_bit(
    bits: &[u32],
    rows: &[u32],
    central_bit: u32,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let mut projected_bits = Vec::with_capacity(bits.len().saturating_sub(1));
    let mut projected_indices = Vec::with_capacity(bits.len().saturating_sub(1));

    for (index, &bit) in bits.iter().enumerate() {
        if bit == central_bit {
            continue;
        }
        projected_bits.push(bit);
        projected_indices.push(index);
    }

    if projected_bits.len() + 1 != bits.len() {
        bail!("central bit {central_bit} missing from table {:?}", bits);
    }

    let mut projected_rows: Vec<u32> = rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);
    Ok((projected_bits, projected_rows))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --example eliminate_central_bits_by_cluster_consistency -- --input <tables.json> [--max-cluster-arity <n>] [--stop-before-bit <bit>] --output-tables <tables.json> --output-report <report.json>"
    );
}
