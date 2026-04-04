use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{project_row, read_tables, sort_dedup_rows, write_json, Table};
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Debug)]
struct Args {
    input: PathBuf,
    central_bit: u32,
    output: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = None;
        let mut central_bit = None;
        let mut output = None;
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--central-bit" => {
                    central_bit = Some(
                        expect_value(&mut iter, "--central-bit")?
                            .parse()
                            .with_context(|| "invalid value for --central-bit")?,
                    )
                }
                "--output" => output = Some(PathBuf::from(expect_value(&mut iter, "--output")?)),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            input: input.context("missing --input")?,
            central_bit: central_bit.context("missing --central-bit")?,
            output: output.context("missing --output")?,
        })
    }
}

#[derive(Clone)]
struct ClusterTable {
    source_index: usize,
    table: Table,
}

#[derive(Clone)]
struct Compatibility {
    neighbors: Vec<Vec<Vec<usize>>>,
}

#[derive(Debug, Serialize)]
struct TableReport {
    cluster_table_index: usize,
    source_table_index: usize,
    bit_count: usize,
    row_count_before: usize,
    row_count_after_explicit: usize,
    row_count_after_merge_projection: usize,
    rows_removed_by_explicit: usize,
    explicit_rows: Vec<u32>,
    merge_projection_rows: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct IterationReport {
    iteration: usize,
    removed_by_rule_31: usize,
    removed_by_rule_32: usize,
    remaining_rows: usize,
}

#[derive(Debug, Serialize)]
struct ClusterReport {
    input: String,
    central_bit: u32,
    cluster_table_count: usize,
    cluster_union_arity: usize,
    total_rows_before: usize,
    total_rows_after_explicit: usize,
    total_rows_after_merge_projection: usize,
    clique_count: usize,
    explicit_matches_merge_shortcut: bool,
    iterations: Vec<IterationReport>,
    tables: Vec<TableReport>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)?;
    let cluster = collect_cluster(&tables, args.central_bit)?;
    let compatibility = build_compatibility(&cluster);

    let (explicit_alive, iterations, clique_count) =
        explicit_cluster_fixed_point(&cluster, &compatibility);
    let merge_cluster = merge_cluster_tables(&cluster, args.central_bit)?;
    let merge_supported = merge_supported_rows(&cluster, &merge_cluster)?;

    let cluster_union_arity = collect_union_arity(&cluster);
    let total_rows_before: usize = cluster.iter().map(|table| table.table.rows.len()).sum();
    let mut total_rows_after_explicit = 0usize;
    let mut total_rows_after_merge_projection = 0usize;
    let mut explicit_matches_merge_shortcut = true;
    let mut tables_report = Vec::with_capacity(cluster.len());

    for (table_index, cluster_table) in cluster.iter().enumerate() {
        let explicit_rows =
            collect_alive_rows(&cluster_table.table.rows, &explicit_alive[table_index]);
        let merge_rows = merge_supported[table_index].clone();
        if explicit_rows != merge_rows {
            explicit_matches_merge_shortcut = false;
        }

        total_rows_after_explicit += explicit_rows.len();
        total_rows_after_merge_projection += merge_rows.len();

        tables_report.push(TableReport {
            cluster_table_index: table_index,
            source_table_index: cluster_table.source_index,
            bit_count: cluster_table.table.bits.len(),
            row_count_before: cluster_table.table.rows.len(),
            row_count_after_explicit: explicit_rows.len(),
            row_count_after_merge_projection: merge_rows.len(),
            rows_removed_by_explicit: cluster_table
                .table
                .rows
                .len()
                .saturating_sub(explicit_rows.len()),
            explicit_rows,
            merge_projection_rows: merge_rows,
        });
    }

    let report = ClusterReport {
        input: args.input.display().to_string(),
        central_bit: args.central_bit,
        cluster_table_count: cluster.len(),
        cluster_union_arity,
        total_rows_before,
        total_rows_after_explicit,
        total_rows_after_merge_projection,
        clique_count,
        explicit_matches_merge_shortcut,
        iterations,
        tables: tables_report,
    };

    write_json(&args.output, &report)?;
    println!("central_bit={}", report.central_bit);
    println!("cluster_tables={}", report.cluster_table_count);
    println!("cluster_union_arity={}", report.cluster_union_arity);
    println!("rows_before={}", report.total_rows_before);
    println!("rows_after_explicit={}", report.total_rows_after_explicit);
    println!(
        "rows_after_merge_projection={}",
        report.total_rows_after_merge_projection
    );
    println!("clique_count={}", report.clique_count);
    println!(
        "explicit_matches_merge_shortcut={}",
        report.explicit_matches_merge_shortcut
    );
    println!("output={}", args.output.display());
    Ok(())
}

fn collect_cluster(tables: &[Table], central_bit: u32) -> Result<Vec<ClusterTable>> {
    let cluster: Vec<ClusterTable> = tables
        .iter()
        .enumerate()
        .filter(|(_, table)| table.bits.binary_search(&central_bit).is_ok())
        .map(|(source_index, table)| ClusterTable {
            source_index,
            table: table.clone(),
        })
        .collect();
    if cluster.is_empty() {
        bail!("central bit {central_bit} is absent from the input");
    }
    Ok(cluster)
}

fn build_compatibility(cluster: &[ClusterTable]) -> Compatibility {
    let table_count = cluster.len();
    let mut neighbors = vec![vec![Vec::<usize>::new(); 0]; table_count];
    for left_index in 0..table_count {
        neighbors[left_index] = (0..cluster[left_index].table.rows.len())
            .map(|_| vec![usize::MAX; table_count])
            .collect::<Vec<_>>();
    }

    let mut adjacency: Vec<Vec<Vec<usize>>> = cluster
        .iter()
        .map(|cluster_table| vec![Vec::new(); cluster_table.table.rows.len()])
        .collect();
    let mut reverse: Vec<Vec<Vec<usize>>> = cluster
        .iter()
        .map(|cluster_table| vec![Vec::new(); cluster_table.table.rows.len()])
        .collect();

    for left_index in 0..table_count {
        for right_index in (left_index + 1)..table_count {
            let pair = compatible_rows(&cluster[left_index].table, &cluster[right_index].table);
            for (left_row, right_rows) in pair.left_to_right.into_iter().enumerate() {
                adjacency[left_index][left_row].extend(
                    right_rows
                        .into_iter()
                        .map(|right_row| encode_neighbor(right_index, right_row)),
                );
            }
            for (right_row, left_rows) in pair.right_to_left.into_iter().enumerate() {
                reverse[right_index][right_row].extend(
                    left_rows
                        .into_iter()
                        .map(|left_row| encode_neighbor(left_index, left_row)),
                );
            }
        }
    }

    for table_index in 0..table_count {
        for row_index in 0..cluster[table_index].table.rows.len() {
            adjacency[table_index][row_index].append(&mut reverse[table_index][row_index]);
            adjacency[table_index][row_index].sort_unstable();
        }
    }

    Compatibility {
        neighbors: adjacency,
    }
}

struct PairCompatibility {
    left_to_right: Vec<Vec<usize>>,
    right_to_left: Vec<Vec<usize>>,
}

fn compatible_rows(left: &Table, right: &Table) -> PairCompatibility {
    let shared_bits = shared_bits(&left.bits, &right.bits);
    let left_shared_indices = indices_for_subset(&left.bits, &shared_bits);
    let right_shared_indices = indices_for_subset(&right.bits, &shared_bits);

    let mut left_to_right = vec![Vec::new(); left.rows.len()];
    let mut right_to_left = vec![Vec::new(); right.rows.len()];
    for (left_row_index, &left_row) in left.rows.iter().enumerate() {
        let left_projection = project_row(left_row, &left_shared_indices);
        for (right_row_index, &right_row) in right.rows.iter().enumerate() {
            let right_projection = project_row(right_row, &right_shared_indices);
            if left_projection == right_projection {
                left_to_right[left_row_index].push(right_row_index);
                right_to_left[right_row_index].push(left_row_index);
            }
        }
    }

    PairCompatibility {
        left_to_right,
        right_to_left,
    }
}

fn explicit_cluster_fixed_point(
    cluster: &[ClusterTable],
    compatibility: &Compatibility,
) -> (Vec<Vec<bool>>, Vec<IterationReport>, usize) {
    let mut alive: Vec<Vec<bool>> = cluster
        .iter()
        .map(|cluster_table| vec![true; cluster_table.table.rows.len()])
        .collect();
    let mut iterations = Vec::new();
    let mut clique_count = 0usize;
    let mut iteration_index = 1usize;

    loop {
        let removed_by_rule_31 = apply_rule_31(&mut alive, compatibility);
        let (participating, found_cliques) = rows_in_full_cliques(&alive, compatibility);
        clique_count = found_cliques;
        let removed_by_rule_32 = apply_rule_32(&mut alive, &participating);
        let remaining_rows = alive
            .iter()
            .map(|table_alive| table_alive.iter().filter(|&&flag| flag).count())
            .sum();
        iterations.push(IterationReport {
            iteration: iteration_index,
            removed_by_rule_31,
            removed_by_rule_32,
            remaining_rows,
        });

        if removed_by_rule_31 == 0 && removed_by_rule_32 == 0 {
            break;
        }
        iteration_index += 1;
    }

    (alive, iterations, clique_count)
}

fn apply_rule_31(alive: &mut [Vec<bool>], compatibility: &Compatibility) -> usize {
    let table_count = alive.len();
    let mut remove = Vec::new();

    for table_index in 0..table_count {
        for row_index in 0..alive[table_index].len() {
            if !alive[table_index][row_index] {
                continue;
            }

            let mut seen_tables = BTreeSet::new();
            for &encoded in &compatibility.neighbors[table_index][row_index] {
                let (other_table, other_row) = decode_neighbor(encoded);
                if alive[other_table][other_row] {
                    seen_tables.insert(other_table);
                }
            }
            if seen_tables.len() + 1 != table_count {
                remove.push((table_index, row_index));
            }
        }
    }

    for (table_index, row_index) in &remove {
        alive[*table_index][*row_index] = false;
    }
    remove.len()
}

fn rows_in_full_cliques(
    alive: &[Vec<bool>],
    compatibility: &Compatibility,
) -> (Vec<Vec<bool>>, usize) {
    let table_order = table_order_by_alive_rows(alive);
    let mut participating: Vec<Vec<bool>> = alive
        .iter()
        .map(|table_alive| vec![false; table_alive.len()])
        .collect();
    let mut selected = vec![None; alive.len()];
    let mut clique_count = 0usize;

    search_cliques(
        0,
        &table_order,
        alive,
        compatibility,
        &mut selected,
        &mut participating,
        &mut clique_count,
    );

    (participating, clique_count)
}

fn search_cliques(
    depth: usize,
    table_order: &[usize],
    alive: &[Vec<bool>],
    compatibility: &Compatibility,
    selected: &mut [Option<usize>],
    participating: &mut [Vec<bool>],
    clique_count: &mut usize,
) {
    if depth == table_order.len() {
        *clique_count += 1;
        for (table_index, maybe_row_index) in selected.iter().enumerate() {
            if let Some(row_index) = maybe_row_index {
                participating[table_index][*row_index] = true;
            }
        }
        return;
    }

    let table_index = table_order[depth];
    let candidates = candidate_rows_for_table(table_index, alive, compatibility, selected);
    if candidates.is_empty() {
        return;
    }

    for row_index in candidates {
        selected[table_index] = Some(row_index);
        search_cliques(
            depth + 1,
            table_order,
            alive,
            compatibility,
            selected,
            participating,
            clique_count,
        );
        selected[table_index] = None;
    }
}

fn candidate_rows_for_table(
    table_index: usize,
    alive: &[Vec<bool>],
    compatibility: &Compatibility,
    selected: &[Option<usize>],
) -> Vec<usize> {
    let mut candidates = Vec::new();
    'row: for row_index in 0..alive[table_index].len() {
        if !alive[table_index][row_index] {
            continue;
        }
        for (other_table, maybe_other_row) in selected.iter().enumerate() {
            let Some(other_row) = maybe_other_row else {
                continue;
            };
            if !rows_compatible(
                table_index,
                row_index,
                other_table,
                *other_row,
                compatibility,
            ) {
                continue 'row;
            }
        }
        candidates.push(row_index);
    }
    candidates
}

fn rows_compatible(
    left_table: usize,
    left_row: usize,
    right_table: usize,
    right_row: usize,
    compatibility: &Compatibility,
) -> bool {
    let needle = encode_neighbor(right_table, right_row);
    compatibility.neighbors[left_table][left_row]
        .binary_search(&needle)
        .is_ok()
}

fn apply_rule_32(alive: &mut [Vec<bool>], participating: &[Vec<bool>]) -> usize {
    let mut removed = 0usize;
    for table_index in 0..alive.len() {
        for row_index in 0..alive[table_index].len() {
            if alive[table_index][row_index] && !participating[table_index][row_index] {
                alive[table_index][row_index] = false;
                removed += 1;
            }
        }
    }
    removed
}

fn table_order_by_alive_rows(alive: &[Vec<bool>]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..alive.len()).collect();
    order.sort_by_key(|&table_index| alive[table_index].iter().filter(|&&flag| flag).count());
    order
}

fn merge_cluster_tables(cluster: &[ClusterTable], central_bit: u32) -> Result<Table> {
    let Some(first_table) = cluster.first() else {
        bail!("central bit {central_bit} has empty cluster");
    };

    let mut merged_bits = first_table.table.bits.clone();
    let mut merged_rows = first_table.table.rows.clone();
    for cluster_table in &cluster[1..] {
        let merged = merge_tables_fast_from_slices(
            &merged_bits,
            &merged_rows,
            &cluster_table.table.bits,
            &cluster_table.table.rows,
        )
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

fn merge_supported_rows(cluster: &[ClusterTable], merged_cluster: &Table) -> Result<Vec<Vec<u32>>> {
    cluster
        .iter()
        .map(|cluster_table| {
            let merged_indices: Result<Vec<usize>> = cluster_table
                .table
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
        })
        .collect()
}

fn collect_alive_rows(rows: &[u32], alive: &[bool]) -> Vec<u32> {
    rows.iter()
        .zip(alive.iter())
        .filter_map(|(&row, &flag)| flag.then_some(row))
        .collect()
}

fn collect_union_arity(cluster: &[ClusterTable]) -> usize {
    let mut bits = Vec::new();
    for cluster_table in cluster {
        bits.extend_from_slice(&cluster_table.table.bits);
    }
    bits.sort_unstable();
    bits.dedup();
    bits.len()
}

fn shared_bits(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut shared = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    shared
}

fn indices_for_subset(bits: &[u32], subset: &[u32]) -> Vec<usize> {
    subset
        .iter()
        .map(|bit| bits.binary_search(bit).unwrap())
        .collect()
}

fn encode_neighbor(table_index: usize, row_index: usize) -> usize {
    (table_index << 20) | row_index
}

fn decode_neighbor(encoded: usize) -> (usize, usize) {
    (encoded >> 20, encoded & ((1usize << 20) - 1))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --example check_single_central_bit_cluster -- --input <tables.json> --central-bit <bit> --output <report.json>"
    );
}
