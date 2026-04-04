use std::collections::BTreeSet;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{
    collect_bits, is_full_row_set, project_row, read_tables, sort_dedup_rows, total_rows,
    write_json, Table,
};
use tables::rank_stats::compute_rank;
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    central_bit: u32,
    output_tables: PathBuf,
    output_derived: PathBuf,
    output_report: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = PathBuf::from("data/raw/tables.json");
        let mut central_bit = None;
        let mut output_tables = PathBuf::from(
            "runs/2026-04-04-central-bit-step/tables.after_step.json",
        );
        let mut output_derived = PathBuf::from(
            "runs/2026-04-04-central-bit-step/derived.after_subset_merge.json",
        );
        let mut output_report =
            PathBuf::from("runs/2026-04-04-central-bit-step/report.json");
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = PathBuf::from(expect_value(&mut iter, "--input")?),
                "--central-bit" => {
                    central_bit = Some(
                        expect_value(&mut iter, "--central-bit")?
                            .parse()
                            .with_context(|| "invalid value for --central-bit")?,
                    )
                }
                "--output-tables" => {
                    output_tables = PathBuf::from(expect_value(&mut iter, "--output-tables")?)
                }
                "--output-derived" => {
                    output_derived = PathBuf::from(expect_value(&mut iter, "--output-derived")?)
                }
                "--output-report" => {
                    output_report = PathBuf::from(expect_value(&mut iter, "--output-report")?)
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            input,
            central_bit: central_bit.context("missing --central-bit")?,
            output_tables,
            output_derived,
            output_report,
        })
    }
}

#[derive(Serialize)]
struct Report {
    method: String,
    input: String,
    central_bit: u32,
    incident_table_ids: Vec<usize>,
    incident_table_count: usize,
    incident_row_count: usize,
    incident_bit_count: usize,
    main_before_drop_bit_count: usize,
    main_before_drop_row_count: usize,
    after_drop_bit_count: usize,
    after_drop_row_count: usize,
    after_drop_is_tautology: bool,
    subset_table_ids: Vec<usize>,
    subset_table_count: usize,
    subset_row_count: usize,
    final_derived_bit_count: usize,
    final_derived_row_count: usize,
    final_derived_rank: f64,
    final_derived_is_tautology: bool,
    removed_table_count: usize,
    removed_row_count: usize,
    final_system_table_count: usize,
    final_system_bit_count: usize,
    final_system_row_count: usize,
    output_tables: String,
    output_derived: String,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let raw_tables = read_tables(&args.input)?;
    let canonical_tables: Vec<Table> = raw_tables
        .into_iter()
        .map(|table| {
            let (bits, rows) = canonicalize_table(&table);
            Table { bits, rows }
        })
        .collect();

    let incident_table_ids = incident_table_ids(&canonical_tables, args.central_bit);
    if incident_table_ids.is_empty() {
        bail!("no tables contain central bit {}", args.central_bit);
    }

    let incident_tables = collect_tables(&canonical_tables, &incident_table_ids)?;
    let incident_row_count: usize = incident_tables.iter().map(|table| table.rows.len()).sum();
    let incident_bit_count = collect_bits(&incident_tables).len();

    let main = merge_all_tables(&incident_tables, args.central_bit)?;
    let after_drop = drop_central_bit(main.clone(), args.central_bit)?;
    let subset_table_ids = find_subset_table_ids(&after_drop.bits, &canonical_tables, &incident_table_ids);
    let subset_tables = collect_tables(&canonical_tables, &subset_table_ids)?;
    let subset_row_count: usize = subset_tables.iter().map(|table| table.rows.len()).sum();

    let mut final_derived = after_drop.clone();
    for subset_table in &subset_tables {
        final_derived = merge_two_tables(&final_derived, subset_table, args.central_bit)?;
    }

    let removed_table_ids: BTreeSet<usize> = incident_table_ids
        .iter()
        .chain(subset_table_ids.iter())
        .copied()
        .collect();
    let removed_row_count: usize = removed_table_ids
        .iter()
        .map(|&table_id| canonical_tables[table_id].rows.len())
        .sum();

    let mut remaining_tables: Vec<Table> = canonical_tables
        .iter()
        .enumerate()
        .filter(|(table_id, _)| !removed_table_ids.contains(table_id))
        .map(|(_, table)| table.clone())
        .collect();
    remaining_tables.push(final_derived.clone());
    let final_system = remaining_tables;

    let report = Report {
        method: "Merge all incident tables for a chosen central bit, remove those source tables, drop the central bit from the merged table, merge every remaining strict-subset table into that derived table, remove those merged subset tables, and add the final derived table back into the system".to_string(),
        input: args.input.display().to_string(),
        central_bit: args.central_bit,
        incident_table_ids: incident_table_ids.clone(),
        incident_table_count: incident_table_ids.len(),
        incident_row_count,
        incident_bit_count,
        main_before_drop_bit_count: main.bits.len(),
        main_before_drop_row_count: main.rows.len(),
        after_drop_bit_count: after_drop.bits.len(),
        after_drop_row_count: after_drop.rows.len(),
        after_drop_is_tautology: is_full_row_set(after_drop.rows.len(), after_drop.bits.len()),
        subset_table_ids: subset_table_ids.clone(),
        subset_table_count: subset_table_ids.len(),
        subset_row_count,
        final_derived_bit_count: final_derived.bits.len(),
        final_derived_row_count: final_derived.rows.len(),
        final_derived_rank: compute_rank(final_derived.rows.len(), final_derived.bits.len()),
        final_derived_is_tautology: is_full_row_set(final_derived.rows.len(), final_derived.bits.len()),
        removed_table_count: removed_table_ids.len(),
        removed_row_count,
        final_system_table_count: final_system.len(),
        final_system_bit_count: collect_bits(&final_system).len(),
        final_system_row_count: total_rows(&final_system),
        output_tables: args.output_tables.display().to_string(),
        output_derived: args.output_derived.display().to_string(),
    };

    write_json(&args.output_tables, &final_system)?;
    write_json(&args.output_derived, &vec![final_derived])?;
    write_json(&args.output_report, &report)?;

    println!("central_bit={}", report.central_bit);
    println!("incident_tables={}", report.incident_table_count);
    println!("after_drop_bits={}", report.after_drop_bit_count);
    println!("after_drop_rows={}", report.after_drop_row_count);
    println!("subset_tables={}", report.subset_table_count);
    println!("final_derived_bits={}", report.final_derived_bit_count);
    println!("final_derived_rows={}", report.final_derived_row_count);
    println!("final_system_tables={}", report.final_system_table_count);
    println!("final_system_bits={}", report.final_system_bit_count);
    println!("final_system_rows={}", report.final_system_row_count);
    println!("output_tables={}", args.output_tables.display());
    println!("output_derived={}", args.output_derived.display());
    println!("output_report={}", args.output_report.display());

    Ok(())
}

fn incident_table_ids(tables: &[Table], central_bit: u32) -> Vec<usize> {
    tables
        .iter()
        .enumerate()
        .filter_map(|(table_id, table)| table.bits.binary_search(&central_bit).ok().map(|_| table_id))
        .collect()
}

fn collect_tables(tables: &[Table], table_ids: &[usize]) -> Result<Vec<Table>> {
    table_ids
        .iter()
        .map(|&table_id| {
            tables
                .get(table_id)
                .cloned()
                .with_context(|| format!("missing table {table_id}"))
        })
        .collect()
}

fn merge_all_tables(tables: &[Table], central_bit: u32) -> Result<Table> {
    let Some(first) = tables.first() else {
        bail!("no incident tables found for central bit {central_bit}");
    };

    let mut merged = first.clone();
    for table in &tables[1..] {
        merged = merge_two_tables(&merged, table, central_bit)?;
    }
    Ok(merged)
}

fn merge_two_tables(left: &Table, right: &Table, central_bit: u32) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!("failed to merge tables for central bit {central_bit}: {error}"))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn drop_central_bit(table: Table, central_bit: u32) -> Result<Table> {
    let mut projected_bits = Vec::with_capacity(table.bits.len().saturating_sub(1));
    let mut projected_indices = Vec::with_capacity(table.bits.len().saturating_sub(1));

    for (index, &bit) in table.bits.iter().enumerate() {
        if bit == central_bit {
            continue;
        }
        projected_bits.push(bit);
        projected_indices.push(index);
    }

    if projected_bits.len() + 1 != table.bits.len() {
        bail!(
            "central bit {central_bit} missing from merged table {:?}",
            table.bits
        );
    }

    let mut projected_rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    Ok(Table {
        bits: projected_bits,
        rows: projected_rows,
    })
}

fn find_subset_table_ids(
    derived_bits: &[u32],
    tables: &[Table],
    excluded_ids: &[usize],
) -> Vec<usize> {
    let excluded: BTreeSet<usize> = excluded_ids.iter().copied().collect();
    let mut subset_ids = Vec::new();

    for (table_id, table) in tables.iter().enumerate() {
        if excluded.contains(&table_id) {
            continue;
        }
        if is_strict_subset_bits(&table.bits, derived_bits) {
            subset_ids.push(table_id);
        }
    }

    subset_ids.sort_by(|left, right| {
        let left_bits = &tables[*left].bits;
        let right_bits = &tables[*right].bits;
        left_bits
            .len()
            .cmp(&right_bits.len())
            .then_with(|| left_bits.cmp(right_bits))
            .then_with(|| left.cmp(right))
    });
    subset_ids
}

fn is_strict_subset_bits(subset: &[u32], superset: &[u32]) -> bool {
    if subset.len() >= superset.len() {
        return false;
    }

    let mut subset_index = 0usize;
    let mut superset_index = 0usize;
    while subset_index < subset.len() && superset_index < superset.len() {
        match subset[subset_index].cmp(&superset[superset_index]) {
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Greater => superset_index += 1,
            std::cmp::Ordering::Equal => {
                subset_index += 1;
                superset_index += 1;
            }
        }
    }

    subset_index == subset.len()
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --bin apply_central_bit_step -- --central-bit <bit> [--input <path>] [--output-tables <path>] [--output-derived <path>] [--output-report <path>]"
    );
}
