use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::write_json;
use tables::tables_file::{read_tables_bundle, write_tables_bundle, OriginArray, TablesBundle};

struct Args {
    input: PathBuf,
    bits_json: PathBuf,
    output: PathBuf,
    report: PathBuf,
    origin_name: String,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/system.after_pipeline.tables",
            ),
            bits_json: PathBuf::from("data/raw/origins.json"),
            output: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/tables.with_any_origin.tables",
            ),
            report: PathBuf::from(
                "runs/2026-04-11-final-path-paths-originals-pipeline/tables.with_any_origin.report.json",
            ),
            origin_name: "origins".to_string(),
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
                "--bits-json" => {
                    args.bits_json = PathBuf::from(expect_value(&mut iter, "--bits-json")?)
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
                "--origin-name" => {
                    args.origin_name = expect_value(&mut iter, "--origin-name")?;
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

#[derive(Serialize)]
struct Report {
    input: String,
    bits_json: String,
    output: String,
    selected_bit_count: usize,
    input_table_count: usize,
    input_row_count: usize,
    matched_table_count: usize,
    matched_row_count: usize,
    matched_unique_bit_count: usize,
    matched_origin_bit_count: usize,
    matched_origin_bits: Vec<u32>,
    origin_bit_hit_counts: BTreeMap<u32, usize>,
    arity_distribution: BTreeMap<usize, usize>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let selected_bits = read_bits_json(&args.bits_json)?;
    let selected_set: BTreeSet<u32> = selected_bits.iter().copied().collect();

    let input_bundle = read_tables_bundle(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;

    let input_table_count = input_bundle.tables.len();
    let input_row_count: usize = input_bundle
        .tables
        .iter()
        .map(|table| table.row_count())
        .sum();

    let mut output_tables = Vec::new();
    let mut matched_bits = BTreeSet::new();
    let mut matched_origin_bits = BTreeSet::new();
    let mut origin_bit_hit_counts: BTreeMap<u32, usize> = BTreeMap::new();
    let mut arity_distribution: BTreeMap<usize, usize> = BTreeMap::new();
    let mut matched_row_count = 0usize;

    for table in input_bundle.tables {
        let hits: Vec<u32> = table
            .bits
            .iter()
            .copied()
            .filter(|bit| selected_set.contains(bit))
            .collect();
        if hits.is_empty() {
            continue;
        }

        for bit in &table.bits {
            matched_bits.insert(*bit);
        }
        for bit in hits {
            matched_origin_bits.insert(bit);
            *origin_bit_hit_counts.entry(bit).or_insert(0) += 1;
        }
        *arity_distribution.entry(table.bits.len()).or_insert(0) += 1;
        matched_row_count += table.row_count();
        output_tables.push(table);
    }

    let origin_arrays = upsert_origin_array(
        input_bundle.origin_arrays,
        &args.origin_name,
        &selected_bits,
    );

    let output_bundle = TablesBundle {
        origin_arrays,
        tables: output_tables,
    };
    write_tables_bundle(&args.output, &output_bundle)
        .with_context(|| format!("failed to write {}", args.output.display()))?;

    let report = Report {
        input: args.input.display().to_string(),
        bits_json: args.bits_json.display().to_string(),
        output: args.output.display().to_string(),
        selected_bit_count: selected_bits.len(),
        input_table_count,
        input_row_count,
        matched_table_count: output_bundle.tables.len(),
        matched_row_count,
        matched_unique_bit_count: matched_bits.len(),
        matched_origin_bit_count: matched_origin_bits.len(),
        matched_origin_bits: matched_origin_bits.into_iter().collect(),
        origin_bit_hit_counts,
        arity_distribution,
    };
    write_json(&args.report, &report)?;

    println!("input_tables: {}", report.input_table_count);
    println!("matched_tables: {}", report.matched_table_count);
    println!("matched_rows: {}", report.matched_row_count);
    println!("matched_origin_bits: {}", report.matched_origin_bit_count);
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());
    Ok(())
}

fn read_bits_json(path: &Path) -> Result<Vec<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

fn upsert_origin_array(
    mut origin_arrays: Vec<OriginArray>,
    origin_name: &str,
    selected_bits: &[u32],
) -> Vec<OriginArray> {
    let mut replaced = false;
    for origin_array in &mut origin_arrays {
        if origin_array.name == origin_name {
            origin_array.values = selected_bits.to_vec();
            replaced = true;
            break;
        }
    }

    if !replaced {
        origin_arrays.push(OriginArray {
            name: origin_name.to_string(),
            values: selected_bits.to_vec(),
        });
        origin_arrays.sort_by(|left, right| left.name.cmp(&right.name));
    }

    origin_arrays
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin filter_tables_by_any_bits -- [--input <system.tables>] [--bits-json <bits.json>] [--output <out.tables>] [--report <report.json>] [--origin-name <name>]"
    );
}

#[cfg(test)]
mod tests {
    use super::upsert_origin_array;
    use tables::tables_file::OriginArray;

    #[test]
    fn upsert_origin_array_replaces_named_array() {
        let updated = upsert_origin_array(
            vec![
                OriginArray {
                    name: "origins".to_string(),
                    values: vec![1, 2],
                },
                OriginArray {
                    name: "other".to_string(),
                    values: vec![9],
                },
            ],
            "origins",
            &[3, 4],
        );

        assert_eq!(updated.len(), 2);
        assert_eq!(updated[0].name, "origins");
        assert_eq!(updated[0].values, vec![3, 4]);
        assert_eq!(updated[1].name, "other");
        assert_eq!(updated[1].values, vec![9]);
    }

    #[test]
    fn upsert_origin_array_adds_named_array_when_missing() {
        let updated = upsert_origin_array(
            vec![OriginArray {
                name: "other".to_string(),
                values: vec![9],
            }],
            "origins",
            &[3, 4],
        );

        assert_eq!(updated.len(), 2);
        assert_eq!(updated[0].name, "origins");
        assert_eq!(updated[0].values, vec![3, 4]);
        assert_eq!(updated[1].name, "other");
        assert_eq!(updated[1].values, vec![9]);
    }
}
