use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json};

#[derive(Debug)]
struct Args {
    input: PathBuf,
    max_merge_arity: usize,
    output: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut input = None;
        let mut max_merge_arity = 16usize;
        let mut output = None;
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => input = Some(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--max-merge-arity" => {
                    max_merge_arity = expect_value(&mut iter, "--max-merge-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-merge-arity")?;
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
            max_merge_arity,
            output: output.context("missing --output")?,
        })
    }
}

#[derive(Debug, Serialize)]
struct BitRecord {
    bit: u32,
    table_count: usize,
    merge_arity: usize,
}

#[derive(Debug, Serialize)]
struct Report {
    input: String,
    max_merge_arity: usize,
    total_bits: usize,
    qualifying_bits: usize,
    qualifying_by_merge_arity: BTreeMap<String, usize>,
    bits: Vec<BitRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)?;

    let mut bit_to_tables: BTreeMap<u32, usize> = BTreeMap::new();
    let mut bit_to_union_bits: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

    for table in &tables {
        for &bit in &table.bits {
            *bit_to_tables.entry(bit).or_insert(0) += 1;
            let union_bits = bit_to_union_bits.entry(bit).or_default();
            union_bits.extend(table.bits.iter().copied());
        }
    }

    let total_bits = bit_to_union_bits.len();
    let mut qualifying_by_merge_arity: BTreeMap<String, usize> = BTreeMap::new();
    let mut bits = Vec::new();

    for (bit, union_bits) in bit_to_union_bits {
        let merge_arity = union_bits.len();
        if merge_arity > args.max_merge_arity {
            continue;
        }

        *qualifying_by_merge_arity
            .entry(merge_arity.to_string())
            .or_insert(0) += 1;
        bits.push(BitRecord {
            bit,
            table_count: bit_to_tables[&bit],
            merge_arity,
        });
    }

    let report = Report {
        input: args.input.display().to_string(),
        max_merge_arity: args.max_merge_arity,
        total_bits,
        qualifying_bits: bits.len(),
        qualifying_by_merge_arity,
        bits,
    };

    write_json(&args.output, &report)?;
    println!(
        "qualifying_bits={} total_bits={} output={}",
        report.qualifying_bits,
        report.total_bits,
        args.output.display()
    );
    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --example find_bits_mergeable_with_all_incident_tables -- --input <tables.json> [--max-merge-arity <n>] --output <report.json>"
    );
}
