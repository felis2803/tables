use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use tables::common::{read_tables, write_json, Table};

struct Args {
    input: PathBuf,
    output: PathBuf,
    exclude_self: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from("runs/2026-04-04-related-bits-map/related_bits_map.json"),
            exclude_self: false,
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
                "--exclude-self" => args.exclude_self = true,
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

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)
        .with_context(|| format!("failed to load tables from {}", args.input.display()))?;
    let related_bits_map = build_related_bits_map(&tables, args.exclude_self);

    write_json(&args.output, &related_bits_map)?;

    let relation_count: usize = related_bits_map.values().map(BTreeSet::len).sum();
    let max_related = related_bits_map
        .values()
        .map(BTreeSet::len)
        .max()
        .unwrap_or(0);

    println!("tables: {}", tables.len());
    println!("bits: {}", related_bits_map.len());
    println!("total related entries: {}", relation_count);
    println!("max related bits for one key: {}", max_related);
    println!("exclude self: {}", args.exclude_self);
    println!("output: {}", args.output.display());

    Ok(())
}

fn build_related_bits_map(
    tables: &[Table],
    exclude_self: bool,
) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut related_bits: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

    for table in tables {
        let bits: BTreeSet<u32> = table.bits.iter().copied().collect();
        for &bit in &table.bits {
            let entry = related_bits.entry(bit).or_default();
            entry.extend(bits.iter().copied());
            if exclude_self {
                entry.remove(&bit);
            }
        }
    }

    related_bits
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --bin build_related_bits_map -- --input <path> --output <path> [--exclude-self]"
    );
}
