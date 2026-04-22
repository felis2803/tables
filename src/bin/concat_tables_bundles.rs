use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use tables::tables_file::{read_tables_bundle, write_tables_bundle, OriginArray, TablesBundle};

struct Args {
    inputs: Vec<PathBuf>,
    output: PathBuf,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut iter = env::args().skip(1);
        let mut inputs = Vec::new();
        let mut output = None;

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--input" => inputs.push(PathBuf::from(expect_value(&mut iter, "--input")?)),
                "--output" => output = Some(PathBuf::from(expect_value(&mut iter, "--output")?)),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }

        if inputs.is_empty() {
            bail!("at least one --input is required");
        }

        Ok(Self {
            inputs,
            output: output.ok_or_else(|| anyhow!("--output is required"))?,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    let mut tables = Vec::new();
    let mut origin_arrays_by_name: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    for input in &args.inputs {
        let bundle = read_tables_bundle(input)
            .with_context(|| format!("failed to read {}", input.display()))?;
        tables.extend(bundle.tables);

        for origin_array in bundle.origin_arrays {
            match origin_arrays_by_name.get(&origin_array.name) {
                Some(existing) if existing != &origin_array.values => {
                    bail!(
                        "conflicting origin array '{}' between inputs",
                        origin_array.name
                    );
                }
                Some(_) => {}
                None => {
                    origin_arrays_by_name.insert(origin_array.name, origin_array.values);
                }
            }
        }
    }

    let origin_arrays = origin_arrays_by_name
        .into_iter()
        .map(|(name, values)| OriginArray { name, values })
        .collect();

    let bundle = TablesBundle {
        origin_arrays,
        tables,
    };
    write_tables_bundle(&args.output, &bundle)
        .with_context(|| format!("failed to write {}", args.output.display()))?;

    println!("inputs: {}", args.inputs.len());
    println!("tables: {}", bundle.tables.len());
    println!("origin_arrays: {}", bundle.origin_arrays.len());
    println!("output: {}", args.output.display());
    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin concat_tables_bundles -- --input <file1.tables> --input <file2.tables> [--input <fileN.tables>] --output <out.tables>"
    );
}
