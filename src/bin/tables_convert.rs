use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use tables::common::{read_tables_json, write_json, Table};
use tables::tables_file::{
    read_tables_bundle, write_tables_bundle, OriginArray, StoredTable, TablesBundle,
};

enum Command {
    JsonToTables,
    TablesToJson,
}

struct Args {
    command: Command,
    tables_input: Option<PathBuf>,
    origins_input: Option<PathBuf>,
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    tables_output: Option<PathBuf>,
    origins_output: Option<PathBuf>,
    origin_name: String,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut iter = env::args().skip(1);
        let Some(command) = iter.next() else {
            print_usage();
            std::process::exit(1);
        };

        let command = match command.as_str() {
            "json-to-tables" => Command::JsonToTables,
            "tables-to-json" => Command::TablesToJson,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown command: {other}"),
        };

        let mut args = Self {
            command,
            tables_input: None,
            origins_input: None,
            input: None,
            output: None,
            tables_output: None,
            origins_output: None,
            origin_name: "origins".to_string(),
        };

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--tables" => args.tables_input = Some(PathBuf::from(expect_value(&mut iter, &flag)?)),
                "--origins" => args.origins_input = Some(PathBuf::from(expect_value(&mut iter, &flag)?)),
                "--input" => args.input = Some(PathBuf::from(expect_value(&mut iter, &flag)?)),
                "--output" => args.output = Some(PathBuf::from(expect_value(&mut iter, &flag)?)),
                "--tables-output" => {
                    args.tables_output = Some(PathBuf::from(expect_value(&mut iter, &flag)?))
                }
                "--origins-output" => {
                    args.origins_output = Some(PathBuf::from(expect_value(&mut iter, &flag)?))
                }
                "--origin-name" => args.origin_name = expect_value(&mut iter, &flag)?,
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
    match args.command {
        Command::JsonToTables => run_json_to_tables(&args),
        Command::TablesToJson => run_tables_to_json(&args),
    }
}

fn run_json_to_tables(args: &Args) -> Result<()> {
    let tables_path = args
        .tables_input
        .as_ref()
        .ok_or_else(|| anyhow!("--tables is required for json-to-tables"))?;
    let output_path = args
        .output
        .as_ref()
        .ok_or_else(|| anyhow!("--output is required for json-to-tables"))?;

    let tables = read_tables_json(tables_path)?;
    let origin_arrays = if let Some(origins_path) = &args.origins_input {
        vec![OriginArray {
            name: args.origin_name.clone(),
            values: read_u32_json_array(origins_path)?,
        }]
    } else {
        Vec::new()
    };

    let bundle = TablesBundle {
        origin_arrays,
        tables: tables.iter().map(StoredTable::from_table).collect(),
    };
    write_tables_bundle(output_path, &bundle)?;

    println!("tables: {}", tables.len());
    println!("origin arrays: {}", bundle.origin_arrays.len());
    println!("output: {}", output_path.display());
    Ok(())
}

fn run_tables_to_json(args: &Args) -> Result<()> {
    let input_path = args
        .input
        .as_ref()
        .ok_or_else(|| anyhow!("--input is required for tables-to-json"))?;
    let tables_output = args
        .tables_output
        .as_ref()
        .ok_or_else(|| anyhow!("--tables-output is required for tables-to-json"))?;

    let bundle = read_tables_bundle(input_path)?;
    let TablesBundle {
        origin_arrays,
        tables,
    } = bundle;
    let tables: Vec<Table> = tables
        .into_iter()
        .map(StoredTable::try_into_table)
        .collect::<Result<_>>()?;
    write_json(tables_output, &tables)?;

    if let Some(origins_output) = &args.origins_output {
        let origins = select_origin_array(&origin_arrays, &args.origin_name)?;
        write_json(origins_output, origins)?;
    }

    println!("tables: {}", tables.len());
    println!("origin arrays: {}", origin_arrays.len());
    println!("input: {}", input_path.display());
    Ok(())
}

fn select_origin_array<'a>(origin_arrays: &'a [OriginArray], name: &str) -> Result<&'a [u32]> {
    match origin_arrays.len() {
        0 => Ok(&[]),
        1 => {
            let only = &origin_arrays[0];
            if only.name == name || name == "origins" {
                Ok(&only.values)
            } else {
                bail!(
                    "origin array '{}' not found; only '{}' is present",
                    name,
                    only.name
                )
            }
        }
        _ => origin_arrays
            .iter()
            .find(|origin_array| origin_array.name == name)
            .map(|origin_array| origin_array.values.as_slice())
            .ok_or_else(|| anyhow!("origin array '{}' not found", name)),
    }
}

fn read_u32_json_array(path: &Path) -> Result<Vec<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin tables_convert -- json-to-tables --tables <tables.json> [--origins <origins.json>] [--origin-name <name>] --output <file.tables>\n  cargo run --release --bin tables_convert -- tables-to-json --input <file.tables> --tables-output <tables.json> [--origins-output <origins.json>] [--origin-name <name>]"
    );
}
