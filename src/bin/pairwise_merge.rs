#![recursion_limit = "256"]

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tables::common::{read_tables, write_json};
use tables::pairwise_merge::run_pairwise_merge;

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    max_result_arity: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from(
                "data/derived/tables.common_node_fixed_point.pairwise_merge.json",
            ),
            report: PathBuf::from(
                "data/reports/report.common_node_fixed_point.pairwise_merge.json",
            ),
            max_result_arity: 16,
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
                "--max-result-arity" => {
                    args.max_result_arity = expect_value(&mut iter, "--max-result-arity")?
                        .parse()
                        .with_context(|| "invalid value for --max-result-arity")?;
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

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)?;
    let (output_tables, stats) = run_pairwise_merge(&tables, args.max_result_arity)?;

    let mut report = serde_json::to_value(&stats).map_err(|error| anyhow!(error))?;
    let Value::Object(ref mut object) = report else {
        bail!("pairwise merge stats did not serialize to an object");
    };
    object.insert(
        "method".to_string(),
        json!("pairwise natural join over all table pairs with more than one shared bit, bounded by max_result_arity, with immediate dropping of source tables covered by retained merged tables"),
    );
    object.insert("input".to_string(), json!(path_string(&args.input)));
    object.insert("output".to_string(), json!(path_string(&args.output)));
    object.insert("backend".to_string(), json!("rust"));

    write_json(&args.output, &output_tables)?;
    write_json(&args.report, &report)?;

    println!("candidate pairs: {}", stats.candidate_pair_count);
    println!(
        "produced nonempty merges: {}",
        stats.produced_nonempty_merges
    );
    println!("dropped source tables: {}", stats.dropped_source_tables);
    println!("final tables: {}", output_tables.len());
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin tables-pairwise-merge -- --input <path> --output <path> --report <path> [--max-result-arity <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
