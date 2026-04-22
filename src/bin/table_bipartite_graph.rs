use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use tables::common::{read_tables, write_json};
use tables::table_bipartite_graph::build_table_bipartite_graph;

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    max_tables: Option<usize>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from("runs/2026-04-05-table-bipartite-graph/graph.json"),
            report: PathBuf::from("runs/2026-04-05-table-bipartite-graph/report.json"),
            max_tables: None,
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
                "--max-tables" => {
                    args.max_tables = Some(
                        expect_value(&mut iter, "--max-tables")?
                            .parse()
                            .with_context(|| "invalid value for --max-tables")?,
                    );
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
    let started = Instant::now();

    let mut tables = read_tables(&args.input)
        .with_context(|| format!("failed to load tables from {}", args.input.display()))?;
    let input_table_count = tables.len();
    if let Some(max_tables) = args.max_tables {
        tables.truncate(max_tables.min(tables.len()));
    }

    let graph = build_table_bipartite_graph(&tables);
    let elapsed_seconds = started.elapsed().as_secs_f64();

    let report = json!({
        "method": "build a table-overlap graph where tables are adjacent iff they share at least one bit, and represent row-level consistency on each adjacent table pair as complete bipartite compatibility blocks grouped by equal projections on the shared bits",
        "input": path_string(&args.input),
        "output": path_string(&args.output),
        "max_tables": args.max_tables,
        "input_table_count_before_limit": input_table_count,
        "effective_table_count": tables.len(),
        "elapsed_seconds": elapsed_seconds,
        "graph_stats": &graph.stats,
    });

    write_json(&args.output, &graph)?;
    write_json(&args.report, &report)?;

    println!("input_table_count_before_limit={input_table_count}");
    println!("effective_table_count={}", tables.len());
    println!("table_edges={}", graph.stats.table_edge_count);
    println!(
        "compatible_row_blocks={}",
        graph.stats.compatible_row_block_count
    );
    println!(
        "compatible_row_pairs={}",
        graph.stats.compatible_row_pair_count
    );
    println!("elapsed_seconds={elapsed_seconds:.3}");
    println!("output={}", args.output.display());
    println!("report={}", args.report.display());

    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin table_bipartite_graph -- --input <path> [--output <path>] [--report <path>] [--max-tables <n>]"
    );
}
