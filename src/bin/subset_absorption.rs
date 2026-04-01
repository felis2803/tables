#![recursion_limit = "256"]

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use serde_json::json;
use tables::common::{read_tables, write_json};
use tables::rank_stats::summarize_table_ranks;
use tables::subset_absorption::{
    collapse_equal_bitsets, merge_subsets, prune_included_tables, to_tables,
};

struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    pairs_output: PathBuf,
    dropped_output: PathBuf,
    prune_included: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            output: PathBuf::from("data/derived/tables.subset_pruned.json"),
            report: PathBuf::from("data/reports/report.subset_pruned.json"),
            pairs_output: PathBuf::from("data/derived/pairs.subset_pruned.subset_superset.json"),
            dropped_output: PathBuf::from(
                "data/derived/tables.subset_pruned.dropped_included.json",
            ),
            prune_included: false,
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
                "--pairs-output" => {
                    args.pairs_output = PathBuf::from(expect_value(&mut iter, "--pairs-output")?)
                }
                "--dropped-output" => {
                    args.dropped_output =
                        PathBuf::from(expect_value(&mut iter, "--dropped-output")?)
                }
                "--prune-included" => args.prune_included = true,
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
    let input_tables = read_tables(&args.input)?;

    let (mut tables_by_bits, duplicate_count) = collapse_equal_bitsets(&input_tables);
    let (merge_stats, pair_details) = merge_subsets(&mut tables_by_bits);
    let canonical_count_before_prune = tables_by_bits.len();
    let effective_pairs = pair_details
        .iter()
        .filter(|pair| pair.rows_removed > 0)
        .count();
    let (tables_by_bits, dropped_tables) = if args.prune_included {
        prune_included_tables(&tables_by_bits, &pair_details)
    } else {
        (tables_by_bits, Vec::new())
    };

    let output_tables = to_tables(&tables_by_bits);
    let report = json!({
        "method": "canonicalize bit order, intersect equal bitsets, merge strict subset tables into supersets",
        "input": path_string(&args.input),
        "output": path_string(&args.output),
        "backend": "rust",
        "prune_included": args.prune_included,
        "original_table_count": input_tables.len(),
        "canonical_table_count_before_prune": canonical_count_before_prune,
        "final_table_count": output_tables.len(),
        "collapsed_duplicate_tables": duplicate_count,
        "canonical_table_count": tables_by_bits.len(),
        "subset_superset_pairs": merge_stats.pair_count,
        "effective_pairs": effective_pairs,
        "effective_subset_pairs": effective_pairs,
        "dropped_included_tables": dropped_tables.len(),
        "changed_tables": merge_stats.changed_tables,
        "subset_changed_tables": merge_stats.changed_tables,
        "row_deletions": merge_stats.row_deletions,
        "subset_row_deletions": merge_stats.row_deletions,
        "emptied_tables": merge_stats.emptied_tables,
        "emptied_tables_during_subset_merge": merge_stats.emptied_tables,
        "input_rank_summary": summarize_table_ranks(&input_tables, 10),
        "final_rank_summary": summarize_table_ranks(&output_tables, 10),
    });

    write_json(&args.output, &output_tables)?;
    write_json(&args.report, &report)?;
    write_json(&args.pairs_output, &pair_details)?;
    write_json(&args.dropped_output, &dropped_tables)?;

    println!("original tables: {}", input_tables.len());
    println!("canonical tables before prune: {canonical_count_before_prune}");
    println!("final tables: {}", output_tables.len());
    println!("duplicate tables collapsed: {duplicate_count}");
    println!("subset/superset pairs: {}", merge_stats.pair_count);
    println!("changed tables: {}", merge_stats.changed_tables);
    println!("row deletions: {}", merge_stats.row_deletions);
    println!("emptied tables: {}", merge_stats.emptied_tables);
    println!("dropped included tables: {}", dropped_tables.len());
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());
    println!("pairs: {}", args.pairs_output.display());
    println!("dropped: {}", args.dropped_output.display());

    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin tables-subset-absorption -- --input <path> --output <path> --report <path> --pairs-output <path> --dropped-output <path> [--prune-included]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
