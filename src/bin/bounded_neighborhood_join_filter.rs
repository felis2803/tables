use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::json;
use tables::bounded_neighborhood_join_filter::{
    filter_tables_by_bounded_neighborhood_join_with_settings, BoundedNeighborhoodJoinInfo,
    BoundedNeighborhoodJoinSettings,
};
use tables::common::{arity_distribution, collect_bits, read_tables, total_rows, write_json, Table};
use tables::rank_stats::{summarize_table_ranks, RankSummary};

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    output: PathBuf,
    report: PathBuf,
    max_rounds: Option<usize>,
    max_union_bits: usize,
    max_tables_per_neighborhood: usize,
    min_tables_per_neighborhood: usize,
}

impl Default for Args {
    fn default() -> Self {
        let settings = BoundedNeighborhoodJoinSettings::default();
        Self {
            input: PathBuf::from(
                "runs/2026-04-05-main-chain-neighbor-consistency-fixed-point/tables.fixed_point.json",
            ),
            output: PathBuf::from(
                "runs/2026-04-05-bounded-neighborhood-join-experiment/tables.fixed_point.json",
            ),
            report: PathBuf::from(
                "runs/2026-04-05-bounded-neighborhood-join-experiment/report.json",
            ),
            max_rounds: None,
            max_union_bits: settings.max_union_bits,
            max_tables_per_neighborhood: settings.max_tables_per_neighborhood,
            min_tables_per_neighborhood: settings.min_tables_per_neighborhood,
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
                "--max-rounds" => {
                    args.max_rounds = Some(
                        expect_value(&mut iter, "--max-rounds")?
                            .parse()
                            .with_context(|| "invalid value for --max-rounds")?,
                    );
                }
                "--max-union-bits" => {
                    args.max_union_bits = expect_value(&mut iter, "--max-union-bits")?
                        .parse()
                        .with_context(|| "invalid value for --max-union-bits")?;
                }
                "--max-tables-per-neighborhood" => {
                    args.max_tables_per_neighborhood =
                        expect_value(&mut iter, "--max-tables-per-neighborhood")?
                            .parse()
                            .with_context(|| "invalid value for --max-tables-per-neighborhood")?;
                }
                "--min-tables-per-neighborhood" => {
                    args.min_tables_per_neighborhood =
                        expect_value(&mut iter, "--min-tables-per-neighborhood")?
                            .parse()
                            .with_context(|| "invalid value for --min-tables-per-neighborhood")?;
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

    fn settings(&self) -> BoundedNeighborhoodJoinSettings {
        BoundedNeighborhoodJoinSettings {
            max_union_bits: self.max_union_bits,
            max_tables_per_neighborhood: self.max_tables_per_neighborhood,
            min_tables_per_neighborhood: self.min_tables_per_neighborhood,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct RoundReport {
    round: usize,
    input_table_count: usize,
    input_bit_count: usize,
    input_row_count: usize,
    input_arity_distribution: std::collections::BTreeMap<String, usize>,
    input_rank_summary: RankSummary,
    filter: BoundedNeighborhoodJoinInfo,
    output_table_count: usize,
    output_bit_count: usize,
    output_row_count: usize,
    output_arity_distribution: std::collections::BTreeMap<String, usize>,
    output_rank_summary: RankSummary,
    changed: bool,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let settings = args.settings();
    settings.validate()?;

    let start = Instant::now();
    let tables = read_tables(&args.input)?;
    let initial_table_count = tables.len();
    let initial_bit_count = collect_bits(&tables).len();
    let initial_row_count = total_rows(&tables);
    let initial_rank_summary = summarize_table_ranks(&tables, 10);

    let (output_tables, rounds, productive_rounds) =
        run_fixed_point_filter(tables, &settings, args.max_rounds)?;

    let report = json!({
        "method": "repeat bounded neighborhood exact-join projection filtering until no further change",
        "input": path_string(&args.input),
        "output": path_string(&args.output),
        "settings": settings,
        "max_rounds": args.max_rounds,
        "initial_table_count": initial_table_count,
        "initial_bit_count": initial_bit_count,
        "initial_row_count": initial_row_count,
        "initial_rank_summary": initial_rank_summary,
        "final_table_count": output_tables.len(),
        "final_bit_count": collect_bits(&output_tables).len(),
        "final_row_count": total_rows(&output_tables),
        "final_rank_summary": summarize_table_ranks(&output_tables, 10),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": rounds.len(),
        "elapsed_seconds": start.elapsed().as_secs_f64(),
        "total_candidate_anchor_tables": rounds.iter().map(|round| round.filter.candidate_anchor_tables).sum::<usize>(),
        "total_joined_anchor_tables": rounds.iter().map(|round| round.filter.joined_anchor_tables).sum::<usize>(),
        "total_changed_tables": rounds.iter().map(|round| round.filter.changed_tables).sum::<usize>(),
        "total_removed_rows": rounds.iter().map(|round| round.filter.removed_rows).sum::<usize>(),
        "rounds": rounds,
    });

    write_json(&args.output, &output_tables)?;
    write_json(&args.report, &report)?;

    println!("productive rounds: {productive_rounds}");
    println!("rounds including final check: {}", report["round_count_including_final_check"]);
    println!("final tables: {}", output_tables.len());
    println!("final bits: {}", collect_bits(&output_tables).len());
    println!("final rows: {}", total_rows(&output_tables));
    println!("elapsed seconds: {:.3}", start.elapsed().as_secs_f64());
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

fn run_fixed_point_filter(
    mut tables: Vec<Table>,
    settings: &BoundedNeighborhoodJoinSettings,
    max_rounds: Option<usize>,
) -> Result<(Vec<Table>, Vec<RoundReport>, usize)> {
    let mut rounds = Vec::new();
    let mut productive_rounds = 0usize;
    let mut round_index = 1usize;

    loop {
        let input_table_count = tables.len();
        let input_bit_count = collect_bits(&tables).len();
        let input_row_count = total_rows(&tables);
        let input_arity_distribution = arity_distribution(&tables);
        let input_rank_summary = summarize_table_ranks(&tables, 10);

        let (output_tables, info) =
            filter_tables_by_bounded_neighborhood_join_with_settings(&tables, settings)?;
        let changed = info.removed_rows > 0;

        let round = RoundReport {
            round: round_index,
            input_table_count,
            input_bit_count,
            input_row_count,
            input_arity_distribution,
            input_rank_summary,
            filter: info,
            output_table_count: output_tables.len(),
            output_bit_count: collect_bits(&output_tables).len(),
            output_row_count: total_rows(&output_tables),
            output_arity_distribution: arity_distribution(&output_tables),
            output_rank_summary: summarize_table_ranks(&output_tables, 10),
            changed,
        };
        rounds.push(round);
        tables = output_tables;

        if !changed {
            break;
        }

        productive_rounds += 1;
        if max_rounds.is_some_and(|limit| round_index >= limit) {
            break;
        }
        round_index += 1;
    }

    Ok((tables, rounds, productive_rounds))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin bounded_neighborhood_join_filter -- --input <path> [--output <path>] [--report <path>] [--max-rounds <n>] [--max-union-bits <n>] [--max-tables-per-neighborhood <n>] [--min-tables-per-neighborhood <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
