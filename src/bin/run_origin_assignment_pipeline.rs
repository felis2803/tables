use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use tables::common::{read_tables, write_json, ForcedRow};
use tables::forced_bits::propagate_forced_bits;

struct Args {
    input: PathBuf,
    assignment: PathBuf,
    output: PathBuf,
    pipeline_exe: PathBuf,
    work_dir: PathBuf,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/derived/tables.common_node_fixed_point.json"),
            assignment: PathBuf::from(
                "experiments/origin_random_full_assignment_search_smoke/trial_001/assignment.json",
            ),
            output: PathBuf::from("data/reports/run_origin_assignment_pipeline.json"),
            pipeline_exe: PathBuf::from("target/release/tables.exe"),
            work_dir: PathBuf::from("experiments/run_origin_assignment_pipeline"),
            max_rounds: None,
            disable_zero_collapse_bit_filter: false,
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
                "--assignment" => {
                    args.assignment = PathBuf::from(expect_value(&mut iter, "--assignment")?)
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--pipeline-exe" => {
                    args.pipeline_exe = PathBuf::from(expect_value(&mut iter, "--pipeline-exe")?)
                }
                "--work-dir" => {
                    args.work_dir = PathBuf::from(expect_value(&mut iter, "--work-dir")?)
                }
                "--max-rounds" => {
                    args.max_rounds = Some(
                        expect_value(&mut iter, "--max-rounds")?
                            .parse()
                            .with_context(|| "invalid value for --max-rounds")?,
                    );
                }
                "--disable-zero-collapse-bit-filter" => {
                    args.disable_zero_collapse_bit_filter = true;
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

#[derive(Clone, Debug, Serialize)]
struct RunReport {
    input: String,
    assignment: String,
    output: String,
    pipeline_exe: String,
    work_dir: String,
    assignment_size: usize,
    assignment_rows: Vec<ForcedRow>,
    contradiction: bool,
    error: Option<String>,
    affected_tables: usize,
    changed_tables: usize,
    removed_rows_before_pipeline: usize,
    collapsed_duplicate_tables_before_pipeline: usize,
    after_forcing_table_count: usize,
    final_table_count: Option<usize>,
    final_bit_count: Option<usize>,
    final_row_count: Option<usize>,
    productive_round_count: Option<usize>,
    round_count_including_final_check: Option<usize>,
    elapsed_ms: u128,
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin run_origin_assignment_pipeline -- [--input <path>] [--assignment <path>] [--output <path>] [--pipeline-exe <path>] [--work-dir <dir>] [--max-rounds <n>] [--disable-zero-collapse-bit-filter]"
    );
}

fn read_assignment(path: &Path) -> Result<Vec<ForcedRow>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let rows = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(rows)
}

fn parse_report_summary(path: &Path) -> Result<(usize, usize, usize, usize, usize)> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let report: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    Ok((
        report["final_table_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("missing final_table_count"))? as usize,
        report["final_bit_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("missing final_bit_count"))? as usize,
        report["final_row_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("missing final_row_count"))? as usize,
        report["productive_round_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("missing productive_round_count"))? as usize,
        report["round_count_including_final_check"]
            .as_u64()
            .ok_or_else(|| anyhow!("missing round_count_including_final_check"))? as usize,
    ))
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let base_tables = read_tables(&args.input)?;
    let assignment_rows = read_assignment(&args.assignment)?;

    if args.work_dir.exists() {
        fs::remove_dir_all(&args.work_dir)
            .with_context(|| format!("failed to reset {}", args.work_dir.display()))?;
    }
    fs::create_dir_all(&args.work_dir)
        .with_context(|| format!("failed to create {}", args.work_dir.display()))?;

    let started = Instant::now();
    let forced_map: BTreeMap<u32, u8> = assignment_rows
        .iter()
        .map(|row| (row.bit, row.value))
        .collect();
    let (forced_tables, forcing_stats) = propagate_forced_bits(&base_tables, &forced_map)?;

    let input_path = args.work_dir.join("tables.after_forcing.json");
    let pipeline_output_path = args.work_dir.join("tables.final.json");
    let report_path = args.work_dir.join("report.json");
    let forced_path = args.work_dir.join("bits.forced.json");
    let mapping_path = args.work_dir.join("bits.rewrite_map.json");
    let components_path = args.work_dir.join("bits.components.json");
    let dropped_path = args.work_dir.join("tables.dropped_included.json");
    let relations_path = args.work_dir.join("pairs.relations.json");
    let nodes_path = args.work_dir.join("nodes.json");
    write_json(&input_path, &forced_tables)?;

    let mut command = Command::new(&args.pipeline_exe);
    command.arg("--input").arg(&input_path);
    if let Some(max_rounds) = args.max_rounds {
        command.arg("--max-rounds").arg(max_rounds.to_string());
    }
    if args.disable_zero_collapse_bit_filter {
        command.arg("--disable-zero-collapse-bit-filter");
    }
    command
        .arg("--output")
        .arg(&pipeline_output_path)
        .arg("--report")
        .arg(&report_path)
        .arg("--forced")
        .arg(&forced_path)
        .arg("--mapping")
        .arg(&mapping_path)
        .arg("--components")
        .arg(&components_path)
        .arg("--dropped")
        .arg(&dropped_path)
        .arg("--relations")
        .arg(&relations_path)
        .arg("--nodes")
        .arg(&nodes_path);

    let output = command
        .output()
        .with_context(|| format!("failed to run {}", args.pipeline_exe.display()))?;

    let mut report = RunReport {
        input: args.input.to_string_lossy().into_owned(),
        assignment: args.assignment.to_string_lossy().into_owned(),
        output: args.output.to_string_lossy().into_owned(),
        pipeline_exe: args.pipeline_exe.to_string_lossy().into_owned(),
        work_dir: args.work_dir.to_string_lossy().into_owned(),
        assignment_size: assignment_rows.len(),
        assignment_rows,
        contradiction: !output.status.success(),
        error: None,
        affected_tables: forcing_stats.affected_tables,
        changed_tables: forcing_stats.changed_tables,
        removed_rows_before_pipeline: forcing_stats.removed_rows,
        collapsed_duplicate_tables_before_pipeline: forcing_stats.collapsed_duplicate_tables,
        after_forcing_table_count: forced_tables.len(),
        final_table_count: None,
        final_bit_count: None,
        final_row_count: None,
        productive_round_count: None,
        round_count_including_final_check: None,
        elapsed_ms: started.elapsed().as_millis(),
    };

    if report.contradiction {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        report.error = Some(if !stderr.is_empty() { stderr } else { stdout });
    } else {
        let (
            final_table_count,
            final_bit_count,
            final_row_count,
            productive_round_count,
            round_count_including_final_check,
        ) = parse_report_summary(&report_path)?;
        report.final_table_count = Some(final_table_count);
        report.final_bit_count = Some(final_bit_count);
        report.final_row_count = Some(final_row_count);
        report.productive_round_count = Some(productive_round_count);
        report.round_count_including_final_check = Some(round_count_including_final_check);
    }

    report.elapsed_ms = started.elapsed().as_millis();
    write_json(&args.output, &report)?;
    if report.contradiction {
        println!("contradiction");
        println!("{}", report.error.as_deref().unwrap_or("unknown error"));
    } else {
        println!(
            "ok: rounds={}, tables={}, bits={}, rows={}",
            report.productive_round_count.unwrap_or(0),
            report.final_table_count.unwrap_or(0),
            report.final_bit_count.unwrap_or(0),
            report.final_row_count.unwrap_or(0)
        );
    }
    println!("report: {}", args.output.display());
    Ok(())
}
