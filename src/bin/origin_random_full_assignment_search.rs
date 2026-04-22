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
    origins: PathBuf,
    output: PathBuf,
    pipeline_exe: PathBuf,
    work_root: PathBuf,
    trials: usize,
    seed: u64,
    max_rounds: Option<usize>,
    disable_zero_collapse_bit_filter: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/derived/tables.common_node_fixed_point.json"),
            origins: PathBuf::from("data/raw/origins.json"),
            output: PathBuf::from(
                "data/reports/origin_random_full_assignment_search.common_node_fixed_point.json",
            ),
            pipeline_exe: PathBuf::from("target/release/tables.exe"),
            work_root: PathBuf::from("experiments/origin_random_full_assignment_search"),
            trials: 64,
            seed: 20260407,
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
                "--origins" => args.origins = PathBuf::from(expect_value(&mut iter, "--origins")?),
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--pipeline-exe" => {
                    args.pipeline_exe = PathBuf::from(expect_value(&mut iter, "--pipeline-exe")?)
                }
                "--work-root" => {
                    args.work_root = PathBuf::from(expect_value(&mut iter, "--work-root")?)
                }
                "--trials" => {
                    args.trials = expect_value(&mut iter, "--trials")?
                        .parse()
                        .with_context(|| "invalid value for --trials")?;
                }
                "--seed" => {
                    args.seed = expect_value(&mut iter, "--seed")?
                        .parse()
                        .with_context(|| "invalid value for --seed")?;
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

        if args.trials == 0 {
            bail!("--trials must be positive");
        }

        Ok(args)
    }
}

#[derive(Clone, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

#[derive(Clone, Debug, Serialize)]
struct TrialResult {
    trial: usize,
    contradiction: bool,
    contradiction_stage: Option<String>,
    error: Option<String>,
    assignment: Vec<ForcedRow>,
    ones: usize,
    zeros: usize,
    initial_forcing_affected_tables: usize,
    initial_forcing_changed_tables: usize,
    initial_forcing_removed_rows: usize,
    initial_forcing_collapsed_duplicate_tables: usize,
    final_table_count: Option<usize>,
    final_bit_count: Option<usize>,
    final_row_count: Option<usize>,
    productive_round_count: Option<usize>,
    round_count_including_final_check: Option<usize>,
    elapsed_ms: u128,
    preserved_artifact_dir: Option<String>,
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn read_origins(path: &Path) -> Result<Vec<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let origins = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(origins)
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

fn print_usage() {
    println!(
        "usage: cargo run --release --bin origin_random_full_assignment_search -- [--input <path>] [--origins <path>] [--output <path>] [--pipeline-exe <path>] [--work-root <dir>] [--trials <n>] [--seed <n>] [--max-rounds <n>] [--disable-zero-collapse-bit-filter]"
    );
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let base_tables = read_tables(&args.input)?;
    let origins = read_origins(&args.origins)?;

    fs::create_dir_all(&args.work_root)
        .with_context(|| format!("failed to create {}", args.work_root.display()))?;

    let started = Instant::now();
    let mut rng = SplitMix64::new(args.seed);
    let mut results = Vec::with_capacity(args.trials);

    for trial_index in 0..args.trials {
        let trial_number = trial_index + 1;
        let trial_started = Instant::now();
        let trial_dir = args.work_root.join(format!("trial_{trial_number:03}"));
        if trial_dir.exists() {
            fs::remove_dir_all(&trial_dir)
                .with_context(|| format!("failed to reset {}", trial_dir.display()))?;
        }
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("failed to create {}", trial_dir.display()))?;

        let assignment: Vec<ForcedRow> = origins
            .iter()
            .copied()
            .map(|bit| ForcedRow {
                bit,
                value: (rng.next_u64() & 1) as u8,
            })
            .collect();
        let ones = assignment.iter().filter(|row| row.value == 1).count();
        let zeros = assignment.len() - ones;
        println!(
            "[{trial_number}/{}] forcing 32 origin bits randomly",
            args.trials
        );

        let forced_map: BTreeMap<u32, u8> =
            assignment.iter().map(|row| (row.bit, row.value)).collect();
        let (forced_tables, forcing_stats) = match propagate_forced_bits(&base_tables, &forced_map)
        {
            Ok(result) => result,
            Err(err) => {
                let assignment_path = trial_dir.join("assignment.json");
                write_json(&assignment_path, &assignment)?;
                println!("  contradiction at initial_assignment");
                results.push(TrialResult {
                    trial: trial_number,
                    contradiction: true,
                    contradiction_stage: Some("initial_assignment".to_string()),
                    error: Some(err.to_string()),
                    assignment,
                    ones,
                    zeros,
                    initial_forcing_affected_tables: 0,
                    initial_forcing_changed_tables: 0,
                    initial_forcing_removed_rows: 0,
                    initial_forcing_collapsed_duplicate_tables: 0,
                    final_table_count: None,
                    final_bit_count: None,
                    final_row_count: None,
                    productive_round_count: None,
                    round_count_including_final_check: None,
                    elapsed_ms: trial_started.elapsed().as_millis(),
                    preserved_artifact_dir: Some(trial_dir.to_string_lossy().into_owned()),
                });
                continue;
            }
        };

        let input_path = trial_dir.join("tables.after_forcing.json");
        let output_path = trial_dir.join("tables.final.json");
        let report_path = trial_dir.join("report.json");
        let forced_path = trial_dir.join("bits.forced.json");
        let mapping_path = trial_dir.join("bits.rewrite_map.json");
        let components_path = trial_dir.join("bits.components.json");
        let dropped_path = trial_dir.join("tables.dropped_included.json");
        let relations_path = trial_dir.join("pairs.relations.json");
        let nodes_path = trial_dir.join("nodes.json");
        let assignment_path = trial_dir.join("assignment.json");

        write_json(&input_path, &forced_tables)?;
        write_json(&assignment_path, &assignment)?;

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
            .arg(&output_path)
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

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let error = if !stderr.is_empty() { stderr } else { stdout };
            println!("  contradiction during pipeline");
            results.push(TrialResult {
                trial: trial_number,
                contradiction: true,
                contradiction_stage: Some("pipeline".to_string()),
                error: Some(error),
                assignment,
                ones,
                zeros,
                initial_forcing_affected_tables: forcing_stats.affected_tables,
                initial_forcing_changed_tables: forcing_stats.changed_tables,
                initial_forcing_removed_rows: forcing_stats.removed_rows,
                initial_forcing_collapsed_duplicate_tables: forcing_stats
                    .collapsed_duplicate_tables,
                final_table_count: None,
                final_bit_count: None,
                final_row_count: None,
                productive_round_count: None,
                round_count_including_final_check: None,
                elapsed_ms: trial_started.elapsed().as_millis(),
                preserved_artifact_dir: Some(trial_dir.to_string_lossy().into_owned()),
            });
            continue;
        }

        let (
            final_table_count,
            final_bit_count,
            final_row_count,
            productive_round_count,
            round_count_including_final_check,
        ) = parse_report_summary(&report_path)?;
        println!(
            "  ok: rounds={}, tables={}, bits={}, rows={}",
            productive_round_count, final_table_count, final_bit_count, final_row_count
        );

        results.push(TrialResult {
            trial: trial_number,
            contradiction: false,
            contradiction_stage: None,
            error: None,
            assignment,
            ones,
            zeros,
            initial_forcing_affected_tables: forcing_stats.affected_tables,
            initial_forcing_changed_tables: forcing_stats.changed_tables,
            initial_forcing_removed_rows: forcing_stats.removed_rows,
            initial_forcing_collapsed_duplicate_tables: forcing_stats.collapsed_duplicate_tables,
            final_table_count: Some(final_table_count),
            final_bit_count: Some(final_bit_count),
            final_row_count: Some(final_row_count),
            productive_round_count: Some(productive_round_count),
            round_count_including_final_check: Some(round_count_including_final_check),
            elapsed_ms: trial_started.elapsed().as_millis(),
            preserved_artifact_dir: None,
        });

        fs::remove_dir_all(&trial_dir)
            .with_context(|| format!("failed to clean {}", trial_dir.display()))?;
    }

    let contradiction_count = results.iter().filter(|result| result.contradiction).count();
    let report = serde_json::json!({
        "input": args.input.to_string_lossy(),
        "origins": args.origins.to_string_lossy(),
        "pipeline_exe": args.pipeline_exe.to_string_lossy(),
        "work_root": args.work_root.to_string_lossy(),
        "output": args.output.to_string_lossy(),
        "trials": args.trials,
        "seed": args.seed,
        "max_rounds": args.max_rounds,
        "zero_collapse_bit_filter_enabled": !args.disable_zero_collapse_bit_filter,
        "contradiction_count": contradiction_count,
        "total_elapsed_ms": started.elapsed().as_millis(),
        "results": results,
    });
    write_json(&args.output, &report)?;

    println!("contradictions: {contradiction_count}");
    println!("report: {}", args.output.display());
    Ok(())
}
