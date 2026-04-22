use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json};
use tables::table_decomposition::{
    canonicalize_table_for_decomposition, search_table_decompositions, LatentBicliqueDecomposition,
    ProjectionDecomposition,
};

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    report: PathBuf,
    details: PathBuf,
    max_small_side_bits: usize,
    max_examples_per_method: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            report: PathBuf::from("runs/2026-04-06-table-decompose-all/report.json"),
            details: PathBuf::from("runs/2026-04-06-table-decompose-all/details.json"),
            max_small_side_bits: 4,
            max_examples_per_method: 20,
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
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
                "--details" => args.details = PathBuf::from(expect_value(&mut iter, "--details")?),
                "--max-small-side-bits" => {
                    args.max_small_side_bits = expect_value(&mut iter, "--max-small-side-bits")?
                        .parse()
                        .with_context(|| "invalid value for --max-small-side-bits")?;
                }
                "--max-examples-per-method" => {
                    args.max_examples_per_method =
                        expect_value(&mut iter, "--max-examples-per-method")?
                            .parse()
                            .with_context(|| "invalid value for --max-examples-per-method")?;
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

#[derive(Clone, Debug, Default, Serialize)]
struct DecompositionCoverage {
    total_tables: usize,
    exact_projection_tables: usize,
    exact_latent_tables: usize,
    exact_latent_arity_reducing_tables: usize,
    selected_projection_tables: usize,
    selected_latent_tables: usize,
    selected_none_tables: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ExampleRecord {
    table_index: usize,
    source_bits: Vec<u32>,
    canonical_bits: Vec<u32>,
    row_count: usize,
    projection: Option<ProjectionDecomposition>,
    latent: Option<LatentBicliqueDecomposition>,
    selected_method: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct DetailRecord {
    table_index: usize,
    source_bits: Vec<u32>,
    canonical_bits: Vec<u32>,
    row_count: usize,
    exact_projection: Option<ProjectionDecomposition>,
    exact_latent_biclique: Option<LatentBicliqueDecomposition>,
    selected_method: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ArityStats {
    table_count: usize,
    exact_projection_tables: usize,
    exact_latent_tables: usize,
    exact_latent_arity_reducing_tables: usize,
    selected_projection_tables: usize,
    selected_latent_tables: usize,
    selected_none_tables: usize,
}

#[derive(Clone, Debug, Serialize)]
struct Report {
    method: String,
    input: String,
    detail_output: String,
    max_small_side_bits: usize,
    elapsed_seconds: f64,
    coverage: DecompositionCoverage,
    by_arity: BTreeMap<String, ArityStats>,
    examples_projection: Vec<ExampleRecord>,
    examples_latent: Vec<ExampleRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let started = Instant::now();
    let tables = read_tables(&args.input)?;

    let mut coverage = DecompositionCoverage {
        total_tables: tables.len(),
        ..DecompositionCoverage::default()
    };
    let mut by_arity = BTreeMap::<String, ArityStats>::new();
    let mut examples_projection = Vec::new();
    let mut examples_latent = Vec::new();
    let mut details = Vec::with_capacity(tables.len());

    for (table_index, table) in tables.iter().enumerate() {
        let canonical = canonicalize_table_for_decomposition(table)?;
        let search =
            search_table_decompositions(table, args.max_small_side_bits).with_context(|| {
                format!(
                    "failed to search decompositions for table_index {}",
                    table_index
                )
            })?;

        let arity_key = canonical.bits.len().to_string();
        let arity_stats = by_arity.entry(arity_key).or_default();
        arity_stats.table_count += 1;

        let selected_method = if search.exact_projection.is_some() {
            coverage.exact_projection_tables += 1;
            arity_stats.exact_projection_tables += 1;
            coverage.selected_projection_tables += 1;
            arity_stats.selected_projection_tables += 1;
            Some("exact_projection".to_string())
        } else if search
            .exact_latent_biclique
            .as_ref()
            .is_some_and(|latent| latent.arity_reducing)
        {
            coverage.selected_latent_tables += 1;
            arity_stats.selected_latent_tables += 1;
            Some("exact_latent_biclique".to_string())
        } else {
            coverage.selected_none_tables += 1;
            arity_stats.selected_none_tables += 1;
            None
        };

        if let Some(latent) = &search.exact_latent_biclique {
            coverage.exact_latent_tables += 1;
            arity_stats.exact_latent_tables += 1;
            if latent.arity_reducing {
                coverage.exact_latent_arity_reducing_tables += 1;
                arity_stats.exact_latent_arity_reducing_tables += 1;
            }
        }

        if let Some(projection) = &search.exact_projection {
            if examples_projection.len() < args.max_examples_per_method {
                examples_projection.push(ExampleRecord {
                    table_index,
                    source_bits: table.bits.clone(),
                    canonical_bits: canonical.bits.clone(),
                    row_count: canonical.rows.len(),
                    projection: Some(projection.clone()),
                    latent: search.exact_latent_biclique.clone(),
                    selected_method: selected_method.clone(),
                });
            }
        }

        if search
            .exact_latent_biclique
            .as_ref()
            .is_some_and(|latent| latent.arity_reducing)
            && examples_latent.len() < args.max_examples_per_method
        {
            examples_latent.push(ExampleRecord {
                table_index,
                source_bits: table.bits.clone(),
                canonical_bits: canonical.bits.clone(),
                row_count: canonical.rows.len(),
                projection: search.exact_projection.clone(),
                latent: search.exact_latent_biclique.clone(),
                selected_method: selected_method.clone(),
            });
        }

        details.push(DetailRecord {
            table_index,
            source_bits: table.bits.clone(),
            canonical_bits: canonical.bits,
            row_count: canonical.rows.len(),
            exact_projection: search.exact_projection,
            exact_latent_biclique: search.exact_latent_biclique,
            selected_method,
        });
    }

    let elapsed_seconds = started.elapsed().as_secs_f64();
    let report = Report {
        method: "Scan every input table for an exact lossless decomposition into two smaller projection factors, then for an exact latent biclique decomposition into two smaller factors with an auxiliary latent bit block; prefer projection, otherwise prefer latent only when both resulting factors have strictly smaller arity than the original table.".to_string(),
        input: path_string(&args.input),
        detail_output: path_string(&args.details),
        max_small_side_bits: args.max_small_side_bits,
        elapsed_seconds,
        coverage,
        by_arity,
        examples_projection,
        examples_latent,
    };

    write_json(&args.details, &details)?;
    write_json(&args.report, &report)?;

    println!("total_tables={}", report.coverage.total_tables);
    println!(
        "exact_projection_tables={}",
        report.coverage.exact_projection_tables
    );
    println!(
        "exact_latent_tables={}",
        report.coverage.exact_latent_tables
    );
    println!(
        "exact_latent_arity_reducing_tables={}",
        report.coverage.exact_latent_arity_reducing_tables
    );
    println!(
        "selected_projection_tables={}",
        report.coverage.selected_projection_tables
    );
    println!(
        "selected_latent_tables={}",
        report.coverage.selected_latent_tables
    );
    println!(
        "selected_none_tables={}",
        report.coverage.selected_none_tables
    );
    println!("elapsed_seconds={elapsed_seconds:.3}");
    println!("report={}", args.report.display());
    println!("details={}", args.details.display());

    Ok(())
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin table_decompose_all -- --input <path> [--report <path>] [--details <path>] [--max-small-side-bits <n>] [--max-examples-per-method <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
