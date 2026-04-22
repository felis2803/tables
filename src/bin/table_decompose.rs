use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, write_json, Table};
use tables::table_decomposition::{
    canonicalize_table_for_decomposition, project_away_bits, search_table_decompositions,
    LatentBicliqueDecomposition, ProjectionDecomposition, TableDecompositionSearch,
};
use tables::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug)]
struct Args {
    input: PathBuf,
    table_index: usize,
    output: PathBuf,
    report: PathBuf,
    max_small_side_bits: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            table_index: 0,
            output: PathBuf::from("runs/2026-04-05-table-decompose/factors.json"),
            report: PathBuf::from("runs/2026-04-05-table-decompose/report.json"),
            max_small_side_bits: 4,
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
                "--table-index" => {
                    args.table_index = expect_value(&mut iter, "--table-index")?
                        .parse()
                        .with_context(|| "invalid value for --table-index")?;
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
                "--max-small-side-bits" => {
                    args.max_small_side_bits = expect_value(&mut iter, "--max-small-side-bits")?
                        .parse()
                        .with_context(|| "invalid value for --max-small-side-bits")?;
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
struct VerificationReport {
    merged_bits: Vec<u32>,
    merged_row_count: usize,
    reconstructed_bits: Vec<u32>,
    reconstructed_row_count: usize,
    matches_canonical_original: bool,
}

#[derive(Clone, Debug, Serialize)]
struct DecompositionReport {
    input: String,
    table_index: usize,
    source_bits: Vec<u32>,
    canonical_bits: Vec<u32>,
    canonical_row_count: usize,
    max_small_side_bits: usize,
    exact_projection: Option<ProjectionDecomposition>,
    exact_latent_biclique: Option<LatentBicliqueDecomposition>,
    selected_method: Option<String>,
    selected_factor_count: usize,
    verification: Option<VerificationReport>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let tables = read_tables(&args.input)?;
    let table = tables
        .get(args.table_index)
        .cloned()
        .with_context(|| format!("table_index {} is out of range", args.table_index))?;
    let canonical = canonicalize_table_for_decomposition(&table)?;

    let search = search_table_decompositions(&table, args.max_small_side_bits)?;
    let (selected_method, selected_factors, verification) = select_output(&canonical, &search)
        .with_context(|| "failed to verify selected decomposition")?;

    let report = DecompositionReport {
        input: path_string(&args.input),
        table_index: args.table_index,
        source_bits: table.bits.clone(),
        canonical_bits: canonical.bits.clone(),
        canonical_row_count: canonical.rows.len(),
        max_small_side_bits: args.max_small_side_bits,
        exact_projection: search.exact_projection,
        exact_latent_biclique: search.exact_latent_biclique,
        selected_method: selected_method.clone(),
        selected_factor_count: selected_factors.len(),
        verification,
    };

    write_json(&args.output, &selected_factors)?;
    write_json(&args.report, &report)?;

    println!("table_index={}", args.table_index);
    println!("original_arity={}", canonical.bits.len());
    println!("original_rows={}", canonical.rows.len());
    println!(
        "selected_method={}",
        selected_method.unwrap_or_else(|| "none".to_string())
    );
    println!("selected_factor_count={}", selected_factors.len());
    println!("output={}", args.output.display());
    println!("report={}", args.report.display());

    Ok(())
}

fn select_output(
    table: &Table,
    search: &TableDecompositionSearch,
) -> Result<(Option<String>, Vec<Table>, Option<VerificationReport>)> {
    if let Some(projection) = &search.exact_projection {
        let factors = vec![
            projection.left_factor.clone(),
            projection.right_factor.clone(),
        ];
        let merged = merge_tables_fast_from_slices(
            &projection.left_factor.bits,
            &projection.left_factor.rows,
            &projection.right_factor.bits,
            &projection.right_factor.rows,
        )
        .map_err(anyhow::Error::msg)?;
        let verification = VerificationReport {
            merged_bits: merged.bits.clone(),
            merged_row_count: merged.rows.len(),
            reconstructed_bits: merged.bits.clone(),
            reconstructed_row_count: merged.rows.len(),
            matches_canonical_original: merged.bits == table.bits && merged.rows == table.rows,
        };
        return Ok((
            Some("exact_projection".to_string()),
            factors,
            Some(verification),
        ));
    }

    if let Some(latent) = &search.exact_latent_biclique {
        if latent.arity_reducing {
            let factors = vec![latent.left_factor.clone(), latent.right_factor.clone()];
            let merged = merge_tables_fast_from_slices(
                &latent.left_factor.bits,
                &latent.left_factor.rows,
                &latent.right_factor.bits,
                &latent.right_factor.rows,
            )
            .map_err(anyhow::Error::msg)?;
            let reconstructed = project_away_bits(
                &Table {
                    bits: merged.bits.clone(),
                    rows: merged.rows.clone(),
                },
                &latent.latent_bits,
            )?;
            let verification = VerificationReport {
                merged_bits: merged.bits,
                merged_row_count: merged.rows.len(),
                reconstructed_bits: reconstructed.bits.clone(),
                reconstructed_row_count: reconstructed.rows.len(),
                matches_canonical_original: reconstructed.bits == table.bits
                    && reconstructed.rows == table.rows,
            };
            return Ok((
                Some("exact_latent_biclique".to_string()),
                factors,
                Some(verification),
            ));
        }
    }

    Ok((None, Vec::new(), None))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin table_decompose -- --input <path> --table-index <n> [--output <path>] [--report <path>] [--max-small-side-bits <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
