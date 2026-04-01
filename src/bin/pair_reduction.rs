#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tables::common::{collect_bits, read_tables, write_json, RewriteRow, Table};
use tables::pair_reduction::{
    build_rewrite_map, extract_relations, rewrite_tables, update_original_mapping,
    PairReductionInfo, PairReductionIterationInfo,
};

#[derive(Clone, Debug, Serialize)]
struct RelationRow {
    iteration: usize,
    left: u32,
    right: u32,
    relation: u8,
    support: usize,
    sources: Vec<usize>,
}

struct Args {
    input: PathBuf,
    mapping_input: PathBuf,
    output: PathBuf,
    mapping_output: PathBuf,
    relations_output: PathBuf,
    report: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/derived/tables.common_node_fixed_point.json"),
            mapping_input: PathBuf::from(
                "data/derived/bits.common_node_fixed_point.mapping_full.json",
            ),
            output: PathBuf::from("data/derived/tables.common_node_fixed_point.pair_reduced.json"),
            mapping_output: PathBuf::from(
                "data/derived/bits.common_node_fixed_point.mapping_full.pair_reduced.json",
            ),
            relations_output: PathBuf::from(
                "data/derived/pairs.common_node_fixed_point.pair_reduced.relations.json",
            ),
            report: PathBuf::from("data/reports/report.common_node_fixed_point.pair_reduced.json"),
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
                "--mapping-input" => {
                    args.mapping_input = PathBuf::from(expect_value(&mut iter, "--mapping-input")?)
                }
                "--output" => args.output = PathBuf::from(expect_value(&mut iter, "--output")?),
                "--mapping-output" => {
                    args.mapping_output =
                        PathBuf::from(expect_value(&mut iter, "--mapping-output")?)
                }
                "--relations-output" => {
                    args.relations_output =
                        PathBuf::from(expect_value(&mut iter, "--relations-output")?)
                }
                "--report" => args.report = PathBuf::from(expect_value(&mut iter, "--report")?),
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
    let original_mapping = read_full_mapping(&args.mapping_input)?;
    let (output_tables, updated_mapping, relation_rows, info) =
        run_pair_reduction_step(tables, original_mapping)?;

    let mut report = serde_json::to_value(&info).map_err(|error| anyhow!(error))?;
    let Value::Object(ref mut object) = report else {
        bail!("pair reduction stats did not serialize to an object");
    };
    object.insert(
        "method".to_string(),
        json!("iterative extraction of equal/opposite bit pairs from pair projections, parity union-find rewriting, and table canonicalization"),
    );
    object.insert("input".to_string(), json!(path_string(&args.input)));
    object.insert("output".to_string(), json!(path_string(&args.output)));
    object.insert("backend".to_string(), json!("rust"));

    write_json(&args.output, &output_tables)?;
    write_json(&args.mapping_output, &full_mapping_rows(&updated_mapping))?;
    write_json(&args.relations_output, &relation_rows)?;
    write_json(&args.report, &report)?;

    println!("iterations: {}", info.iterations.len());
    println!("final tables: {}", output_tables.len());
    println!("relations found: {}", relation_rows.len());
    println!("output: {}", args.output.display());
    println!("report: {}", args.report.display());

    Ok(())
}

fn run_pair_reduction_step(
    mut tables: Vec<Table>,
    mut original_mapping: BTreeMap<u32, (u32, u8)>,
) -> Result<(
    Vec<Table>,
    BTreeMap<u32, (u32, u8)>,
    Vec<RelationRow>,
    PairReductionInfo,
)> {
    let mut relation_rows = Vec::new();
    let mut iterations = Vec::new();
    let mut iteration_index = 1usize;

    loop {
        let relations = extract_relations(&tables)?;
        if relations.is_empty() {
            break;
        }

        let (rewrite_map, component_stats) = build_rewrite_map(&relations)?;
        original_mapping = update_original_mapping(&original_mapping, &rewrite_map);
        let (rewritten_tables, rewrite_stats) = rewrite_tables(&tables, &rewrite_map);

        relation_rows.extend(relations.iter().map(|relation| RelationRow {
            iteration: iteration_index,
            left: relation.left,
            right: relation.right,
            relation: relation.relation,
            support: relation.support,
            sources: relation.sources.clone(),
        }));

        iterations.push(PairReductionIterationInfo {
            iteration: iteration_index,
            relation_pair_count: relations.len(),
            bits_involved: component_stats.bits_involved,
            component_count: component_stats.component_count,
            replaced_bit_count: component_stats.replaced_bit_count,
            changed_tables: rewrite_stats.changed_tables,
            reduced_arity_tables: rewrite_stats.reduced_arity_tables,
            same_arity_changed_tables: rewrite_stats.same_arity_changed_tables,
            removed_rows: rewrite_stats.removed_rows,
            collapsed_duplicate_tables: rewrite_stats.collapsed_duplicate_tables,
            table_count_after_iteration: rewritten_tables.len(),
            bit_count_after_iteration: collect_bits(&rewritten_tables).len(),
        });

        tables = rewritten_tables;
        iteration_index += 1;
    }

    let info = PairReductionInfo {
        pair_relation_pairs_total: iterations.iter().map(|item| item.relation_pair_count).sum(),
        pair_replaced_bits_total: iterations.iter().map(|item| item.replaced_bit_count).sum(),
        iterations,
    };

    Ok((tables, original_mapping, relation_rows, info))
}

fn read_full_mapping(path: &Path) -> Result<BTreeMap<u32, (u32, u8)>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let rows: Vec<RewriteRow> = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let mut mapping = BTreeMap::new();
    for row in rows {
        mapping.insert(row.bit, (row.representative, u8::from(row.inverted)));
    }
    Ok(mapping)
}

fn full_mapping_rows(mapping: &BTreeMap<u32, (u32, u8)>) -> Vec<RewriteRow> {
    mapping
        .iter()
        .map(|(&bit, &(representative, inverted))| RewriteRow {
            bit,
            representative,
            inverted: inverted != 0,
        })
        .collect()
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin tables-pair-reduction -- --input <path> --mapping-input <path> --output <path> --mapping-output <path> --relations-output <path> --report <path>"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
