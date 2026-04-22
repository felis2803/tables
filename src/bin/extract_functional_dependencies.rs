use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use tables::common::{project_row, sort_dedup_rows, write_json, Table};
use tables::tables_file::{read_tables_bundle, write_tables_bundle, StoredTable, TablesBundle};

struct Args {
    input: PathBuf,
    output_root: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("runs/2026-04-12-originals-max-arity/originals.max_arity.tables"),
            output_root: PathBuf::from(
                "runs/2026-04-12-originals-max-arity/functional_dependencies",
            ),
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
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
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

#[derive(Clone, Copy, Debug)]
struct DependencyStats {
    determining_assignment_count: usize,
    full_determining_domain: bool,
}

#[derive(Serialize)]
struct RuleRecord {
    global_rule_index: usize,
    projected_table_index: usize,
    union_size: usize,
    determined_size: usize,
    determining_size: usize,
    union_bits: Vec<u32>,
    determined_bits: Vec<u32>,
    determining_bits: Vec<u32>,
    projected_row_count: usize,
    determining_assignment_count: usize,
    determining_domain_size: usize,
    full_determining_domain: bool,
}

#[derive(Serialize)]
struct TableRules {
    source_table_index: usize,
    source_table_bits: Vec<u32>,
    source_row_count: usize,
    rule_count: usize,
    rules: Vec<RuleRecord>,
}

#[derive(Serialize)]
struct RulesArtifact {
    input: String,
    projected_tables_output: String,
    source_table_count: usize,
    total_rule_count: usize,
    tables: Vec<TableRules>,
}

#[derive(Serialize)]
struct Summary {
    input: String,
    source_table_count: usize,
    source_origin_array_count: usize,
    total_rule_count: usize,
    projected_table_count: usize,
    tables_with_rules: usize,
    tables_without_rules: usize,
    rule_count_by_union_size: BTreeMap<String, usize>,
    rule_count_by_partition: BTreeMap<String, usize>,
    full_determining_domain_rule_count: usize,
    table_rule_count_distribution: BTreeMap<String, usize>,
    max_rule_count_per_table: usize,
    min_rule_count_per_table: usize,
    mean_rule_count_per_table: f64,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let rules_path = args.output_root.join("rules.json");
    let summary_path = args.output_root.join("summary.json");
    let subtables_path = args.output_root.join("subtables.tables");

    let input_bundle = read_tables_bundle(&args.input)?;
    let source_tables: Vec<Table> = input_bundle
        .tables
        .into_iter()
        .map(StoredTable::try_into_table)
        .collect::<Result<_>>()?;

    let mut grouped_rules = Vec::with_capacity(source_tables.len());
    let mut projected_tables = Vec::new();
    let mut global_rule_index = 0usize;
    let mut rule_count_by_union_size: BTreeMap<String, usize> = BTreeMap::new();
    let mut rule_count_by_partition: BTreeMap<String, usize> = BTreeMap::new();
    let mut table_rule_count_distribution: BTreeMap<String, usize> = BTreeMap::new();
    let mut tables_with_rules = 0usize;
    let mut full_determining_domain_rule_count = 0usize;
    let mut max_rule_count_per_table = 0usize;
    let mut min_rule_count_per_table = usize::MAX;
    let mut total_rule_count_for_mean = 0usize;

    for (source_table_index, table) in source_tables.iter().enumerate() {
        let (rules, subtables) = extract_rules_for_table(
            table,
            source_table_index,
            &mut global_rule_index,
            &mut full_determining_domain_rule_count,
            &mut rule_count_by_union_size,
            &mut rule_count_by_partition,
        )?;

        let rule_count = rules.len();
        if rule_count > 0 {
            tables_with_rules += 1;
        }
        max_rule_count_per_table = max_rule_count_per_table.max(rule_count);
        min_rule_count_per_table = min_rule_count_per_table.min(rule_count);
        total_rule_count_for_mean += rule_count;
        *table_rule_count_distribution
            .entry(rule_count.to_string())
            .or_insert(0) += 1;

        projected_tables.extend(
            subtables
                .into_iter()
                .map(|table| StoredTable::from_table(&table)),
        );
        grouped_rules.push(TableRules {
            source_table_index,
            source_table_bits: table.bits.clone(),
            source_row_count: table.rows.len(),
            rule_count,
            rules,
        });
    }

    if min_rule_count_per_table == usize::MAX {
        min_rule_count_per_table = 0;
    }

    let output_bundle = TablesBundle {
        origin_arrays: input_bundle.origin_arrays,
        tables: projected_tables,
    };
    write_tables_bundle(&subtables_path, &output_bundle)?;

    let total_rule_count = global_rule_index;
    let rules_artifact = RulesArtifact {
        input: args.input.display().to_string(),
        projected_tables_output: subtables_path.display().to_string(),
        source_table_count: source_tables.len(),
        total_rule_count,
        tables: grouped_rules,
    };
    write_json(&rules_path, &rules_artifact)?;

    let summary = Summary {
        input: args.input.display().to_string(),
        source_table_count: source_tables.len(),
        source_origin_array_count: output_bundle.origin_arrays.len(),
        total_rule_count,
        projected_table_count: output_bundle.tables.len(),
        tables_with_rules,
        tables_without_rules: source_tables.len().saturating_sub(tables_with_rules),
        rule_count_by_union_size,
        rule_count_by_partition,
        full_determining_domain_rule_count,
        table_rule_count_distribution,
        max_rule_count_per_table,
        min_rule_count_per_table,
        mean_rule_count_per_table: if source_tables.is_empty() {
            0.0
        } else {
            total_rule_count_for_mean as f64 / source_tables.len() as f64
        },
    };
    write_json(&summary_path, &summary)?;

    println!("source_tables: {}", summary.source_table_count);
    println!("total_rules: {}", summary.total_rule_count);
    println!("projected_tables: {}", summary.projected_table_count);
    println!("tables_with_rules: {}", summary.tables_with_rules);
    println!(
        "full_determining_domain_rules: {}",
        summary.full_determining_domain_rule_count
    );
    println!("rules: {}", rules_path.display());
    println!("summary: {}", summary_path.display());
    println!("subtables: {}", subtables_path.display());
    Ok(())
}

fn extract_rules_for_table(
    table: &Table,
    _source_table_index: usize,
    global_rule_index: &mut usize,
    full_determining_domain_rule_count: &mut usize,
    rule_count_by_union_size: &mut BTreeMap<String, usize>,
    rule_count_by_partition: &mut BTreeMap<String, usize>,
) -> Result<(Vec<RuleRecord>, Vec<Table>)> {
    let arity = table.bits.len();
    if arity < 2 {
        return Ok((Vec::new(), Vec::new()));
    }
    if arity >= usize::BITS as usize {
        bail!(
            "extract_functional_dependencies requires table arity below {}, got {}",
            usize::BITS,
            arity
        );
    }

    let mask_count = 1usize << arity;
    let mut indices_by_mask = vec![Vec::new(); mask_count];
    let mut bits_by_mask = vec![Vec::new(); mask_count];
    for mask in 0..mask_count {
        for bit_index in 0..arity {
            if ((mask >> bit_index) & 1) != 0 {
                indices_by_mask[mask].push(bit_index);
                bits_by_mask[mask].push(table.bits[bit_index]);
            }
        }
    }

    let mut projected_by_union_mask: Vec<Option<Table>> = vec![None; mask_count];
    let mut rules = Vec::new();
    let mut subtables = Vec::new();

    for union_mask in 1..mask_count {
        if union_mask.count_ones() < 2 {
            continue;
        }

        let mut determining_mask = union_mask;
        while determining_mask > 0 {
            determining_mask = (determining_mask - 1) & union_mask;
            if determining_mask == 0 {
                break;
            }
            let determined_mask = union_mask ^ determining_mask;
            if determined_mask == 0 {
                continue;
            }

            let Some(stats) = detect_subset_dependency(
                table,
                &indices_by_mask[determined_mask],
                &indices_by_mask[determining_mask],
            ) else {
                continue;
            };

            if stats.full_determining_domain {
                *full_determining_domain_rule_count += 1;
            }

            let union_size = bits_by_mask[union_mask].len();
            let determined_size = bits_by_mask[determined_mask].len();
            let determining_size = bits_by_mask[determining_mask].len();
            *rule_count_by_union_size
                .entry(union_size.to_string())
                .or_insert(0) += 1;
            *rule_count_by_partition
                .entry(format!("{determined_size}<-{determining_size}"))
                .or_insert(0) += 1;

            if projected_by_union_mask[union_mask].is_none() {
                projected_by_union_mask[union_mask] = Some(project_table(
                    table,
                    &indices_by_mask[union_mask],
                    &bits_by_mask[union_mask],
                ));
            }
            let projected = projected_by_union_mask[union_mask]
                .as_ref()
                .expect("projected table must exist")
                .clone();

            let projected_table_index = *global_rule_index;
            let determining_domain_size = 1usize << determining_size;
            rules.push(RuleRecord {
                global_rule_index: *global_rule_index,
                projected_table_index,
                union_size,
                determined_size,
                determining_size,
                union_bits: bits_by_mask[union_mask].clone(),
                determined_bits: bits_by_mask[determined_mask].clone(),
                determining_bits: bits_by_mask[determining_mask].clone(),
                projected_row_count: projected.rows.len(),
                determining_assignment_count: stats.determining_assignment_count,
                determining_domain_size,
                full_determining_domain: stats.full_determining_domain,
            });
            subtables.push(projected);
            *global_rule_index += 1;
        }
    }

    Ok((rules, subtables))
}

fn detect_subset_dependency(
    table: &Table,
    determined_indices: &[usize],
    determining_indices: &[usize],
) -> Option<DependencyStats> {
    let mut assignments: BTreeMap<u32, u32> = BTreeMap::new();
    for &row in &table.rows {
        let key = project_row(row, determining_indices);
        let value = project_row(row, determined_indices);
        match assignments.get(&key) {
            Some(&existing) if existing != value => return None,
            Some(_) => {}
            None => {
                assignments.insert(key, value);
            }
        }
    }

    let determining_assignment_count = assignments.len();
    let full_determining_domain = determining_indices.len() < usize::BITS as usize
        && determining_assignment_count == (1usize << determining_indices.len());
    Some(DependencyStats {
        determining_assignment_count,
        full_determining_domain,
    })
}

fn project_table(table: &Table, kept_indices: &[usize], kept_bits: &[u32]) -> Table {
    let mut projected_rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, kept_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);
    Table {
        bits: kept_bits.to_vec(),
        rows: projected_rows,
    }
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin extract_functional_dependencies -- [--input <system.tables>] [--output-root <dir>]"
    );
}

#[cfg(test)]
mod tests {
    use super::{detect_subset_dependency, extract_rules_for_table};
    use tables::common::Table;

    #[test]
    fn detects_single_bit_dependency() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0, 3],
        };

        let dependency = detect_subset_dependency(&table, &[0], &[1]).unwrap();
        assert_eq!(dependency.determining_assignment_count, 2);
        assert!(dependency.full_determining_domain);
    }

    #[test]
    fn rejects_conflicting_dependency() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0, 1, 2],
        };

        assert!(detect_subset_dependency(&table, &[0], &[1]).is_none());
    }

    #[test]
    fn extracts_multibit_dependency() {
        let table = Table {
            bits: vec![10, 20, 30, 40],
            rows: vec![0b0000, 0b0110, 0b1001, 0b1111],
        };
        let mut global_rule_index = 0usize;
        let mut full_domain_count = 0usize;
        let mut by_union_size = Default::default();
        let mut by_partition = Default::default();

        let (rules, _) = extract_rules_for_table(
            &table,
            0,
            &mut global_rule_index,
            &mut full_domain_count,
            &mut by_union_size,
            &mut by_partition,
        )
        .unwrap();

        assert!(rules.iter().any(|rule| {
            rule.determined_bits == vec![10, 20] && rule.determining_bits == vec![30, 40]
        }));
    }

    #[test]
    fn rejects_tables_too_wide_for_subset_enumeration() {
        let table = Table {
            bits: (0..64).collect(),
            rows: vec![0],
        };
        let mut global_rule_index = 0usize;
        let mut full_domain_count = 0usize;
        let mut by_union_size = Default::default();
        let mut by_partition = Default::default();

        let error = extract_rules_for_table(
            &table,
            0,
            &mut global_rule_index,
            &mut full_domain_count,
            &mut by_union_size,
            &mut by_partition,
        )
        .err()
        .expect("expected wide-arity extraction to fail")
        .to_string();

        assert!(error.contains("requires table arity below"));
    }
}
