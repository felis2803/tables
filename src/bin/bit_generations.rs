use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{collect_bits, project_row, read_tables, write_json, Table};
use tables::tables_file::{has_tables_extension, read_tables_bundle, OriginArray, StoredTable};

struct Args {
    input: PathBuf,
    origins: Option<PathBuf>,
    output_root: PathBuf,
    summary_output: Option<PathBuf>,
    report_output: Option<PathBuf>,
    generations_output: Option<PathBuf>,
    generation_by_bit_output: Option<PathBuf>,
    unreachable_bits_output: Option<PathBuf>,
    constant_bits_output: Option<PathBuf>,
    max_generations: usize,
    until_fixed_point: bool,
    origin_name: String,
    require_full_domain: bool,
    allow_empty_determining_set: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            origins: Some(PathBuf::from("data/raw/origins.json")),
            output_root: PathBuf::from("data/derived/generation_chain.raw"),
            summary_output: None,
            report_output: None,
            generations_output: None,
            generation_by_bit_output: None,
            unreachable_bits_output: None,
            constant_bits_output: None,
            max_generations: 10,
            until_fixed_point: false,
            origin_name: "origins".to_string(),
            require_full_domain: false,
            allow_empty_determining_set: false,
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
                "--origins" => {
                    args.origins = Some(PathBuf::from(expect_value(&mut iter, "--origins")?))
                }
                "--no-origins-file" => args.origins = None,
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--summary-output" => {
                    args.summary_output =
                        Some(PathBuf::from(expect_value(&mut iter, "--summary-output")?))
                }
                "--report-output" => {
                    args.report_output =
                        Some(PathBuf::from(expect_value(&mut iter, "--report-output")?))
                }
                "--generations-output" => {
                    args.generations_output = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--generations-output",
                    )?))
                }
                "--generation-by-bit-output" => {
                    args.generation_by_bit_output = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--generation-by-bit-output",
                    )?))
                }
                "--unreachable-bits-output" => {
                    args.unreachable_bits_output = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--unreachable-bits-output",
                    )?))
                }
                "--constant-bits-output" => {
                    args.constant_bits_output = Some(PathBuf::from(expect_value(
                        &mut iter,
                        "--constant-bits-output",
                    )?))
                }
                "--max-generations" => {
                    args.max_generations = expect_value(&mut iter, "--max-generations")?
                        .parse()
                        .map_err(|_| anyhow!("invalid value for --max-generations"))?
                }
                "--until-fixed-point" => args.until_fixed_point = true,
                "--origin-name" => args.origin_name = expect_value(&mut iter, "--origin-name")?,
                "--require-full-domain" => args.require_full_domain = true,
                "--allow-empty-determining-set" => args.allow_empty_determining_set = true,
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

#[derive(Clone, Debug)]
struct CandidateRule {
    determining_bits: Vec<u32>,
    source_table_index: usize,
    source_arity: usize,
    source_row_count: usize,
    full_determining_domain: bool,
}

#[derive(Clone, Debug, Serialize)]
struct BitWitnessRecord {
    bit: u32,
    determining_bits: Vec<u32>,
    determining_generations: Vec<usize>,
    determining_arity: usize,
    source_table_index: usize,
    source_table_bits: Vec<u32>,
    source_row_count: usize,
    full_determining_domain: bool,
}

#[derive(Clone, Debug, Serialize)]
struct GenerationRecord {
    generation: usize,
    bit_count: usize,
    bits: Vec<u32>,
    witnesses: Vec<BitWitnessRecord>,
}

#[derive(Serialize)]
struct ReachableBitRecord {
    bit: u32,
    generation: usize,
}

#[derive(Serialize)]
struct Summary {
    input: String,
    origins: Option<String>,
    origin_name: String,
    source_table_count: usize,
    unique_bit_count: usize,
    origin_count: usize,
    constant_bit_count: usize,
    constant_bits_sample: Vec<u32>,
    bits_with_dependency_rules: usize,
    dependency_rule_count: usize,
    require_full_domain: bool,
    allow_empty_determining_set: bool,
    until_fixed_point: bool,
    requested_generations: usize,
    computed_generation_count: usize,
    last_non_empty_generation: usize,
    fixed_point_reached: bool,
    non_empty_generations: usize,
    reachable_bit_count: usize,
    unresolved_bit_count: usize,
    generation_sizes: BTreeMap<String, usize>,
}

#[derive(Serialize)]
struct Report {
    summary: Summary,
    generations: Vec<GenerationRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let summary_path = args
        .summary_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("summary.json"));
    let report_path = args
        .report_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("report.json"));
    let generations_path = args
        .generations_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("generations.json"));
    let generation_by_bit_path = args
        .generation_by_bit_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("generation_by_bit.json"));
    let unreachable_bits_path = args
        .unreachable_bits_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("unreachable_bits.json"));
    let constant_bits_path = args
        .constant_bits_output
        .clone()
        .unwrap_or_else(|| args.output_root.join("constant_bits.json"));

    let (tables, origins) = load_tables_and_origins(&args)?;
    let rules_by_bit = collect_dependency_rules(
        &tables,
        args.require_full_domain,
        args.allow_empty_determining_set,
    );
    let constant_bits = collect_constant_bits(&tables);
    let report = build_report(&args, &tables, &origins, &rules_by_bit, &constant_bits);
    let generation_by_bit = build_generation_by_bit(&report);
    let unreachable_bits = build_unreachable_bits(&tables, &origins, &report);

    write_json(&summary_path, &report.summary)?;
    write_json(&report_path, &report)?;
    write_json(&generations_path, &report.generations)?;
    write_json(&generation_by_bit_path, &generation_by_bit)?;
    write_json(&unreachable_bits_path, &unreachable_bits)?;
    write_json(&constant_bits_path, &constant_bits)?;

    println!("source_tables: {}", report.summary.source_table_count);
    println!("unique_bits: {}", report.summary.unique_bit_count);
    println!("origins: {}", report.summary.origin_count);
    println!(
        "bits_with_dependency_rules: {}",
        report.summary.bits_with_dependency_rules
    );
    println!("dependency_rules: {}", report.summary.dependency_rule_count);
    println!("reachable_bits: {}", report.summary.reachable_bit_count);
    println!("unresolved_bits: {}", report.summary.unresolved_bit_count);
    for generation in &report.generations {
        println!(
            "generation {}: {}",
            generation.generation, generation.bit_count
        );
    }
    println!("summary: {}", summary_path.display());
    println!("report: {}", report_path.display());
    println!("generations: {}", generations_path.display());
    println!("generation_by_bit: {}", generation_by_bit_path.display());
    println!("unreachable_bits: {}", unreachable_bits_path.display());
    println!("constant_bits: {}", constant_bits_path.display());
    Ok(())
}

fn load_tables_and_origins(args: &Args) -> Result<(Vec<Table>, Vec<u32>)> {
    if has_tables_extension(&args.input) {
        let bundle = read_tables_bundle(&args.input)?;
        let tables: Vec<Table> = bundle
            .tables
            .into_iter()
            .map(StoredTable::try_into_table)
            .collect::<Result<_>>()?;
        let mut origins = match &args.origins {
            Some(path) => read_u32_json_array(path)?,
            None => select_origin_array(&bundle.origin_arrays, &args.origin_name)?
                .values
                .clone(),
        };
        origins.sort_unstable();
        origins.dedup();
        return Ok((tables, origins));
    }

    let tables = read_tables(&args.input)?;
    let origins_path = args.origins.as_ref().ok_or_else(|| {
        anyhow!(
            "--origins is required for non-.tables input {}; use --no-origins-file only with .tables input",
            args.input.display()
        )
    })?;
    let mut origins = read_u32_json_array(origins_path)?;
    origins.sort_unstable();
    origins.dedup();
    Ok((tables, origins))
}

fn select_origin_array<'a>(
    origin_arrays: &'a [OriginArray],
    name: &str,
) -> Result<&'a OriginArray> {
    if let Some(found) = origin_arrays
        .iter()
        .find(|origin_array| origin_array.name == name)
    {
        return Ok(found);
    }
    if name == "origins" && origin_arrays.len() == 1 {
        return Ok(&origin_arrays[0]);
    }
    bail!("origin array {name:?} not found in input bundle")
}

fn read_u32_json_array(path: &Path) -> Result<Vec<u32>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let values = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(values)
}

fn collect_dependency_rules(
    tables: &[Table],
    require_full_domain: bool,
    allow_empty_determining_set: bool,
) -> BTreeMap<u32, Vec<CandidateRule>> {
    let mut rules_by_bit: BTreeMap<u32, Vec<CandidateRule>> = BTreeMap::new();

    for (source_table_index, table) in tables.iter().enumerate() {
        if table.bits.is_empty() {
            continue;
        }

        let mask_count = 1usize << table.bits.len();
        let mut indices_by_mask = vec![Vec::new(); mask_count];
        for mask in 0..mask_count {
            for bit_index in 0..table.bits.len() {
                if ((mask >> bit_index) & 1) != 0 {
                    indices_by_mask[mask].push(bit_index);
                }
            }
        }

        for target_index in 0..table.bits.len() {
            let target_bit = table.bits[target_index];
            let minimal_masks = find_minimal_determining_masks(
                table,
                target_index,
                &indices_by_mask,
                require_full_domain,
                allow_empty_determining_set,
            );
            if minimal_masks.is_empty() {
                continue;
            }

            let candidates = rules_by_bit.entry(target_bit).or_default();
            for mask in minimal_masks {
                insert_candidate_rule(
                    candidates,
                    CandidateRule {
                        determining_bits: indices_by_mask[mask]
                            .iter()
                            .map(|&index| table.bits[index])
                            .collect(),
                        source_table_index,
                        source_arity: table.bits.len(),
                        source_row_count: table.rows.len(),
                        full_determining_domain: determining_mask_is_full_domain(
                            table,
                            target_index,
                            &indices_by_mask[mask],
                        ),
                    },
                );
            }
        }
    }

    for rules in rules_by_bit.values_mut() {
        rules.sort_by(compare_candidate_rules);
    }

    rules_by_bit
}

fn collect_constant_bits(tables: &[Table]) -> Vec<u32> {
    let mut constants = BTreeSet::new();

    for table in tables {
        for target_index in 0..table.bits.len() {
            if dependency_holds(table, target_index, &[], false).is_some() {
                constants.insert(table.bits[target_index]);
            }
        }
    }

    constants.into_iter().collect()
}

fn find_minimal_determining_masks(
    table: &Table,
    target_index: usize,
    indices_by_mask: &[Vec<usize>],
    require_full_domain: bool,
    allow_empty_determining_set: bool,
) -> Vec<usize> {
    let mut masks: Vec<usize> = (0..indices_by_mask.len())
        .filter(|mask| ((mask >> target_index) & 1) == 0)
        .filter(|mask| allow_empty_determining_set || *mask != 0)
        .collect();
    masks.sort_by_key(|mask| (mask.count_ones(), *mask));

    let mut minimal_masks = Vec::new();
    for mask in masks {
        if minimal_masks
            .iter()
            .any(|&existing_mask| (existing_mask & mask) == existing_mask)
        {
            continue;
        }

        if dependency_holds(
            table,
            target_index,
            &indices_by_mask[mask],
            require_full_domain,
        )
        .is_some()
        {
            minimal_masks.push(mask);
        }
    }

    minimal_masks
}

fn dependency_holds(
    table: &Table,
    target_index: usize,
    determining_indices: &[usize],
    require_full_domain: bool,
) -> Option<bool> {
    let domain_size = 1usize << determining_indices.len();
    let mut assignments = vec![u8::MAX; domain_size];
    let mut assignment_count = 0usize;

    for &row in &table.rows {
        let key = project_row(row, determining_indices) as usize;
        let value = ((row >> target_index) & 1) as u8;
        match assignments[key] {
            existing if existing == u8::MAX => {
                assignments[key] = value;
                assignment_count += 1;
            }
            existing if existing == value => {}
            _ => return None,
        }
    }

    let full_domain = assignment_count == domain_size;
    if require_full_domain && !full_domain {
        return None;
    }
    Some(full_domain)
}

fn determining_mask_is_full_domain(
    table: &Table,
    target_index: usize,
    determining_indices: &[usize],
) -> bool {
    dependency_holds(table, target_index, determining_indices, false).unwrap_or(false)
}

fn insert_candidate_rule(candidates: &mut Vec<CandidateRule>, candidate: CandidateRule) {
    if let Some(existing) = candidates
        .iter_mut()
        .find(|existing| existing.determining_bits == candidate.determining_bits)
    {
        if compare_candidate_rules(&candidate, existing) == Ordering::Less {
            *existing = candidate;
        }
        return;
    }
    candidates.push(candidate);
}

fn compare_candidate_rules(left: &CandidateRule, right: &CandidateRule) -> Ordering {
    left.determining_bits
        .len()
        .cmp(&right.determining_bits.len())
        .then_with(|| {
            right
                .full_determining_domain
                .cmp(&left.full_determining_domain)
        })
        .then_with(|| left.source_arity.cmp(&right.source_arity))
        .then_with(|| left.source_row_count.cmp(&right.source_row_count))
        .then_with(|| left.source_table_index.cmp(&right.source_table_index))
        .then_with(|| left.determining_bits.cmp(&right.determining_bits))
}

fn build_report(
    args: &Args,
    tables: &[Table],
    origins: &[u32],
    rules_by_bit: &BTreeMap<u32, Vec<CandidateRule>>,
    constant_bits: &[u32],
) -> Report {
    let mut all_bits = collect_bits(tables);
    all_bits.extend_from_slice(origins);
    all_bits.sort_unstable();
    all_bits.dedup();

    let mut known_bits: BTreeSet<u32> = origins.iter().copied().collect();
    let mut generation_by_bit: BTreeMap<u32, usize> =
        origins.iter().copied().map(|bit| (bit, 0usize)).collect();
    let generation_capacity = if args.until_fixed_point {
        16
    } else {
        args.max_generations.saturating_add(1)
    };
    let mut generations = Vec::with_capacity(generation_capacity);
    let mut generation_sizes = BTreeMap::new();
    let mut fixed_point_reached = known_bits.len() == all_bits.len();

    let generation_zero = GenerationRecord {
        generation: 0,
        bit_count: origins.len(),
        bits: origins.to_vec(),
        witnesses: Vec::new(),
    };
    generation_sizes.insert("0".to_string(), generation_zero.bit_count);
    generations.push(generation_zero);

    let mut generation_index = 1usize;
    loop {
        if !args.until_fixed_point && generation_index > args.max_generations {
            break;
        }

        let mut next = Vec::new();

        for &bit in &all_bits {
            if known_bits.contains(&bit) {
                continue;
            }

            let Some(candidate_rules) = rules_by_bit.get(&bit) else {
                continue;
            };
            let Some(rule) = candidate_rules
                .iter()
                .filter(|rule| {
                    rule.determining_bits
                        .iter()
                        .all(|determining_bit| known_bits.contains(determining_bit))
                })
                .min_by(|left, right| compare_candidate_rules(left, right))
            else {
                continue;
            };

            let determining_generations: Vec<usize> = rule
                .determining_bits
                .iter()
                .map(|determining_bit| generation_by_bit[determining_bit])
                .collect();
            next.push(BitWitnessRecord {
                bit,
                determining_bits: rule.determining_bits.clone(),
                determining_generations,
                determining_arity: rule.determining_bits.len(),
                source_table_index: rule.source_table_index,
                source_table_bits: tables[rule.source_table_index].bits.clone(),
                source_row_count: rule.source_row_count,
                full_determining_domain: rule.full_determining_domain,
            });
        }

        if args.until_fixed_point && next.is_empty() {
            fixed_point_reached = true;
            break;
        }

        next.sort_by(|left, right| left.bit.cmp(&right.bit));
        for record in &next {
            known_bits.insert(record.bit);
            generation_by_bit.insert(record.bit, generation_index);
        }

        let bits: Vec<u32> = next.iter().map(|record| record.bit).collect();
        generation_sizes.insert(generation_index.to_string(), bits.len());
        generations.push(GenerationRecord {
            generation: generation_index,
            bit_count: bits.len(),
            bits,
            witnesses: next,
        });

        if known_bits.len() == all_bits.len() {
            fixed_point_reached = true;
            break;
        }

        generation_index += 1;
    }

    let dependency_rule_count: usize = rules_by_bit.values().map(Vec::len).sum();
    let non_empty_generations = generations
        .iter()
        .skip(1)
        .filter(|generation| generation.bit_count > 0)
        .count();
    let last_non_empty_generation = generations
        .iter()
        .rev()
        .find(|generation| generation.bit_count > 0)
        .map(|generation| generation.generation)
        .unwrap_or(0);
    let reachable_bit_count = known_bits.len();
    let unresolved_bit_count = all_bits.len().saturating_sub(reachable_bit_count);

    let summary = Summary {
        input: args.input.display().to_string(),
        origins: args.origins.as_ref().map(|path| path.display().to_string()),
        origin_name: args.origin_name.clone(),
        source_table_count: tables.len(),
        unique_bit_count: all_bits.len(),
        origin_count: origins.len(),
        constant_bit_count: constant_bits.len(),
        constant_bits_sample: constant_bits.iter().copied().take(32).collect(),
        bits_with_dependency_rules: rules_by_bit.len(),
        dependency_rule_count,
        require_full_domain: args.require_full_domain,
        allow_empty_determining_set: args.allow_empty_determining_set,
        until_fixed_point: args.until_fixed_point,
        requested_generations: args.max_generations,
        computed_generation_count: generations.len(),
        last_non_empty_generation,
        fixed_point_reached,
        non_empty_generations,
        reachable_bit_count,
        unresolved_bit_count,
        generation_sizes,
    };

    Report {
        summary,
        generations,
    }
}

fn build_generation_by_bit(report: &Report) -> Vec<ReachableBitRecord> {
    let mut records = Vec::new();

    for generation in &report.generations {
        for &bit in &generation.bits {
            records.push(ReachableBitRecord {
                bit,
                generation: generation.generation,
            });
        }
    }

    records.sort_by(|left, right| left.bit.cmp(&right.bit));
    records
}

fn build_unreachable_bits(tables: &[Table], origins: &[u32], report: &Report) -> Vec<u32> {
    let mut all_bits = collect_bits(tables);
    all_bits.extend_from_slice(origins);
    all_bits.sort_unstable();
    all_bits.dedup();

    let reachable: BTreeSet<u32> = report
        .generations
        .iter()
        .flat_map(|generation| generation.bits.iter().copied())
        .collect();

    all_bits
        .into_iter()
        .filter(|bit| !reachable.contains(bit))
        .collect()
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin bit_generations -- [--input <tables.json|system.tables>] [--origins <origins.json> | --no-origins-file] [--origin-name <name>] [--output-root <dir>] [--summary-output <path>] [--report-output <path>] [--generations-output <path>] [--generation-by-bit-output <path>] [--unreachable-bits-output <path>] [--constant-bits-output <path>] [--max-generations <n> | --until-fixed-point] [--require-full-domain] [--allow-empty-determining-set]"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        build_report, collect_constant_bits, collect_dependency_rules, dependency_holds, Args,
        BitWitnessRecord, GenerationRecord,
    };
    use std::path::PathBuf;
    use tables::common::Table;

    fn test_args() -> Args {
        Args {
            input: PathBuf::from("input.json"),
            origins: Some(PathBuf::from("origins.json")),
            output_root: PathBuf::from("out"),
            summary_output: None,
            report_output: None,
            generations_output: None,
            generation_by_bit_output: None,
            unreachable_bits_output: None,
            constant_bits_output: None,
            max_generations: 4,
            until_fixed_point: false,
            origin_name: "origins".to_string(),
            require_full_domain: false,
            allow_empty_determining_set: false,
        }
    }

    fn generations(report: &super::Report) -> &[GenerationRecord] {
        &report.generations
    }

    fn witnesses(report: &super::Report, generation: usize) -> &[BitWitnessRecord] {
        &report.generations[generation].witnesses
    }

    #[test]
    fn dependency_holds_for_single_bit_rule() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0, 3],
        };

        assert_eq!(dependency_holds(&table, 1, &[0], false), Some(true));
    }

    #[test]
    fn dependency_rejects_conflict() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0, 1, 2],
        };

        assert_eq!(dependency_holds(&table, 1, &[0], false), None);
    }

    #[test]
    fn dependency_can_be_constant_on_empty_determining_set() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0, 1],
        };

        assert_eq!(dependency_holds(&table, 1, &[], false), Some(true));
    }

    #[test]
    fn dependency_requires_full_domain_when_requested() {
        let table = Table {
            bits: vec![10, 20],
            rows: vec![0],
        };

        assert_eq!(dependency_holds(&table, 1, &[0], false), Some(false));
        assert_eq!(dependency_holds(&table, 1, &[0], true), None);
    }

    #[test]
    fn collect_dependency_rules_keeps_minimal_rule() {
        let table = Table {
            bits: vec![10, 20, 30],
            rows: vec![0b000, 0b001, 0b110, 0b111],
        };

        let rules = collect_dependency_rules(&[table], false, false);
        let target_rules = rules.get(&30).expect("missing rules for target bit");

        assert_eq!(target_rules.len(), 1);
        assert_eq!(target_rules[0].determining_bits, vec![20]);
    }

    #[test]
    fn build_report_chains_generations() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0, 3],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0, 3],
            },
        ];
        let origins = vec![1];
        let args = test_args();
        let rules = collect_dependency_rules(&tables, false, false);
        let constants = collect_constant_bits(&tables);
        let report = build_report(&args, &tables, &origins, &rules, &constants);

        assert_eq!(generations(&report)[0].bits, vec![1]);
        assert_eq!(generations(&report)[1].bits, vec![2]);
        assert_eq!(generations(&report)[2].bits, vec![3]);
        assert!(witnesses(&report, 1)[0].determining_bits == vec![1]);
        assert!(witnesses(&report, 2)[0].determining_bits == vec![2]);
    }

    #[test]
    fn build_report_preserves_empty_generations_after_stall() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0, 3],
            },
            Table {
                bits: vec![4],
                rows: vec![0, 1],
            },
        ];
        let origins = vec![1];
        let mut args = test_args();
        args.max_generations = 3;
        let rules = collect_dependency_rules(&tables, false, false);
        let constants = collect_constant_bits(&tables);
        let report = build_report(&args, &tables, &origins, &rules, &constants);

        assert_eq!(generations(&report)[1].bits, vec![2]);
        assert!(generations(&report)[2].bits.is_empty());
        assert!(generations(&report)[3].bits.is_empty());
    }

    #[test]
    fn build_report_counts_unresolved_bits() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0, 3],
            },
            Table {
                bits: vec![4],
                rows: vec![0, 1],
            },
        ];
        let origins = vec![1];
        let args = test_args();
        let rules = collect_dependency_rules(&tables, false, false);
        let constants = collect_constant_bits(&tables);
        let report = build_report(&args, &tables, &origins, &rules, &constants);

        assert_eq!(report.summary.reachable_bit_count, 2);
        assert_eq!(report.summary.unresolved_bit_count, 1);
    }

    #[test]
    fn build_report_until_fixed_point_stops_without_empty_tail() {
        let tables = vec![Table {
            bits: vec![1, 2],
            rows: vec![0, 3],
        }];
        let origins = vec![1];
        let mut args = test_args();
        args.until_fixed_point = true;
        let rules = collect_dependency_rules(&tables, false, false);
        let constants = collect_constant_bits(&tables);
        let report = build_report(&args, &tables, &origins, &rules, &constants);

        assert_eq!(generations(&report).len(), 2);
        assert_eq!(report.summary.last_non_empty_generation, 1);
        assert!(report.summary.fixed_point_reached);
    }

    #[test]
    fn build_unreachable_bits_lists_remaining_bits() {
        let tables = vec![
            Table {
                bits: vec![1, 2],
                rows: vec![0, 3],
            },
            Table {
                bits: vec![4],
                rows: vec![0, 1],
            },
        ];
        let origins = vec![1];
        let args = test_args();
        let rules = collect_dependency_rules(&tables, false, false);
        let constants = collect_constant_bits(&tables);
        let report = build_report(&args, &tables, &origins, &rules, &constants);
        let unreachable = super::build_unreachable_bits(&tables, &origins, &report);

        assert_eq!(unreachable, vec![4]);
    }
}
