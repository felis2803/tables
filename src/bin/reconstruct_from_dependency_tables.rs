use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde::Serialize;
use tables::common::{write_json, Table};
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;
use tables::tables_file::{read_tables_bundle, write_tables_bundle, StoredTable, TablesBundle};

struct Args {
    sources: PathBuf,
    rules: PathBuf,
    factors: PathBuf,
    output_root: PathBuf,
    max_factor_arity: Option<usize>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            sources: PathBuf::from(
                "runs/2026-04-12-originals-max-arity/min_rank_partners/merged.tables",
            ),
            rules: PathBuf::from(
                "runs/2026-04-12-originals-max-arity/min_rank_partners/functional_dependencies/rules.json",
            ),
            factors: PathBuf::from(
                "runs/2026-04-12-originals-max-arity/min_rank_partners/functional_dependencies/subtables.tables",
            ),
            output_root: PathBuf::from(
                "runs/2026-04-12-originals-max-arity/min_rank_partners/reconstruction",
            ),
            max_factor_arity: None,
        }
    }
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self::default();
        let mut iter = env::args().skip(1);

        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--sources" => args.sources = PathBuf::from(expect_value(&mut iter, "--sources")?),
                "--rules" => args.rules = PathBuf::from(expect_value(&mut iter, "--rules")?),
                "--factors" => args.factors = PathBuf::from(expect_value(&mut iter, "--factors")?),
                "--output-root" => {
                    args.output_root = PathBuf::from(expect_value(&mut iter, "--output-root")?)
                }
                "--max-factor-arity" => {
                    args.max_factor_arity = Some(
                        expect_value(&mut iter, "--max-factor-arity")?
                            .parse()
                            .map_err(|_| anyhow!("invalid value for --max-factor-arity"))?,
                    )
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

#[derive(Deserialize)]
struct RulesArtifact {
    tables: Vec<TableRules>,
}

#[derive(Deserialize)]
struct TableRules {
    source_table_index: usize,
    rules: Vec<RuleRecord>,
}

#[derive(Deserialize)]
struct RuleRecord {
    global_rule_index: usize,
    projected_table_index: usize,
}

#[derive(Clone)]
struct UniqueFactor {
    local_factor_index: usize,
    table: Table,
    source_rule_indices: Vec<usize>,
    projected_table_indices: Vec<usize>,
}

#[derive(Clone)]
struct SearchState {
    table: Table,
    parent_state_index: Option<usize>,
    via_factor_local_index: usize,
    depth: usize,
}

#[derive(Serialize)]
struct PathStep {
    step: usize,
    path_table_index: usize,
    factor_local_index: usize,
    factor_bits: Vec<u32>,
    factor_row_count: usize,
    factor_source_rule_indices: Vec<usize>,
    factor_projected_table_indices: Vec<usize>,
    merged_bits: Vec<u32>,
    merged_row_count: usize,
}

#[derive(Serialize)]
struct ReconstructionRecord {
    source_table_index: usize,
    source_bits: Vec<u32>,
    source_row_count: usize,
    total_rule_count: usize,
    unique_factor_count: usize,
    proper_factor_count: usize,
    source_arity_factor_count: usize,
    exact_source_factor_count: usize,
    reachable_state_count: usize,
    found: bool,
    final_bits: Vec<u32>,
    final_row_count: usize,
    matches_source: bool,
    step_count: usize,
    arity_sequence: Vec<usize>,
    path: Vec<PathStep>,
}

#[derive(Serialize)]
struct Summary {
    sources: String,
    rules: String,
    factors: String,
    source_table_count: usize,
    reconstructed_count: usize,
    failed_count: usize,
    max_steps: usize,
    min_steps: usize,
    mean_steps: f64,
    final_arity_distribution: BTreeMap<String, usize>,
    step_count_distribution: BTreeMap<String, usize>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let report_path = args.output_root.join("report.json");
    let summary_path = args.output_root.join("summary.json");
    let reconstructed_path = args.output_root.join("reconstructed.tables");
    let path_tables_path = args.output_root.join("path_tables.tables");

    let source_bundle = read_tables_bundle(&args.sources)?;
    let factor_bundle = read_tables_bundle(&args.factors)?;
    let source_tables: Vec<Table> = source_bundle
        .tables
        .into_iter()
        .map(StoredTable::try_into_table)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(canonicalize_single_table)
        .collect();
    let factor_tables: Vec<Table> = factor_bundle
        .tables
        .into_iter()
        .map(StoredTable::try_into_table)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(canonicalize_single_table)
        .collect();
    let rules = read_json::<RulesArtifact>(&args.rules)?;

    if source_tables.len() != rules.tables.len() {
        bail!(
            "source table count {} does not match rule group count {}",
            source_tables.len(),
            rules.tables.len()
        );
    }

    let mut reconstructed_tables = Vec::with_capacity(source_tables.len());
    let mut path_tables = Vec::new();
    let mut records = Vec::with_capacity(source_tables.len());

    for (source_table_index, source_table) in source_tables.iter().enumerate() {
        let table_rules = rules
            .tables
            .get(source_table_index)
            .with_context(|| format!("missing rule group for source table {source_table_index}"))?;
        if table_rules.source_table_index != source_table_index {
            bail!(
                "rule group order mismatch: expected source_table_index {}, got {}",
                source_table_index,
                table_rules.source_table_index
            );
        }

        let unique_factors = build_unique_factors(table_rules, &factor_tables)?;
        let allowed_factors: Vec<_> = unique_factors
            .iter()
            .filter(|factor| {
                args.max_factor_arity
                    .is_none_or(|max_arity| factor.table.bits.len() <= max_arity)
            })
            .cloned()
            .collect();
        let proper_factors: Vec<_> = allowed_factors
            .iter()
            .filter(|factor| factor.table.bits.len() < source_table.bits.len())
            .cloned()
            .collect();
        let exact_source_factor_count = unique_factors
            .iter()
            .filter(|factor| {
                factor.table.bits == source_table.bits && factor.table.rows == source_table.rows
            })
            .count();
        let source_arity_factor_count = unique_factors
            .iter()
            .filter(|factor| factor.table.bits.len() == source_table.bits.len())
            .count();

        let search = search_reconstruction(source_table, &proper_factors, &allowed_factors)?;
        let chain = search
            .terminal_state_index
            .map(|terminal| chain_indices(&search.states, terminal))
            .unwrap_or_default();
        let final_table = chain
            .last()
            .map(|&state_index| search.states[state_index].table.clone())
            .unwrap_or_else(empty_table);
        let arity_sequence: Vec<usize> = chain
            .iter()
            .map(|&state_index| search.states[state_index].table.bits.len())
            .collect();
        reconstructed_tables.push(StoredTable::from_table(&final_table));

        let path_steps =
            build_path_steps(&search.states, &unique_factors, &chain, &mut path_tables);
        let record = ReconstructionRecord {
            source_table_index,
            source_bits: source_table.bits.clone(),
            source_row_count: source_table.rows.len(),
            total_rule_count: table_rules.rules.len(),
            unique_factor_count: unique_factors.len(),
            proper_factor_count: proper_factors.len(),
            source_arity_factor_count,
            exact_source_factor_count,
            reachable_state_count: search.states.len(),
            found: search.found_state_index.is_some(),
            final_bits: final_table.bits.clone(),
            final_row_count: final_table.rows.len(),
            matches_source: final_table.bits == source_table.bits
                && final_table.rows == source_table.rows,
            step_count: path_steps.len(),
            arity_sequence,
            path: path_steps,
        };
        records.push(record);

        if source_table_index % 25 == 24 || source_table_index + 1 == source_tables.len() {
            println!(
                "reconstructed_sources: {}/{}",
                source_table_index + 1,
                source_tables.len()
            );
        }
    }

    let reconstructed_bundle = TablesBundle {
        origin_arrays: factor_bundle.origin_arrays.clone(),
        tables: reconstructed_tables,
    };
    write_tables_bundle(&reconstructed_path, &reconstructed_bundle)?;

    let path_tables_bundle = TablesBundle {
        origin_arrays: factor_bundle.origin_arrays,
        tables: path_tables,
    };
    write_tables_bundle(&path_tables_path, &path_tables_bundle)?;

    write_json(&report_path, &records)?;
    let summary = summarize(&args, &records);
    write_json(&summary_path, &summary)?;

    println!("source_tables: {}", summary.source_table_count);
    println!("reconstructed: {}", summary.reconstructed_count);
    println!("failed: {}", summary.failed_count);
    println!("report: {}", report_path.display());
    println!("summary: {}", summary_path.display());
    println!("reconstructed_tables: {}", reconstructed_path.display());
    println!("path_tables: {}", path_tables_path.display());
    Ok(())
}

fn build_unique_factors(
    table_rules: &TableRules,
    factor_tables: &[Table],
) -> Result<Vec<UniqueFactor>> {
    let mut grouped: BTreeMap<(Vec<u32>, Vec<u32>), (Vec<usize>, Vec<usize>)> = BTreeMap::new();

    for rule in &table_rules.rules {
        let factor = factor_tables
            .get(rule.projected_table_index)
            .with_context(|| format!("missing factor {}", rule.projected_table_index))?;
        let entry = grouped
            .entry((factor.bits.clone(), factor.rows.clone()))
            .or_insert_with(|| (Vec::new(), Vec::new()));
        entry.0.push(rule.global_rule_index);
        entry.1.push(rule.projected_table_index);
    }

    Ok(grouped
        .into_iter()
        .enumerate()
        .map(
            |(
                local_factor_index,
                ((bits, rows), (source_rule_indices, projected_table_indices)),
            )| {
                UniqueFactor {
                    local_factor_index,
                    table: Table { bits, rows },
                    source_rule_indices,
                    projected_table_indices,
                }
            },
        )
        .collect())
}

struct SearchResult {
    states: Vec<SearchState>,
    found_state_index: Option<usize>,
    terminal_state_index: Option<usize>,
}

fn search_reconstruction(
    source: &Table,
    start_factors: &[UniqueFactor],
    expansion_factors: &[UniqueFactor],
) -> Result<SearchResult> {
    let mut states = Vec::new();
    let mut queue = VecDeque::new();
    let mut visited = HashMap::new();

    let seed_factors = if start_factors.is_empty() {
        expansion_factors
    } else {
        start_factors
    };

    let mut sorted_start_factor_indices: Vec<usize> = (0..seed_factors.len()).collect();
    sorted_start_factor_indices.sort_by(|left, right| {
        let left_factor = &seed_factors[*left];
        let right_factor = &seed_factors[*right];
        left_factor
            .table
            .bits
            .len()
            .cmp(&right_factor.table.bits.len())
            .then_with(|| {
                left_factor
                    .table
                    .rows
                    .len()
                    .cmp(&right_factor.table.rows.len())
            })
            .then_with(|| left_factor.table.bits.cmp(&right_factor.table.bits))
            .then_with(|| left_factor.table.rows.cmp(&right_factor.table.rows))
    });
    let mut sorted_expansion_factor_indices: Vec<usize> = (0..expansion_factors.len()).collect();
    sorted_expansion_factor_indices.sort_by(|left, right| {
        let left_factor = &expansion_factors[*left];
        let right_factor = &expansion_factors[*right];
        left_factor
            .table
            .bits
            .len()
            .cmp(&right_factor.table.bits.len())
            .then_with(|| {
                left_factor
                    .table
                    .rows
                    .len()
                    .cmp(&right_factor.table.rows.len())
            })
            .then_with(|| left_factor.table.bits.cmp(&right_factor.table.bits))
            .then_with(|| left_factor.table.rows.cmp(&right_factor.table.rows))
    });

    for &factor_index in &sorted_start_factor_indices {
        let factor = &seed_factors[factor_index];
        let key = table_key(&factor.table);
        if visited.contains_key(&key) {
            continue;
        }
        let state_index = states.len();
        visited.insert(key, state_index);
        states.push(SearchState {
            table: factor.table.clone(),
            parent_state_index: None,
            via_factor_local_index: factor.local_factor_index,
            depth: 1,
        });
        queue.push_back(state_index);
    }

    let mut found_state_index = None;
    while let Some(state_index) = queue.pop_front() {
        let current = states[state_index].clone();
        if current.table.bits == source.bits && current.table.rows == source.rows {
            found_state_index = Some(state_index);
            break;
        }

        for &factor_index in &sorted_expansion_factor_indices {
            let factor = &expansion_factors[factor_index];
            let merged = merge_exact(&current.table, &factor.table)?;
            if merged.rows.is_empty() {
                continue;
            }
            if merged.bits.len() <= current.table.bits.len() {
                continue;
            }
            let key = table_key(&merged);
            if visited.contains_key(&key) {
                continue;
            }
            let next_index = states.len();
            visited.insert(key, next_index);
            states.push(SearchState {
                table: merged,
                parent_state_index: Some(state_index),
                via_factor_local_index: factor.local_factor_index,
                depth: current.depth + 1,
            });
            queue.push_back(next_index);
        }
    }

    let terminal_state_index = found_state_index.or_else(|| best_terminal_state_index(&states));

    Ok(SearchResult {
        states,
        found_state_index,
        terminal_state_index,
    })
}

fn best_terminal_state_index(states: &[SearchState]) -> Option<usize> {
    states
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            left.table
                .bits
                .len()
                .cmp(&right.table.bits.len())
                .then_with(|| right.table.rows.len().cmp(&left.table.rows.len()))
                .then_with(|| right.depth.cmp(&left.depth))
                .then_with(|| right_index.cmp(left_index))
        })
        .map(|(index, _)| index)
}

fn chain_indices(states: &[SearchState], terminal_state_index: usize) -> Vec<usize> {
    let mut chain = Vec::new();
    let mut current = Some(terminal_state_index);
    while let Some(state_index) = current {
        chain.push(state_index);
        current = states[state_index].parent_state_index;
    }
    chain.reverse();
    chain
}

fn build_path_steps(
    states: &[SearchState],
    factors: &[UniqueFactor],
    chain: &[usize],
    path_tables: &mut Vec<StoredTable>,
) -> Vec<PathStep> {
    let factor_by_local_index: HashMap<usize, &UniqueFactor> = factors
        .iter()
        .map(|factor| (factor.local_factor_index, factor))
        .collect();
    chain
        .iter()
        .copied()
        .enumerate()
        .map(|(step_index, state_index)| {
            let state = &states[state_index];
            let factor = factor_by_local_index
                .get(&state.via_factor_local_index)
                .expect("missing factor for local index");
            let path_table_index = path_tables.len();
            path_tables.push(StoredTable::from_table(&state.table));
            PathStep {
                step: step_index + 1,
                path_table_index,
                factor_local_index: factor.local_factor_index,
                factor_bits: factor.table.bits.clone(),
                factor_row_count: factor.table.rows.len(),
                factor_source_rule_indices: factor.source_rule_indices.clone(),
                factor_projected_table_indices: factor.projected_table_indices.clone(),
                merged_bits: state.table.bits.clone(),
                merged_row_count: state.table.rows.len(),
            }
        })
        .collect()
}

fn merge_exact(left: &Table, right: &Table) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!(error))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn table_key(table: &Table) -> (Vec<u32>, Vec<u32>) {
    (table.bits.clone(), table.rows.clone())
}

fn canonicalize_single_table(table: Table) -> Table {
    let (bits, rows) = canonicalize_table(&table);
    Table { bits, rows }
}

fn empty_table() -> Table {
    Table {
        bits: Vec::new(),
        rows: Vec::new(),
    }
}

fn summarize(args: &Args, records: &[ReconstructionRecord]) -> Summary {
    let reconstructed_count = records
        .iter()
        .filter(|record| record.matches_source)
        .count();
    let failed_count = records.len().saturating_sub(reconstructed_count);
    let mut final_arity_distribution = BTreeMap::new();
    let mut step_count_distribution = BTreeMap::new();
    let mut steps = Vec::new();

    for record in records {
        *final_arity_distribution
            .entry(record.final_bits.len().to_string())
            .or_insert(0) += 1;
        *step_count_distribution
            .entry(record.step_count.to_string())
            .or_insert(0) += 1;
        steps.push(record.step_count);
    }

    let (min_steps, max_steps, mean_steps) = if steps.is_empty() {
        (0, 0, 0.0)
    } else {
        (
            *steps.iter().min().unwrap(),
            *steps.iter().max().unwrap(),
            steps.iter().sum::<usize>() as f64 / steps.len() as f64,
        )
    };

    Summary {
        sources: args.sources.display().to_string(),
        rules: args.rules.display().to_string(),
        factors: args.factors.display().to_string(),
        source_table_count: records.len(),
        reconstructed_count,
        failed_count,
        max_steps,
        min_steps,
        mean_steps,
        final_arity_distribution,
        step_count_distribution,
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &PathBuf) -> Result<T> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage:\n  cargo run --release --bin reconstruct_from_dependency_tables -- [--sources <merged.tables>] [--rules <rules.json>] [--factors <subtables.tables>] [--output-root <dir>] [--max-factor-arity <n>]"
    );
}

#[cfg(test)]
mod tests {
    use super::{search_reconstruction, UniqueFactor};
    use tables::common::Table;

    fn factor(local_factor_index: usize, bits: &[u32], rows: &[u32]) -> UniqueFactor {
        UniqueFactor {
            local_factor_index,
            table: Table {
                bits: bits.to_vec(),
                rows: rows.to_vec(),
            },
            source_rule_indices: vec![local_factor_index],
            projected_table_indices: vec![local_factor_index],
        }
    }

    #[test]
    fn search_reconstruction_uses_exact_factor_when_no_proper_start_exists() {
        let source = Table {
            bits: vec![1, 2],
            rows: vec![0, 3],
        };
        let exact = factor(0, &[1, 2], &[0, 3]);

        let search = search_reconstruction(&source, &[], &[exact]).unwrap();

        assert!(search.found_state_index.is_some());
        let terminal = search.terminal_state_index.expect("terminal state");
        assert_eq!(search.states[terminal].table.bits, source.bits);
        assert_eq!(search.states[terminal].table.rows, source.rows);
    }
}
