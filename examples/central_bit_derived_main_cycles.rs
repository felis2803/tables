use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{
    arity_distribution, collect_bits, is_full_row_set, project_row, read_tables, sort_dedup_rows,
    total_rows, write_json, Table,
};
use tables::rank_stats::{summarize_table_ranks, RankSummary};
use tables::subset_absorption::canonicalize_table;
use tables::table_merge_fast::merge_tables_fast_from_slices;

const MAX_ROW_ARITY: usize = 32;

#[derive(Debug)]
struct Args {
    input: PathBuf,
    cycles: usize,
    bit_order: BitOrder,
    derived_neighbor_source: DerivedNeighborSource,
    output_tables: PathBuf,
    output_derived_mains: PathBuf,
    output_report: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            cycles: 1000,
            bit_order: BitOrder::FewestRelated,
            derived_neighbor_source: DerivedNeighborSource::Current,
            output_tables: PathBuf::from(
                "runs/central_bit_derived_main_1000/tables.after_1000_cycles.json",
            ),
            output_derived_mains: PathBuf::from(
                "runs/central_bit_derived_main_1000/derived_mains.after_1000_cycles.json",
            ),
            output_report: PathBuf::from(
                "runs/central_bit_derived_main_1000/report.after_1000_cycles.json",
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
                "--cycles" => {
                    args.cycles = expect_value(&mut iter, "--cycles")?
                        .parse()
                        .with_context(|| "invalid value for --cycles")?;
                }
                "--bit-order" => {
                    args.bit_order = parse_bit_order(&expect_value(&mut iter, "--bit-order")?)?
                }
                "--derived-neighbor-source" => {
                    args.derived_neighbor_source = parse_derived_neighbor_source(&expect_value(
                        &mut iter,
                        "--derived-neighbor-source",
                    )?)?
                }
                "--output-tables" => {
                    args.output_tables = PathBuf::from(expect_value(&mut iter, "--output-tables")?)
                }
                "--output-derived-mains" => {
                    args.output_derived_mains =
                        PathBuf::from(expect_value(&mut iter, "--output-derived-mains")?)
                }
                "--output-report" => {
                    args.output_report = PathBuf::from(expect_value(&mut iter, "--output-report")?)
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
enum BitOrder {
    FewestRelated,
    MostRelated,
    DerivedNeighborFirst,
}

impl BitOrder {
    fn as_str(self) -> &'static str {
        match self {
            Self::FewestRelated => "fewest-related",
            Self::MostRelated => "most-related",
            Self::DerivedNeighborFirst => "derived-neighbor-first",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum DerivedNeighborSource {
    Current,
    Initial,
}

impl DerivedNeighborSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Initial => "initial",
        }
    }
}

fn parse_bit_order(value: &str) -> Result<BitOrder> {
    match value {
        "fewest-related" => Ok(BitOrder::FewestRelated),
        "most-related" => Ok(BitOrder::MostRelated),
        "derived-neighbor-first" => Ok(BitOrder::DerivedNeighborFirst),
        _ => bail!(
            "invalid --bit-order: {value}; expected one of: fewest-related, most-related, derived-neighbor-first"
        ),
    }
}

fn parse_derived_neighbor_source(value: &str) -> Result<DerivedNeighborSource> {
    match value {
        "current" => Ok(DerivedNeighborSource::Current),
        "initial" => Ok(DerivedNeighborSource::Initial),
        _ => bail!("invalid --derived-neighbor-source: {value}; expected one of: current, initial"),
    }
}

#[derive(Clone, Debug, Serialize)]
struct SystemMetrics {
    table_count: usize,
    bit_count: usize,
    row_count: usize,
    arity_distribution: BTreeMap<String, usize>,
    rank_summary: RankSummary,
}

#[derive(Clone, Debug, Serialize)]
struct MetricDelta {
    table_count_delta: isize,
    bit_count_delta: isize,
    row_count_delta: isize,
    rank_min_delta: f64,
    rank_max_delta: f64,
    rank_mean_delta: f64,
    rank_median_delta: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
struct InputNormalizationStats {
    changed_table_count: usize,
    reordered_table_count: usize,
    deduped_row_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct CycleRecord {
    cycle: usize,
    central_bit: u32,
    related_bit_count_before: usize,
    derived_neighbor_bit_count: usize,
    available_derived_neighbor_count: usize,
    remaining_bits_before: usize,
    incident_table_count: usize,
    incident_row_count: usize,
    absorbed_subset_table_count: usize,
    removed_system_table_count: usize,
    removed_system_row_count: usize,
    derived_tables_considered: usize,
    derived_subtables_merged: usize,
    main_bit_count_before_drop: usize,
    main_row_count_before_drop: usize,
    main_bit_count_after_drop: usize,
    main_row_count_after_drop: usize,
    stored_derived_main: bool,
    derived_main_was_empty: bool,
    derived_main_was_tautology: bool,
    system_table_count_after: usize,
    system_bit_count_after: usize,
    system_row_count_after: usize,
    skipped: bool,
    skip_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ExperimentReport {
    method: String,
    input: String,
    requested_cycles: usize,
    executed_cycles: usize,
    bit_order: String,
    derived_neighbor_source: String,
    raw_input_metrics: SystemMetrics,
    input_normalization: InputNormalizationStats,
    initial_metrics: SystemMetrics,
    final_metrics: SystemMetrics,
    derived_main_metrics: SystemMetrics,
    delta: MetricDelta,
    processed_bits: usize,
    skipped_bits: usize,
    remaining_unprocessed_bits: usize,
    derived_main_count: usize,
    total_absorbed_subset_tables: usize,
    total_removed_system_tables: usize,
    total_removed_system_rows: usize,
    total_derived_subtables_merged: usize,
    total_derived_tables_considered: usize,
    empty_main_count: usize,
    tautological_main_count: usize,
    cycles: Vec<CycleRecord>,
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let raw_tables = read_tables(&args.input)?;
    let raw_input_metrics = collect_metrics(&raw_tables);
    let (source_tables, input_normalization) = canonicalize_tables_individually(raw_tables);
    let initial_metrics = collect_metrics(&source_tables);

    let mut system_tables: Vec<Option<Table>> = source_tables.into_iter().map(Some).collect();
    let mut bit_to_tables = build_bit_to_tables(&system_tables);
    let original_tables: Vec<Table> = system_tables.iter().flatten().cloned().collect();
    let initial_related_bits_map = build_related_bits_map(&original_tables);
    let mut ordering_related_bits_map = initial_related_bits_map.clone();
    let mut remaining_bits: BTreeSet<u32> = ordering_related_bits_map.keys().copied().collect();
    let mut derived_main_store: Vec<Table> = Vec::new();
    let mut derived_main_map: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    let mut available_derived_bits: BTreeSet<u32> = BTreeSet::new();
    let mut live_table_count = initial_metrics.table_count;
    let mut live_row_count = initial_metrics.row_count;

    let mut cycles = Vec::new();
    let mut skipped_bits = 0usize;
    let mut total_absorbed_subset_tables = 0usize;
    let mut total_removed_system_tables = 0usize;
    let mut total_removed_system_rows = 0usize;
    let mut total_derived_subtables_merged = 0usize;
    let mut total_derived_tables_considered = 0usize;
    let mut empty_main_count = 0usize;
    let mut tautological_main_count = 0usize;

    for cycle_index in 1..=args.cycles {
        let Some(central_bit) = choose_next_bit(
            args.bit_order,
            &remaining_bits,
            &ordering_related_bits_map,
            &initial_related_bits_map,
            &available_derived_bits,
        ) else {
            break;
        };

        let ordering_related_bits = ordering_related_bits_map
            .get(&central_bit)
            .cloned()
            .unwrap_or_else(BTreeSet::new);
        let derived_neighbor_bits = match args.derived_neighbor_source {
            DerivedNeighborSource::Current => ordering_related_bits.clone(),
            DerivedNeighborSource::Initial => initial_related_bits_map
                .get(&central_bit)
                .cloned()
                .unwrap_or_else(BTreeSet::new),
        };
        let available_derived_neighbor_count = derived_neighbor_bits
            .iter()
            .filter(|bit| **bit != central_bit && available_derived_bits.contains(bit))
            .count();
        let incident_table_ids: Vec<usize> = bit_to_tables
            .get(&central_bit)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
        let incident_tables = collect_tables(&system_tables, &incident_table_ids)?;
        let incident_row_count = incident_tables.iter().map(|table| table.rows.len()).sum();

        if incident_tables.is_empty() {
            skipped_bits += 1;
            cycles.push(CycleRecord {
                cycle: cycle_index,
                central_bit,
                related_bit_count_before: ordering_related_bits.len(),
                derived_neighbor_bit_count: derived_neighbor_bits.len(),
                available_derived_neighbor_count,
                remaining_bits_before: remaining_bits.len(),
                incident_table_count: 0,
                incident_row_count: 0,
                absorbed_subset_table_count: 0,
                removed_system_table_count: 0,
                removed_system_row_count: 0,
                derived_tables_considered: 0,
                derived_subtables_merged: 0,
                main_bit_count_before_drop: 0,
                main_row_count_before_drop: 0,
                main_bit_count_after_drop: 0,
                main_row_count_after_drop: 0,
                stored_derived_main: false,
                derived_main_was_empty: false,
                derived_main_was_tautology: false,
                system_table_count_after: live_table_count,
                system_bit_count_after: bit_to_tables.len(),
                system_row_count_after: live_row_count,
                skipped: true,
                skip_reason: Some("bit no longer present in the remaining system".to_string()),
            });
            finalize_processed_bit(
                central_bit,
                &mut remaining_bits,
                &mut ordering_related_bits_map,
            );
            continue;
        }

        let build_main_result: Result<(Table, BTreeSet<usize>, Vec<usize>, usize, usize)> =
            (|| {
                let mut main = merge_all_tables(&incident_tables, central_bit)?;
                let mut included_table_ids: BTreeSet<usize> =
                    incident_table_ids.iter().copied().collect();
                let subset_table_ids =
                    find_subset_table_ids(&main.bits, &system_tables, &included_table_ids);
                for table_id in &subset_table_ids {
                    let table = system_tables[*table_id]
                        .as_ref()
                        .with_context(|| format!("missing subset table {table_id}"))?;
                    main = merge_into_main(&main, table, central_bit)?;
                    included_table_ids.insert(*table_id);
                }

                let mut derived_tables_considered = 0usize;
                let mut derived_subtables_merged = 0usize;
                let mut derived_main_ids = BTreeSet::new();
                for related_bit in derived_neighbor_bits
                    .iter()
                    .copied()
                    .filter(|bit| *bit != central_bit)
                {
                    let Some(derived_ids) = derived_main_map.get(&related_bit) else {
                        continue;
                    };
                    derived_main_ids.extend(derived_ids.iter().copied());
                }

                for derived_id in derived_main_ids {
                    let derived_table = derived_main_store
                        .get(derived_id)
                        .with_context(|| format!("missing derived main {derived_id}"))?;
                    derived_tables_considered += 1;
                    if let Some(projected) =
                        project_table_to_bit_set(derived_table, &derived_neighbor_bits)?
                    {
                        main = merge_into_main(&main, &projected, central_bit)?;
                        derived_subtables_merged += 1;
                    }
                }

                Ok((
                    main,
                    included_table_ids,
                    subset_table_ids,
                    derived_tables_considered,
                    derived_subtables_merged,
                ))
            })();

        let (
            main,
            included_table_ids,
            subset_table_ids,
            derived_tables_considered,
            derived_subtables_merged,
        ) = match build_main_result {
            Ok(result) => result,
            Err(error) => {
                skipped_bits += 1;
                cycles.push(CycleRecord {
                    cycle: cycle_index,
                    central_bit,
                    related_bit_count_before: ordering_related_bits.len(),
                    derived_neighbor_bit_count: derived_neighbor_bits.len(),
                    available_derived_neighbor_count,
                    remaining_bits_before: remaining_bits.len(),
                    incident_table_count: incident_table_ids.len(),
                    incident_row_count,
                    absorbed_subset_table_count: 0,
                    removed_system_table_count: 0,
                    removed_system_row_count: 0,
                    derived_tables_considered: 0,
                    derived_subtables_merged: 0,
                    main_bit_count_before_drop: 0,
                    main_row_count_before_drop: 0,
                    main_bit_count_after_drop: 0,
                    main_row_count_after_drop: 0,
                    stored_derived_main: false,
                    derived_main_was_empty: false,
                    derived_main_was_tautology: false,
                    system_table_count_after: live_table_count,
                    system_bit_count_after: bit_to_tables.len(),
                    system_row_count_after: live_row_count,
                    skipped: true,
                    skip_reason: Some(error.to_string()),
                });
                finalize_processed_bit(
                    central_bit,
                    &mut remaining_bits,
                    &mut ordering_related_bits_map,
                );
                continue;
            }
        };

        let removed_system_row_count =
            remove_tables_from_system(&mut system_tables, &mut bit_to_tables, &included_table_ids)?;
        live_table_count -= included_table_ids.len();
        live_row_count -= removed_system_row_count;
        total_absorbed_subset_tables += subset_table_ids.len();
        total_removed_system_tables += included_table_ids.len();
        total_removed_system_rows += removed_system_row_count;
        total_derived_tables_considered += derived_tables_considered;
        total_derived_subtables_merged += derived_subtables_merged;

        let main_bit_count_before_drop = main.bits.len();
        let main_row_count_before_drop = main.rows.len();
        let derived_main = drop_central_bit(main, central_bit)?;
        let (
            main_bit_count_after_drop,
            main_row_count_after_drop,
            stored_derived_main,
            empty_main,
            tautological_main,
        ) = match derived_main {
            DerivedMain::Stored(table) => {
                let bit_count = table.bits.len();
                let row_count = table.rows.len();
                let derived_id = derived_main_store.len();
                for &bit in &table.bits {
                    derived_main_map.entry(bit).or_default().push(derived_id);
                    available_derived_bits.insert(bit);
                }
                derived_main_store.push(table);
                (bit_count, row_count, true, false, false)
            }
            DerivedMain::Empty { bit_count } => {
                empty_main_count += 1;
                (bit_count, 0, false, true, false)
            }
            DerivedMain::Tautology {
                bit_count,
                row_count,
            } => {
                tautological_main_count += 1;
                (bit_count, row_count, false, false, true)
            }
        };

        cycles.push(CycleRecord {
            cycle: cycle_index,
            central_bit,
            related_bit_count_before: ordering_related_bits.len(),
            derived_neighbor_bit_count: derived_neighbor_bits.len(),
            available_derived_neighbor_count,
            remaining_bits_before: remaining_bits.len(),
            incident_table_count: incident_table_ids.len(),
            incident_row_count,
            absorbed_subset_table_count: subset_table_ids.len(),
            removed_system_table_count: included_table_ids.len(),
            removed_system_row_count,
            derived_tables_considered,
            derived_subtables_merged,
            main_bit_count_before_drop,
            main_row_count_before_drop,
            main_bit_count_after_drop,
            main_row_count_after_drop,
            stored_derived_main,
            derived_main_was_empty: empty_main,
            derived_main_was_tautology: tautological_main,
            system_table_count_after: live_table_count,
            system_bit_count_after: bit_to_tables.len(),
            system_row_count_after: live_row_count,
            skipped: false,
            skip_reason: None,
        });

        finalize_processed_bit(
            central_bit,
            &mut remaining_bits,
            &mut ordering_related_bits_map,
        );
    }

    let final_tables: Vec<Table> = system_tables.into_iter().flatten().collect();
    let final_metrics = collect_metrics(&final_tables);
    let derived_main_metrics = collect_metrics(&derived_main_store);
    let report = ExperimentReport {
        method: "for each central bit ordered by current related-bit set size: merge all incident system tables into main, merge all remaining system tables whose bit sets are subsets of main.bits, remove consumed system tables, merge projected subtables from previously derived mains keyed by related bits, drop the central bit from main, store the non-empty non-tautological derived main, remove the processed bit from all related-bit sets, and repeat".to_string(),
        input: args.input.display().to_string(),
        requested_cycles: args.cycles,
        executed_cycles: cycles.len(),
        bit_order: args.bit_order.as_str().to_string(),
        derived_neighbor_source: args.derived_neighbor_source.as_str().to_string(),
        raw_input_metrics,
        input_normalization,
        initial_metrics: initial_metrics.clone(),
        final_metrics: final_metrics.clone(),
        derived_main_metrics: derived_main_metrics.clone(),
        delta: metric_delta(&initial_metrics, &final_metrics),
        processed_bits: cycles.len() - skipped_bits,
        skipped_bits,
        remaining_unprocessed_bits: remaining_bits.len(),
        derived_main_count: derived_main_store.len(),
        total_absorbed_subset_tables,
        total_removed_system_tables,
        total_removed_system_rows,
        total_derived_subtables_merged,
        total_derived_tables_considered,
        empty_main_count,
        tautological_main_count,
        cycles,
    };

    write_json(&args.output_tables, &final_tables)?;
    write_json(&args.output_derived_mains, &derived_main_store)?;
    write_json(&args.output_report, &report)?;

    println!("executed_cycles={}", report.executed_cycles);
    println!("processed_bits={}", report.processed_bits);
    println!("skipped_bits={}", report.skipped_bits);
    println!("initial_tables={}", report.initial_metrics.table_count);
    println!("final_tables={}", report.final_metrics.table_count);
    println!("initial_bits={}", report.initial_metrics.bit_count);
    println!("final_bits={}", report.final_metrics.bit_count);
    println!("initial_rows={}", report.initial_metrics.row_count);
    println!("final_rows={}", report.final_metrics.row_count);
    println!(
        "initial_rank_mean={:.12}",
        report.initial_metrics.rank_summary.mean_rank
    );
    println!(
        "final_rank_mean={:.12}",
        report.final_metrics.rank_summary.mean_rank
    );
    println!(
        "initial_rank_median={:.12}",
        report.initial_metrics.rank_summary.median_rank
    );
    println!(
        "final_rank_median={:.12}",
        report.final_metrics.rank_summary.median_rank
    );
    println!(
        "derived_main_tables={}",
        report.derived_main_metrics.table_count
    );
    println!(
        "derived_main_bits={}",
        report.derived_main_metrics.bit_count
    );
    println!(
        "derived_main_rows={}",
        report.derived_main_metrics.row_count
    );
    println!(
        "derived_main_rank_mean={:.12}",
        report.derived_main_metrics.rank_summary.mean_rank
    );
    println!(
        "derived_main_rank_median={:.12}",
        report.derived_main_metrics.rank_summary.median_rank
    );
    println!("output_tables={}", args.output_tables.display());
    println!(
        "output_derived_mains={}",
        args.output_derived_mains.display()
    );
    println!("output_report={}", args.output_report.display());
    Ok(())
}

fn collect_metrics(tables: &[Table]) -> SystemMetrics {
    SystemMetrics {
        table_count: tables.len(),
        bit_count: collect_bits(tables).len(),
        row_count: total_rows(tables),
        arity_distribution: arity_distribution(tables),
        rank_summary: summarize_table_ranks(tables, 10),
    }
}

fn canonicalize_tables_individually(tables: Vec<Table>) -> (Vec<Table>, InputNormalizationStats) {
    let mut normalized = Vec::with_capacity(tables.len());
    let mut stats = InputNormalizationStats::default();

    for table in tables {
        let original_bits = table.bits.clone();
        let original_rows = table.rows.clone();
        let (bits, rows) = canonicalize_table(&table);

        if bits != original_bits || rows != original_rows {
            stats.changed_table_count += 1;
        }
        if bits != original_bits {
            stats.reordered_table_count += 1;
        }
        if rows.len() < original_rows.len() {
            stats.deduped_row_count += original_rows.len() - rows.len();
        }

        normalized.push(Table { bits, rows });
    }

    (normalized, stats)
}

fn metric_delta(initial: &SystemMetrics, final_metrics: &SystemMetrics) -> MetricDelta {
    MetricDelta {
        table_count_delta: final_metrics.table_count as isize - initial.table_count as isize,
        bit_count_delta: final_metrics.bit_count as isize - initial.bit_count as isize,
        row_count_delta: final_metrics.row_count as isize - initial.row_count as isize,
        rank_min_delta: final_metrics.rank_summary.min_rank - initial.rank_summary.min_rank,
        rank_max_delta: final_metrics.rank_summary.max_rank - initial.rank_summary.max_rank,
        rank_mean_delta: final_metrics.rank_summary.mean_rank - initial.rank_summary.mean_rank,
        rank_median_delta: final_metrics.rank_summary.median_rank
            - initial.rank_summary.median_rank,
    }
}

fn build_bit_to_tables(tables: &[Option<Table>]) -> BTreeMap<u32, BTreeSet<usize>> {
    let mut bit_to_tables: BTreeMap<u32, BTreeSet<usize>> = BTreeMap::new();
    for (table_id, table) in tables.iter().enumerate() {
        let Some(table) = table else {
            continue;
        };
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().insert(table_id);
        }
    }
    bit_to_tables
}

fn build_related_bits_map(tables: &[Table]) -> BTreeMap<u32, BTreeSet<u32>> {
    let mut related_bits: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    for table in tables {
        let bits: BTreeSet<u32> = table.bits.iter().copied().collect();
        for &bit in &table.bits {
            related_bits
                .entry(bit)
                .or_default()
                .extend(bits.iter().copied());
        }
    }
    related_bits
}

fn choose_next_bit(
    bit_order: BitOrder,
    remaining_bits: &BTreeSet<u32>,
    ordering_related_bits_map: &BTreeMap<u32, BTreeSet<u32>>,
    initial_related_bits_map: &BTreeMap<u32, BTreeSet<u32>>,
    available_derived_bits: &BTreeSet<u32>,
) -> Option<u32> {
    let candidate_bits: Vec<u32> = remaining_bits
        .iter()
        .copied()
        .filter(|bit| {
            ordering_related_bits_map
                .get(bit)
                .map(|bits| bits.len() <= MAX_ROW_ARITY)
                .unwrap_or(false)
        })
        .collect();

    match bit_order {
        BitOrder::FewestRelated => candidate_bits.into_iter().min_by(|left, right| {
            ordering_related_bits_map
                .get(left)
                .map(|bits| bits.len())
                .unwrap_or(0)
                .cmp(
                    &ordering_related_bits_map
                        .get(right)
                        .map(|bits| bits.len())
                        .unwrap_or(0),
                )
                .then_with(|| left.cmp(right))
        }),
        BitOrder::MostRelated => candidate_bits.into_iter().max_by(|left, right| {
            ordering_related_bits_map
                .get(left)
                .map(|bits| bits.len())
                .unwrap_or(0)
                .cmp(
                    &ordering_related_bits_map
                        .get(right)
                        .map(|bits| bits.len())
                        .unwrap_or(0),
                )
                .then_with(|| right.cmp(left))
        }),
        BitOrder::DerivedNeighborFirst => candidate_bits.into_iter().max_by(|left, right| {
            available_derived_neighbor_count(
                *left,
                initial_related_bits_map,
                available_derived_bits,
            )
            .cmp(&available_derived_neighbor_count(
                *right,
                initial_related_bits_map,
                available_derived_bits,
            ))
            .then_with(|| {
                initial_related_bits_map
                    .get(left)
                    .map(|bits| bits.len())
                    .unwrap_or(0)
                    .cmp(
                        &initial_related_bits_map
                            .get(right)
                            .map(|bits| bits.len())
                            .unwrap_or(0),
                    )
            })
            .then_with(|| right.cmp(left))
        }),
    }
}

fn available_derived_neighbor_count(
    bit: u32,
    initial_related_bits_map: &BTreeMap<u32, BTreeSet<u32>>,
    available_derived_bits: &BTreeSet<u32>,
) -> usize {
    initial_related_bits_map
        .get(&bit)
        .into_iter()
        .flat_map(|bits| bits.iter().copied())
        .filter(|related_bit| *related_bit != bit && available_derived_bits.contains(related_bit))
        .count()
}

fn collect_tables(system_tables: &[Option<Table>], table_ids: &[usize]) -> Result<Vec<Table>> {
    table_ids
        .iter()
        .map(|&table_id| {
            system_tables[table_id]
                .clone()
                .with_context(|| format!("missing table {table_id}"))
        })
        .collect()
}

fn merge_all_tables(tables: &[Table], central_bit: u32) -> Result<Table> {
    let Some(first) = tables.first() else {
        bail!("central bit {central_bit} has no incident tables");
    };

    let mut main = first.clone();
    for table in &tables[1..] {
        main = merge_into_main(&main, table, central_bit)?;
    }
    Ok(main)
}

fn merge_into_main(main: &Table, other: &Table, central_bit: u32) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&main.bits, &main.rows, &other.bits, &other.rows)
        .map_err(|error| anyhow!("failed to merge main for central bit {central_bit}: {error}"))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn find_subset_table_ids(
    main_bits: &[u32],
    system_tables: &[Option<Table>],
    excluded_ids: &BTreeSet<usize>,
) -> Vec<usize> {
    let mut matches = Vec::new();
    for (table_id, table) in system_tables.iter().enumerate() {
        if excluded_ids.contains(&table_id) {
            continue;
        }
        let Some(table) = table else {
            continue;
        };
        if is_strict_subset_bits(&table.bits, main_bits) {
            matches.push(table_id);
        }
    }

    matches.sort_by(|left, right| {
        let left_bits = &system_tables[*left].as_ref().unwrap().bits;
        let right_bits = &system_tables[*right].as_ref().unwrap().bits;
        left_bits
            .len()
            .cmp(&right_bits.len())
            .then_with(|| left_bits.cmp(right_bits))
            .then_with(|| left.cmp(right))
    });
    matches
}

fn is_strict_subset_bits(subset: &[u32], superset: &[u32]) -> bool {
    if subset.len() >= superset.len() {
        return false;
    }

    let mut subset_index = 0usize;
    let mut superset_index = 0usize;
    while subset_index < subset.len() && superset_index < superset.len() {
        match subset[subset_index].cmp(&superset[superset_index]) {
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Greater => superset_index += 1,
            std::cmp::Ordering::Equal => {
                subset_index += 1;
                superset_index += 1;
            }
        }
    }

    subset_index == subset.len()
}

fn remove_tables_from_system(
    system_tables: &mut [Option<Table>],
    bit_to_tables: &mut BTreeMap<u32, BTreeSet<usize>>,
    removed_ids: &BTreeSet<usize>,
) -> Result<usize> {
    let mut removed_rows = 0usize;

    for &table_id in removed_ids {
        let Some(table) = system_tables[table_id].take() else {
            continue;
        };

        removed_rows += table.rows.len();
        for bit in table.bits {
            let should_remove_key = if let Some(table_ids) = bit_to_tables.get_mut(&bit) {
                table_ids.remove(&table_id);
                table_ids.is_empty()
            } else {
                false
            };
            if should_remove_key {
                bit_to_tables.remove(&bit);
            }
        }
    }

    Ok(removed_rows)
}

fn project_table_to_bit_set(table: &Table, allowed_bits: &BTreeSet<u32>) -> Result<Option<Table>> {
    let mut projected_bits = Vec::new();
    let mut projected_indices = Vec::new();
    for (index, &bit) in table.bits.iter().enumerate() {
        if allowed_bits.contains(&bit) {
            projected_bits.push(bit);
            projected_indices.push(index);
        }
    }

    if projected_bits.is_empty() {
        return Ok(None);
    }

    let mut projected_rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    if projected_rows.is_empty() {
        bail!(
            "projection to bits {:?} produced an empty subtable",
            projected_bits
        );
    }
    if is_full_row_set(projected_rows.len(), projected_bits.len()) {
        return Ok(None);
    }

    Ok(Some(Table {
        bits: projected_bits,
        rows: projected_rows,
    }))
}

enum DerivedMain {
    Stored(Table),
    Empty { bit_count: usize },
    Tautology { bit_count: usize, row_count: usize },
}

fn drop_central_bit(main: Table, central_bit: u32) -> Result<DerivedMain> {
    let mut projected_bits = Vec::with_capacity(main.bits.len().saturating_sub(1));
    let mut projected_indices = Vec::with_capacity(main.bits.len().saturating_sub(1));

    for (index, &bit) in main.bits.iter().enumerate() {
        if bit == central_bit {
            continue;
        }
        projected_bits.push(bit);
        projected_indices.push(index);
    }

    if projected_bits.len() + 1 != main.bits.len() {
        bail!(
            "central bit {central_bit} missing from main {:?}",
            main.bits
        );
    }

    let mut projected_rows: Vec<u32> = main
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &projected_indices))
        .collect();
    sort_dedup_rows(&mut projected_rows);

    if projected_bits.is_empty() {
        if projected_rows.is_empty() {
            return Ok(DerivedMain::Empty { bit_count: 0 });
        }
        if projected_rows == vec![0] {
            return Ok(DerivedMain::Tautology {
                bit_count: 0,
                row_count: 1,
            });
        }
        bail!("invalid zero-bit derived main after removing central bit {central_bit}");
    }

    if projected_rows.is_empty() {
        return Ok(DerivedMain::Empty {
            bit_count: projected_bits.len(),
        });
    }
    if is_full_row_set(projected_rows.len(), projected_bits.len()) {
        return Ok(DerivedMain::Tautology {
            bit_count: projected_bits.len(),
            row_count: projected_rows.len(),
        });
    }

    Ok(DerivedMain::Stored(Table {
        bits: projected_bits,
        rows: projected_rows,
    }))
}

fn finalize_processed_bit(
    central_bit: u32,
    remaining_bits: &mut BTreeSet<u32>,
    related_bits_map: &mut BTreeMap<u32, BTreeSet<u32>>,
) {
    remaining_bits.remove(&central_bit);
    related_bits_map.remove(&central_bit);
    for related_bits in related_bits_map.values_mut() {
        related_bits.remove(&central_bit);
    }
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --release --example central_bit_derived_main_cycles -- [--input <tables.json>] [--cycles <n>] [--output-tables <tables.json>] [--output-derived-mains <tables.json>] [--output-report <report.json>]"
    );
}
