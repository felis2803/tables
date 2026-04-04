#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tables::common::{read_tables, sort_dedup_rows, total_rows, write_json, Table};
use tables::rank_stats::compute_rank;
use tables::table_merge_fast::merge_tables_fast_from_slices;

struct Args {
    input: PathBuf,
    report: PathBuf,
    top_examples: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            input: PathBuf::from("data/raw/tables.json"),
            report: PathBuf::from("runs/2026-04-03-origin-merge-prediction/report.json"),
            top_examples: 10,
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
                "--top-examples" => {
                    args.top_examples = expect_value(&mut iter, "--top-examples")?
                        .parse()
                        .with_context(|| "invalid value for --top-examples")?;
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

#[derive(Clone)]
struct TableStats {
    row_count: usize,
    arity: usize,
    one_probs: Vec<f64>,
}

#[derive(Clone, Copy)]
struct PairMeta {
    shared_count: u8,
    union_arity: u8,
}

impl PairMeta {
    fn flipped(self) -> Self {
        self
    }
}

#[derive(Clone, Default)]
struct MetricsAccumulator {
    pair_count: usize,
    exact_empty_count: usize,
    sum_predicted_rows: f64,
    sum_actual_rows: f64,
    sum_abs_rows_error: f64,
    sum_abs_rank_error: f64,
    sum_abs_log2_rows_plus1_error: f64,
    sum_sq_log2_rows_plus1_error: f64,
    q_errors_plus1: Vec<f64>,
    predicted_empty_count: usize,
    true_positive_empty_count: usize,
    false_positive_empty_count: usize,
    false_negative_empty_count: usize,
}

#[derive(Clone, Serialize)]
struct TimingReport {
    seconds: f64,
    nanos_per_pair: f64,
    pairs_per_second: f64,
}

#[derive(Clone, Serialize)]
struct WorstExample {
    left_index: usize,
    right_index: usize,
    left_arity: usize,
    right_arity: usize,
    left_rows: usize,
    right_rows: usize,
    shared_bit_count: usize,
    result_arity: usize,
    predicted_rows: f64,
    actual_rows: usize,
    predicted_rank: f64,
    actual_rank: f64,
    q_error_plus1: f64,
    abs_log2_rows_plus1_error: f64,
    abs_rank_error: f64,
}

#[derive(Clone, Serialize)]
struct ErrorSummary {
    pair_count: usize,
    exact_empty_count: usize,
    mean_predicted_rows: f64,
    mean_actual_rows: f64,
    mean_abs_rows_error: f64,
    mean_abs_rank_error: f64,
    mean_abs_log2_rows_plus1_error: f64,
    rmse_log2_rows_plus1_error: f64,
    mean_q_error_plus1: f64,
    median_q_error_plus1: f64,
    p95_q_error_plus1: f64,
    max_q_error_plus1: f64,
    predicted_empty_threshold: f64,
    predicted_empty_count: usize,
    true_positive_empty_count: usize,
    false_positive_empty_count: usize,
    false_negative_empty_count: usize,
    empty_precision: f64,
    empty_recall: f64,
}

#[derive(Clone, Serialize)]
struct SharedCountReport {
    shared_bit_count: usize,
    metrics: ErrorSummary,
}

#[derive(Clone, Serialize)]
struct PredictorReport {
    name: String,
    description: String,
    timing: TimingReport,
    overall: ErrorSummary,
    by_shared_bit_count: Vec<SharedCountReport>,
    worst_examples_by_q_error: Vec<WorstExample>,
}

#[derive(Clone, Serialize)]
struct ExactMergeSummary {
    pair_count: usize,
    empty_merge_count: usize,
    nonempty_merge_count: usize,
    mean_rows: f64,
    max_rows: usize,
    mean_rank: f64,
    max_rank: f64,
}

#[derive(Clone, Serialize)]
struct Report {
    method: String,
    input: String,
    pair_filter: String,
    prediction_cap: String,
    table_count: usize,
    total_input_rows: usize,
    max_input_arity: usize,
    pair_generation_timing: TimingReport,
    pair_metadata_timing: TimingReport,
    exact_merge_timing: TimingReport,
    exact_merge_summary: ExactMergeSummary,
    predictors: Vec<PredictorReport>,
}

impl MetricsAccumulator {
    fn observe(&mut self, predicted_rows: f64, actual_rows: usize, result_arity: usize) {
        const EMPTY_THRESHOLD: f64 = 0.5;

        let actual_rows_f = actual_rows as f64;
        let predicted_rank = compute_rank_float(predicted_rows, result_arity);
        let actual_rank = compute_rank(actual_rows, result_arity);
        let abs_rows_error = (predicted_rows - actual_rows_f).abs();
        let pred_log = log2_plus1(predicted_rows);
        let actual_log = log2_plus1(actual_rows_f);
        let abs_log_error = (pred_log - actual_log).abs();
        let q_error = q_error_plus1(predicted_rows, actual_rows_f);

        self.pair_count += 1;
        self.sum_predicted_rows += predicted_rows;
        self.sum_actual_rows += actual_rows_f;
        self.sum_abs_rows_error += abs_rows_error;
        self.sum_abs_rank_error += (predicted_rank - actual_rank).abs();
        self.sum_abs_log2_rows_plus1_error += abs_log_error;
        self.sum_sq_log2_rows_plus1_error += abs_log_error * abs_log_error;
        self.q_errors_plus1.push(q_error);

        if actual_rows == 0 {
            self.exact_empty_count += 1;
        }
        if predicted_rows < EMPTY_THRESHOLD {
            self.predicted_empty_count += 1;
            if actual_rows == 0 {
                self.true_positive_empty_count += 1;
            } else {
                self.false_positive_empty_count += 1;
            }
        } else if actual_rows == 0 {
            self.false_negative_empty_count += 1;
        }
    }

    fn finalize(mut self) -> ErrorSummary {
        const EMPTY_THRESHOLD: f64 = 0.5;

        if self.pair_count == 0 {
            return ErrorSummary {
                pair_count: 0,
                exact_empty_count: 0,
                mean_predicted_rows: 0.0,
                mean_actual_rows: 0.0,
                mean_abs_rows_error: 0.0,
                mean_abs_rank_error: 0.0,
                mean_abs_log2_rows_plus1_error: 0.0,
                rmse_log2_rows_plus1_error: 0.0,
                mean_q_error_plus1: 0.0,
                median_q_error_plus1: 0.0,
                p95_q_error_plus1: 0.0,
                max_q_error_plus1: 0.0,
                predicted_empty_threshold: EMPTY_THRESHOLD,
                predicted_empty_count: 0,
                true_positive_empty_count: 0,
                false_positive_empty_count: 0,
                false_negative_empty_count: 0,
                empty_precision: 0.0,
                empty_recall: 0.0,
            };
        }

        self.q_errors_plus1
            .sort_by(|left, right| left.total_cmp(right));
        let pair_count_f = self.pair_count as f64;
        let tp = self.true_positive_empty_count as f64;
        let fp = self.false_positive_empty_count as f64;
        let fn_count = self.false_negative_empty_count as f64;

        ErrorSummary {
            pair_count: self.pair_count,
            exact_empty_count: self.exact_empty_count,
            mean_predicted_rows: self.sum_predicted_rows / pair_count_f,
            mean_actual_rows: self.sum_actual_rows / pair_count_f,
            mean_abs_rows_error: self.sum_abs_rows_error / pair_count_f,
            mean_abs_rank_error: self.sum_abs_rank_error / pair_count_f,
            mean_abs_log2_rows_plus1_error: self.sum_abs_log2_rows_plus1_error / pair_count_f,
            rmse_log2_rows_plus1_error: (self.sum_sq_log2_rows_plus1_error / pair_count_f).sqrt(),
            mean_q_error_plus1: self.q_errors_plus1.iter().sum::<f64>() / pair_count_f,
            median_q_error_plus1: percentile_sorted(&self.q_errors_plus1, 0.5),
            p95_q_error_plus1: percentile_sorted(&self.q_errors_plus1, 0.95),
            max_q_error_plus1: *self.q_errors_plus1.last().unwrap(),
            predicted_empty_threshold: EMPTY_THRESHOLD,
            predicted_empty_count: self.predicted_empty_count,
            true_positive_empty_count: self.true_positive_empty_count,
            false_positive_empty_count: self.false_positive_empty_count,
            false_negative_empty_count: self.false_negative_empty_count,
            empty_precision: ratio_or_zero(tp, tp + fp),
            empty_recall: ratio_or_zero(tp, tp + fn_count),
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse()?;

    let tables = canonicalize_tables(read_tables(&args.input)?);
    let total_input_rows = total_rows(&tables);
    let max_input_arity = tables.iter().map(|table| table.bits.len()).max().unwrap_or(0);

    println!("loaded {} tables from {}", tables.len(), args.input.display());

    let table_stats = build_table_stats(&tables);

    let pair_generation_started = Instant::now();
    let pair_keys = generate_shared_pairs(&tables);
    let pair_generation_timing = elapsed_report(pair_generation_started.elapsed().as_secs_f64(), pair_keys.len());
    println!("shared pairs: {}", pair_keys.len());

    let pair_metadata_started = Instant::now();
    let pair_meta = build_pair_meta(&tables, &pair_keys)?;
    let pair_metadata_timing = elapsed_report(pair_metadata_started.elapsed().as_secs_f64(), pair_keys.len());

    let method1_started = Instant::now();
    let mut method1_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left = &tables[left_index];
        let right = &tables[right_index];
        let left_stats = &table_stats[left_index];
        let right_stats = &table_stats[right_index];
        let meta = pair_meta[pair_index];
        let predicted_rows = predict_uniform_shared_bits(left_stats, right_stats, meta);
        let predicted_rows = cap_predicted_rows(predicted_rows, left_stats, right_stats, meta);
        method1_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method1 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
        debug_assert_eq!(left.bits.len(), left_stats.arity);
        debug_assert_eq!(right.bits.len(), right_stats.arity);
    }
    let method1_timing = elapsed_report(method1_started.elapsed().as_secs_f64(), pair_keys.len());

    let method2_started = Instant::now();
    let mut method2_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left = &tables[left_index];
        let right = &tables[right_index];
        let left_stats = &table_stats[left_index];
        let right_stats = &table_stats[right_index];
        let meta = pair_meta[pair_index];
        let predicted_rows =
            predict_marginal_shared_bits(left, right, left_stats, right_stats, meta);
        let predicted_rows = cap_predicted_rows(predicted_rows, left_stats, right_stats, meta);
        method2_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method2 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
    }
    let method2_timing = elapsed_report(method2_started.elapsed().as_secs_f64(), pair_keys.len());

    let method3_started = Instant::now();
    let mut method3_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left = &tables[left_index];
        let right = &tables[right_index];
        let predicted_rows = predict_shared_assignment_histogram(left, right);
        method3_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method3 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
    }
    let method3_timing = elapsed_report(method3_started.elapsed().as_secs_f64(), pair_keys.len());

    let method4_started = Instant::now();
    let mut method4_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left_stats = &table_stats[left_index];
        let right_stats = &table_stats[right_index];
        let meta = pair_meta[pair_index];
        let predicted_rows = predict_rank_extension(left_stats, right_stats, meta);
        let predicted_rows = cap_predicted_rows(predicted_rows, left_stats, right_stats, meta);
        method4_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method4 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
    }
    let method4_timing = elapsed_report(method4_started.elapsed().as_secs_f64(), pair_keys.len());

    let method5_started = Instant::now();
    let mut method5_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left_stats = &table_stats[left_index];
        let right_stats = &table_stats[right_index];
        let meta = pair_meta[pair_index];
        let predicted_rows = predict_rank_extension(right_stats, left_stats, meta.flipped());
        let predicted_rows = cap_predicted_rows(predicted_rows, left_stats, right_stats, meta);
        method5_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method5 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
    }
    let method5_timing = elapsed_report(method5_started.elapsed().as_secs_f64(), pair_keys.len());

    let method6_started = Instant::now();
    let mut method6_predictions = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left_stats = &table_stats[left_index];
        let right_stats = &table_stats[right_index];
        let meta = pair_meta[pair_index];
        let predicted_rows = if table_rank(left_stats) <= table_rank(right_stats) {
            predict_rank_extension(left_stats, right_stats, meta)
        } else {
            predict_rank_extension(right_stats, left_stats, meta.flipped())
        };
        let predicted_rows = cap_predicted_rows(predicted_rows, left_stats, right_stats, meta);
        method6_predictions.push(predicted_rows);

        if (pair_index + 1) % 250_000 == 0 {
            println!(
                "method6 predictions: {}/{}",
                pair_index + 1,
                pair_keys.len()
            );
        }
    }
    let method6_timing = elapsed_report(method6_started.elapsed().as_secs_f64(), pair_keys.len());

    let exact_started = Instant::now();
    let mut exact_empty_count = 0usize;
    let mut exact_total_rows = 0f64;
    let mut exact_total_rank = 0f64;
    let mut exact_max_rows = 0usize;
    let mut exact_max_rank = 0f64;

    let mut method1_metrics = MetricsAccumulator::default();
    let mut method2_metrics = MetricsAccumulator::default();
    let mut method3_metrics = MetricsAccumulator::default();
    let mut method4_metrics = MetricsAccumulator::default();
    let mut method5_metrics = MetricsAccumulator::default();
    let mut method6_metrics = MetricsAccumulator::default();
    let mut method1_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method2_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method3_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method4_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method5_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method6_by_shared: BTreeMap<usize, MetricsAccumulator> = BTreeMap::new();
    let mut method1_worst = Vec::new();
    let mut method2_worst = Vec::new();
    let mut method3_worst = Vec::new();
    let mut method4_worst = Vec::new();
    let mut method5_worst = Vec::new();
    let mut method6_worst = Vec::new();

    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let left = &tables[left_index];
        let right = &tables[right_index];
        let meta = pair_meta[pair_index];

        let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
            .map_err(|error| anyhow!(error))
            .with_context(|| {
                format!(
                    "failed to merge pair ({left_index}, {right_index}) on schemas {:?} and {:?}",
                    left.bits, right.bits
                )
            })?;

        let actual_rows = merged.rows.len();
        let actual_rank = compute_rank(actual_rows, meta.union_arity as usize);
        exact_total_rows += actual_rows as f64;
        exact_total_rank += actual_rank;
        exact_max_rows = exact_max_rows.max(actual_rows);
        exact_max_rank = exact_max_rank.max(actual_rank);
        if actual_rows == 0 {
            exact_empty_count += 1;
        }

        let predicted1 = method1_predictions[pair_index];
        let predicted2 = method2_predictions[pair_index];
        let predicted3 = method3_predictions[pair_index];
        let predicted4 = method4_predictions[pair_index];
        let predicted5 = method5_predictions[pair_index];
        let predicted6 = method6_predictions[pair_index];

        method1_metrics.observe(predicted1, actual_rows, meta.union_arity as usize);
        method2_metrics.observe(predicted2, actual_rows, meta.union_arity as usize);
        method3_metrics.observe(predicted3, actual_rows, meta.union_arity as usize);
        method4_metrics.observe(predicted4, actual_rows, meta.union_arity as usize);
        method5_metrics.observe(predicted5, actual_rows, meta.union_arity as usize);
        method6_metrics.observe(predicted6, actual_rows, meta.union_arity as usize);
        method1_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted1, actual_rows, meta.union_arity as usize);
        method2_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted2, actual_rows, meta.union_arity as usize);
        method3_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted3, actual_rows, meta.union_arity as usize);
        method4_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted4, actual_rows, meta.union_arity as usize);
        method5_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted5, actual_rows, meta.union_arity as usize);
        method6_by_shared
            .entry(meta.shared_count as usize)
            .or_default()
            .observe(predicted6, actual_rows, meta.union_arity as usize);

        maybe_push_worst(
            &mut method1_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted1,
                actual_rows,
            ),
        );
        maybe_push_worst(
            &mut method2_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted2,
                actual_rows,
            ),
        );
        maybe_push_worst(
            &mut method3_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted3,
                actual_rows,
            ),
        );
        maybe_push_worst(
            &mut method4_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted4,
                actual_rows,
            ),
        );
        maybe_push_worst(
            &mut method5_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted5,
                actual_rows,
            ),
        );
        maybe_push_worst(
            &mut method6_worst,
            args.top_examples,
            build_worst_example(
                left_index,
                right_index,
                left,
                right,
                meta,
                predicted6,
                actual_rows,
            ),
        );

        if (pair_index + 1) % 100_000 == 0 {
            println!("exact merges: {}/{}", pair_index + 1, pair_keys.len());
        }

        debug_assert_eq!(merged.bits.len(), meta.union_arity as usize);
    }
    let exact_merge_timing = elapsed_report(exact_started.elapsed().as_secs_f64(), pair_keys.len());

    sort_worst_desc(&mut method1_worst);
    sort_worst_desc(&mut method2_worst);
    sort_worst_desc(&mut method3_worst);
    sort_worst_desc(&mut method4_worst);
    sort_worst_desc(&mut method5_worst);
    sort_worst_desc(&mut method6_worst);

    let exact_merge_summary = ExactMergeSummary {
        pair_count: pair_keys.len(),
        empty_merge_count: exact_empty_count,
        nonempty_merge_count: pair_keys.len().saturating_sub(exact_empty_count),
        mean_rows: ratio_or_zero(exact_total_rows, pair_keys.len() as f64),
        max_rows: exact_max_rows,
        mean_rank: ratio_or_zero(exact_total_rank, pair_keys.len() as f64),
        max_rank: exact_max_rank,
    };

    let predictors = vec![
        PredictorReport {
            name: "uniform_shared_bits".to_string(),
            description: "rows_pred = rows(A) * rows(B) / 2^shared_bits, then capped by exact combinatorial upper bounds".to_string(),
            timing: method1_timing,
            overall: method1_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method1_by_shared),
            worst_examples_by_q_error: method1_worst,
        },
        PredictorReport {
            name: "shared_bit_marginals".to_string(),
            description: "rows_pred = rows(A) * rows(B) * product over shared bits of per-bit assignment match probabilities, then capped by exact combinatorial upper bounds".to_string(),
            timing: method2_timing,
            overall: method2_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method2_by_shared),
            worst_examples_by_q_error: method2_worst,
        },
        PredictorReport {
            name: "shared_assignment_histogram".to_string(),
            description: "rows_pred = exact overlap count over the full shared-bit assignment histogram; no merged rows are materialized".to_string(),
            timing: method3_timing,
            overall: method3_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method3_by_shared),
            worst_examples_by_q_error: method3_worst,
        },
        PredictorReport {
            name: "rank_extension_left_to_right".to_string(),
            description: "rows_pred = rank(A)^bits(A) * rank(B)^(bits(B)-shared_bits), evaluated with A = left table and B = right table".to_string(),
            timing: method4_timing,
            overall: method4_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method4_by_shared),
            worst_examples_by_q_error: method4_worst,
        },
        PredictorReport {
            name: "rank_extension_right_to_left".to_string(),
            description: "rows_pred = rank(B)^bits(B) * rank(A)^(bits(A)-shared_bits), evaluated by swapping the pair direction".to_string(),
            timing: method5_timing,
            overall: method5_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method5_by_shared),
            worst_examples_by_q_error: method5_worst,
        },
        PredictorReport {
            name: "rank_extension_low_to_high_rank".to_string(),
            description: "rows_pred = rank(A)^bits(A) * rank(B)^(bits(B)-shared_bits), with A chosen so that rank(A) <= rank(B)".to_string(),
            timing: method6_timing,
            overall: method6_metrics.finalize(),
            by_shared_bit_count: finalize_by_shared(method6_by_shared),
            worst_examples_by_q_error: method6_worst,
        },
    ];

    let report = Report {
        method: "Evaluate two fast pairwise-merge row-count predictors on all origin-table pairs with at least one shared bit; compare to exact merges; derive rank from predicted and exact row counts".to_string(),
        input: path_string(&args.input),
        pair_filter: "all unordered pairs of origin tables with at least one shared bit".to_string(),
        prediction_cap: "pred <= min(2^union_arity, rows(A) * 2^(arity(B)-shared_bits), rows(B) * 2^(arity(A)-shared_bits))".to_string(),
        table_count: tables.len(),
        total_input_rows,
        max_input_arity,
        pair_generation_timing,
        pair_metadata_timing,
        exact_merge_timing,
        exact_merge_summary,
        predictors,
    };

    write_json(&args.report, &report)?;

    println!("report: {}", args.report.display());

    Ok(())
}

fn build_table_stats(tables: &[Table]) -> Vec<TableStats> {
    tables
        .iter()
        .map(|table| {
            let row_count = table.rows.len();
            let mut one_counts = vec![0usize; table.bits.len()];
            for &row in &table.rows {
                for (local_index, count) in one_counts.iter_mut().enumerate() {
                    *count += ((row >> local_index) & 1) as usize;
                }
            }
            let one_probs = if row_count == 0 {
                vec![0.0; table.bits.len()]
            } else {
                one_counts
                    .into_iter()
                    .map(|count| count as f64 / row_count as f64)
                    .collect()
            };
            TableStats {
                row_count,
                arity: table.bits.len(),
                one_probs,
            }
        })
        .collect()
}

fn canonicalize_tables(tables: Vec<Table>) -> Vec<Table> {
    tables.into_iter().map(canonicalize_table).collect()
}

fn canonicalize_table(mut table: Table) -> Table {
    let mut order: Vec<usize> = (0..table.bits.len()).collect();
    order.sort_unstable_by_key(|&index| table.bits[index]);

    if order
        .iter()
        .copied()
        .enumerate()
        .all(|(new_index, old_index)| new_index == old_index)
    {
        sort_dedup_rows(&mut table.rows);
        return table;
    }

    let mut inverse = vec![0usize; order.len()];
    for (new_index, old_index) in order.iter().copied().enumerate() {
        inverse[old_index] = new_index;
    }

    let old_bits = table.bits;
    let old_rows = table.rows;
    let new_bits: Vec<u32> = order.iter().map(|&index| old_bits[index]).collect();
    let mut new_rows = Vec::with_capacity(old_rows.len());
    for row in old_rows {
        let mut remapped = 0u32;
        for (old_index, &new_index) in inverse.iter().enumerate() {
            if ((row >> old_index) & 1) != 0 {
                remapped |= 1u32 << new_index;
            }
        }
        new_rows.push(remapped);
    }
    sort_dedup_rows(&mut new_rows);

    Table {
        bits: new_bits,
        rows: new_rows,
    }
}

fn generate_shared_pairs(tables: &[Table]) -> Vec<u64> {
    let mut bit_to_tables: HashMap<u32, Vec<usize>> = HashMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for &bit in &table.bits {
            bit_to_tables.entry(bit).or_default().push(table_index);
        }
    }

    let mut pair_keys = HashSet::new();
    for table_ids in bit_to_tables.values() {
        for left_offset in 0..table_ids.len() {
            for right_offset in (left_offset + 1)..table_ids.len() {
                pair_keys.insert(pair_key(
                    table_ids[left_offset] as u32,
                    table_ids[right_offset] as u32,
                ));
            }
        }
    }

    let mut pair_keys: Vec<u64> = pair_keys.into_iter().collect();
    pair_keys.sort_unstable();
    pair_keys
}

fn build_pair_meta(tables: &[Table], pair_keys: &[u64]) -> Result<Vec<PairMeta>> {
    let mut meta = Vec::with_capacity(pair_keys.len());
    for (pair_index, &pair_key) in pair_keys.iter().enumerate() {
        let (left_index, right_index) = unpack_pair_key(pair_key);
        let (shared_count, union_arity) =
            shared_count_and_union_arity(&tables[left_index].bits, &tables[right_index].bits);
        if shared_count == 0 {
            bail!("generated pair ({left_index}, {right_index}) without shared bits");
        }
        meta.push(PairMeta {
            shared_count: shared_count as u8,
            union_arity: union_arity as u8,
        });

        if (pair_index + 1) % 500_000 == 0 {
            println!("pair metadata: {}/{}", pair_index + 1, pair_keys.len());
        }
    }
    Ok(meta)
}

fn shared_count_and_union_arity(left_bits: &[u32], right_bits: &[u32]) -> (usize, usize) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut shared_count = 0usize;

    while left_index < left_bits.len() && right_index < right_bits.len() {
        match left_bits[left_index].cmp(&right_bits[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                shared_count += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }

    (shared_count, left_bits.len() + right_bits.len() - shared_count)
}

fn predict_uniform_shared_bits(left: &TableStats, right: &TableStats, meta: PairMeta) -> f64 {
    (left.row_count as f64 * right.row_count as f64) / pow2_f64(meta.shared_count as usize)
}

fn predict_marginal_shared_bits(
    left: &Table,
    right: &Table,
    left_stats: &TableStats,
    right_stats: &TableStats,
    meta: PairMeta,
) -> f64 {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut match_probability_product = 1.0;
    let mut seen_shared = 0usize;

    while left_index < left.bits.len() && right_index < right.bits.len() {
        match left.bits[left_index].cmp(&right.bits[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                let left_p = left_stats.one_probs[left_index];
                let right_p = right_stats.one_probs[right_index];
                let match_probability =
                    left_p * right_p + (1.0 - left_p) * (1.0 - right_p);
                match_probability_product *= match_probability;
                seen_shared += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }

    debug_assert_eq!(seen_shared, meta.shared_count as usize);

    left.rows.len() as f64 * right.rows.len() as f64 * match_probability_product
}

fn predict_rank_extension(base: &TableStats, ext: &TableStats, meta: PairMeta) -> f64 {
    let ext_nonshared = ext.arity.saturating_sub(meta.shared_count as usize);
    base.row_count as f64 * table_rank(ext).powf(ext_nonshared as f64)
}

fn table_rank(table: &TableStats) -> f64 {
    compute_rank(table.row_count, table.arity)
}

fn predict_shared_assignment_histogram(left: &Table, right: &Table) -> f64 {
    let (left_shared_indices, right_shared_indices) = shared_index_lists(&left.bits, &right.bits);
    let shared_count = left_shared_indices.len();
    debug_assert!(shared_count > 0);

    let (build_rows, build_indices, probe_rows, probe_indices) = if left.rows.len() <= right.rows.len()
    {
        (&left.rows, &left_shared_indices, &right.rows, &right_shared_indices)
    } else {
        (&right.rows, &right_shared_indices, &left.rows, &left_shared_indices)
    };

    if shared_count <= 16 {
        let mut counts = vec![0u32; 1usize << shared_count];
        for &row in build_rows {
            let key = project_bits(row, build_indices) as usize;
            counts[key] += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices) as usize;
            total += counts[key] as u64;
        }
        total as f64
    } else {
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for &row in build_rows {
            let key = project_bits(row, build_indices);
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut total = 0u64;
        for &row in probe_rows {
            let key = project_bits(row, probe_indices);
            total += counts.get(&key).copied().unwrap_or(0) as u64;
        }
        total as f64
    }
}

fn cap_predicted_rows(
    predicted_rows: f64,
    left: &TableStats,
    right: &TableStats,
    meta: PairMeta,
) -> f64 {
    let shared_count = meta.shared_count as usize;
    let union_arity = meta.union_arity as usize;
    predicted_rows
        .max(0.0)
        .min(pow2_f64(union_arity))
        .min(left.row_count as f64 * pow2_f64(right.arity.saturating_sub(shared_count)))
        .min(right.row_count as f64 * pow2_f64(left.arity.saturating_sub(shared_count)))
}

fn finalize_by_shared(by_shared: BTreeMap<usize, MetricsAccumulator>) -> Vec<SharedCountReport> {
    by_shared
        .into_iter()
        .map(|(shared_bit_count, metrics)| SharedCountReport {
            shared_bit_count,
            metrics: metrics.finalize(),
        })
        .collect()
}

fn shared_index_lists(left_bits: &[u32], right_bits: &[u32]) -> (Vec<u8>, Vec<u8>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut left_shared_indices = Vec::new();
    let mut right_shared_indices = Vec::new();

    while left_index < left_bits.len() && right_index < right_bits.len() {
        match left_bits[left_index].cmp(&right_bits[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                left_shared_indices.push(left_index as u8);
                right_shared_indices.push(right_index as u8);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    (left_shared_indices, right_shared_indices)
}

fn project_bits(row: u32, indices: &[u8]) -> u32 {
    let mut projected = 0u32;
    for (new_pos, &old_pos) in indices.iter().enumerate() {
        projected |= ((row >> old_pos) & 1) << new_pos;
    }
    projected
}

fn build_worst_example(
    left_index: usize,
    right_index: usize,
    left: &Table,
    right: &Table,
    meta: PairMeta,
    predicted_rows: f64,
    actual_rows: usize,
) -> WorstExample {
    let predicted_rank = compute_rank_float(predicted_rows, meta.union_arity as usize);
    let actual_rank = compute_rank(actual_rows, meta.union_arity as usize);
    WorstExample {
        left_index,
        right_index,
        left_arity: left.bits.len(),
        right_arity: right.bits.len(),
        left_rows: left.rows.len(),
        right_rows: right.rows.len(),
        shared_bit_count: meta.shared_count as usize,
        result_arity: meta.union_arity as usize,
        predicted_rows,
        actual_rows,
        predicted_rank,
        actual_rank,
        q_error_plus1: q_error_plus1(predicted_rows, actual_rows as f64),
        abs_log2_rows_plus1_error: (log2_plus1(predicted_rows) - log2_plus1(actual_rows as f64))
            .abs(),
        abs_rank_error: (predicted_rank - actual_rank).abs(),
    }
}

fn maybe_push_worst(examples: &mut Vec<WorstExample>, limit: usize, candidate: WorstExample) {
    if limit == 0 {
        return;
    }
    examples.push(candidate);
    sort_worst_desc(examples);
    if examples.len() > limit {
        examples.truncate(limit);
    }
}

fn sort_worst_desc(examples: &mut [WorstExample]) {
    examples.sort_by(|left, right| {
        right
            .q_error_plus1
            .total_cmp(&left.q_error_plus1)
            .then_with(|| {
                right
                    .abs_log2_rows_plus1_error
                    .total_cmp(&left.abs_log2_rows_plus1_error)
            })
    });
}

fn pair_key(left: u32, right: u32) -> u64 {
    let (left, right) = if left < right {
        (left as u64, right as u64)
    } else {
        (right as u64, left as u64)
    };
    (left << 32) | right
}

fn unpack_pair_key(key: u64) -> (usize, usize) {
    ((key >> 32) as usize, key as u32 as usize)
}

fn compute_rank_float(row_count: f64, bit_count: usize) -> f64 {
    if row_count <= 0.0 {
        0.0
    } else {
        row_count.powf(1.0 / bit_count as f64)
    }
}

fn q_error_plus1(predicted_rows: f64, actual_rows: f64) -> f64 {
    let predicted = predicted_rows + 1.0;
    let actual = actual_rows + 1.0;
    if predicted >= actual {
        predicted / actual
    } else {
        actual / predicted
    }
}

fn log2_plus1(value: f64) -> f64 {
    (value + 1.0).log2()
}

fn percentile_sorted(sorted_values: &[f64], quantile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let index = ((sorted_values.len() - 1) as f64 * quantile).round() as usize;
    sorted_values[index]
}

fn pow2_f64(exponent: usize) -> f64 {
    if exponent > i32::MAX as usize {
        f64::INFINITY
    } else {
        2.0_f64.powi(exponent as i32)
    }
}

fn elapsed_report(seconds: f64, pair_count: usize) -> TimingReport {
    if pair_count == 0 {
        return TimingReport {
            seconds,
            nanos_per_pair: 0.0,
            pairs_per_second: 0.0,
        };
    }
    TimingReport {
        seconds,
        nanos_per_pair: seconds * 1_000_000_000.0 / pair_count as f64,
        pairs_per_second: pair_count as f64 / seconds.max(f64::MIN_POSITIVE),
    }
}

fn ratio_or_zero(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn expect_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn print_usage() {
    println!(
        "usage: cargo run --release --bin merge_prediction_eval -- --input <path> --report <path> [--top-examples <n>]"
    );
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
