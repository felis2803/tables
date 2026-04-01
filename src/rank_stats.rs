use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::common::Table;

#[derive(Clone, Debug, Serialize)]
pub struct RankByArity {
    pub bit_count: usize,
    pub table_count: usize,
    pub min_rank: f64,
    pub max_rank: f64,
    pub mean_rank: f64,
    pub median_rank: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopSignature {
    pub bit_count: usize,
    pub row_count: usize,
    pub rank: f64,
    pub table_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct RankSummary {
    pub metric: String,
    pub table_count: usize,
    pub min_rank: f64,
    pub max_rank: f64,
    pub mean_rank: f64,
    pub median_rank: f64,
    pub unique_signatures: usize,
    pub by_arity: Vec<RankByArity>,
    pub top_signatures: Vec<TopSignature>,
}

pub fn compute_rank(row_count: usize, bit_count: usize) -> f64 {
    (row_count as f64).powf(1.0 / bit_count as f64)
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn median(values: &[f64]) -> f64 {
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

pub fn summarize_rank_pairs<I>(pairs: I, topn: usize) -> RankSummary
where
    I: IntoIterator<Item = (usize, usize)>,
{
    let mut grouped: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
    let mut signature_counts: HashMap<(usize, usize), usize> = HashMap::new();
    let mut ranks = Vec::new();

    for (bit_count, row_count) in pairs {
        let rank = compute_rank(row_count, bit_count);
        grouped.entry(bit_count).or_default().push(rank);
        *signature_counts.entry((bit_count, row_count)).or_insert(0) += 1;
        ranks.push(rank);
    }

    if ranks.is_empty() {
        return RankSummary {
            metric: "rank = row_count ** (1 / bit_count)".to_string(),
            table_count: 0,
            min_rank: 0.0,
            max_rank: 0.0,
            mean_rank: 0.0,
            median_rank: 0.0,
            unique_signatures: 0,
            by_arity: Vec::new(),
            top_signatures: Vec::new(),
        };
    }

    ranks.sort_by(|left, right| left.total_cmp(right));

    let by_arity = grouped
        .into_iter()
        .map(|(bit_count, mut arity_ranks)| {
            arity_ranks.sort_by(|left, right| left.total_cmp(right));
            RankByArity {
                bit_count,
                table_count: arity_ranks.len(),
                min_rank: *arity_ranks.first().unwrap(),
                max_rank: *arity_ranks.last().unwrap(),
                mean_rank: mean(&arity_ranks),
                median_rank: median(&arity_ranks),
            }
        })
        .collect();

    let mut top_signatures_raw: Vec<_> = signature_counts.into_iter().collect();
    top_signatures_raw.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| left.0 .0.cmp(&right.0 .0))
            .then_with(|| left.0 .1.cmp(&right.0 .1))
    });
    let unique_signatures = top_signatures_raw.len();

    let top_signatures = top_signatures_raw
        .into_iter()
        .take(topn)
        .map(|((bit_count, row_count), table_count)| TopSignature {
            bit_count,
            row_count,
            rank: compute_rank(row_count, bit_count),
            table_count,
        })
        .collect();

    RankSummary {
        metric: "rank = row_count ** (1 / bit_count)".to_string(),
        table_count: ranks.len(),
        min_rank: *ranks.first().unwrap(),
        max_rank: *ranks.last().unwrap(),
        mean_rank: mean(&ranks),
        median_rank: median(&ranks),
        unique_signatures,
        by_arity,
        top_signatures,
    }
}

pub fn summarize_table_ranks(tables: &[Table], topn: usize) -> RankSummary {
    summarize_rank_pairs(
        tables
            .iter()
            .map(|table| (table.bits.len(), table.rows.len())),
        topn,
    )
}
