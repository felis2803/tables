use std::collections::HashSet;

use serde::Serialize;

use crate::common::Table;

const DENSE_FAST_PATH_MAX_BITS: usize = 20;
const FAST_PATH_MIN_ROWS: usize = 64;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BitZeroCollapseMetric {
    pub bit: u32,
    pub bit_index: usize,
    pub row_count_before: usize,
    pub row_count_after_zeroing: usize,
    pub collapsed_rows: usize,
    pub zero_collapse: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TableBitZeroCollapseReport {
    pub metric_name: String,
    pub metric_definition: String,
    pub bit_count: usize,
    pub row_count_before: usize,
    pub max_zero_collapse: f64,
    pub mean_zero_collapse: f64,
    pub metrics: Vec<BitZeroCollapseMetric>,
}

pub fn compute_bit_zero_collapse_metrics(table: &Table) -> Vec<BitZeroCollapseMetric> {
    let row_count_before = table.rows.len();
    let mut metrics = Vec::with_capacity(table.bits.len());
    let collapsed_rows_by_bit = compute_collapsed_rows_by_bit(&table.rows, table.bits.len());

    for (bit_index, &bit) in table.bits.iter().enumerate() {
        let collapsed_rows = collapsed_rows_by_bit[bit_index];
        let row_count_after_zeroing = row_count_before.saturating_sub(collapsed_rows);
        let zero_collapse = if row_count_before == 0 {
            0.0
        } else {
            collapsed_rows as f64 / row_count_before as f64
        };

        metrics.push(BitZeroCollapseMetric {
            bit,
            bit_index,
            row_count_before,
            row_count_after_zeroing,
            collapsed_rows,
            zero_collapse,
        });
    }

    metrics
}

fn compute_collapsed_rows_by_bit(rows: &[u32], bit_count: usize) -> Vec<usize> {
    if rows.is_empty() || bit_count == 0 {
        return vec![0; bit_count];
    }

    if rows.len() < FAST_PATH_MIN_ROWS {
        return compute_collapsed_rows_naive(rows, bit_count);
    }

    if bit_count <= DENSE_FAST_PATH_MAX_BITS {
        return compute_collapsed_rows_dense(rows, bit_count);
    }

    compute_collapsed_rows_sparse(rows, bit_count)
}

fn compute_collapsed_rows_naive(rows: &[u32], bit_count: usize) -> Vec<usize> {
    let row_count_before = rows.len();
    let mut collapsed_rows_by_bit = Vec::with_capacity(bit_count);

    for bit_index in 0..bit_count {
        let zero_mask = !(1u32 << bit_index);
        let mut zeroed_rows = Vec::with_capacity(row_count_before);
        for &row in rows {
            zeroed_rows.push(row & zero_mask);
        }
        crate::common::sort_dedup_rows(&mut zeroed_rows);
        collapsed_rows_by_bit.push(row_count_before.saturating_sub(zeroed_rows.len()));
    }

    collapsed_rows_by_bit
}

fn compute_collapsed_rows_dense(rows: &[u32], bit_count: usize) -> Vec<usize> {
    let domain_size = 1usize << bit_count;
    let mut occupancy = vec![0u64; domain_size.div_ceil(64)];
    for &row in rows {
        set_bit(&mut occupancy, row as usize);
    }

    let mut collapsed_rows_by_bit = vec![0usize; bit_count];
    for (bit_index, collapsed_rows) in collapsed_rows_by_bit.iter_mut().enumerate() {
        let mask = 1u32 << bit_index;
        let mut pair_count = 0usize;

        // On canonical deduplicated rows, zeroing bit i collapses exactly the
        // pairs of rows that differ only on i.
        for &row in rows {
            if (row & mask) == 0 && get_bit(&occupancy, (row | mask) as usize) {
                pair_count += 1;
            }
        }

        *collapsed_rows = pair_count;
    }

    collapsed_rows_by_bit
}

fn compute_collapsed_rows_sparse(rows: &[u32], bit_count: usize) -> Vec<usize> {
    let row_set: HashSet<u32> = rows.iter().copied().collect();
    let mut collapsed_rows_by_bit = vec![0usize; bit_count];

    for (bit_index, collapsed_rows) in collapsed_rows_by_bit.iter_mut().enumerate() {
        let mask = 1u32 << bit_index;
        let mut pair_count = 0usize;

        for &row in rows {
            if (row & mask) == 0 && row_set.contains(&(row | mask)) {
                pair_count += 1;
            }
        }

        *collapsed_rows = pair_count;
    }

    collapsed_rows_by_bit
}

fn set_bit(bits: &mut [u64], index: usize) {
    bits[index / 64] |= 1u64 << (index % 64);
}

fn get_bit(bits: &[u64], index: usize) -> bool {
    ((bits[index / 64] >> (index % 64)) & 1) != 0
}

pub fn build_table_bit_zero_collapse_report(table: &Table) -> TableBitZeroCollapseReport {
    let metrics = compute_bit_zero_collapse_metrics(table);
    let max_zero_collapse = metrics
        .iter()
        .map(|metric| metric.zero_collapse)
        .fold(0.0, f64::max);
    let mean_zero_collapse = if metrics.is_empty() {
        0.0
    } else {
        metrics
            .iter()
            .map(|metric| metric.zero_collapse)
            .sum::<f64>()
            / metrics.len() as f64
    };

    TableBitZeroCollapseReport {
        metric_name: "zero-collapse".to_string(),
        metric_definition:
            "zero-collapse(bit) = (row_count_before - row_count_after_zeroing_and_dedup) / row_count_before".to_string(),
        bit_count: table.bits.len(),
        row_count_before: table.rows.len(),
        max_zero_collapse,
        mean_zero_collapse,
        metrics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compute_bit_zero_collapse_metrics_naive(table: &Table) -> Vec<BitZeroCollapseMetric> {
        let row_count_before = table.rows.len();
        let mut metrics = Vec::with_capacity(table.bits.len());

        for (bit_index, &bit) in table.bits.iter().enumerate() {
            let zero_mask = !(1u32 << bit_index);
            let mut zeroed_rows = Vec::with_capacity(table.rows.len());
            for &row in &table.rows {
                zeroed_rows.push(row & zero_mask);
            }
            crate::common::sort_dedup_rows(&mut zeroed_rows);

            let row_count_after_zeroing = zeroed_rows.len();
            let collapsed_rows = row_count_before.saturating_sub(row_count_after_zeroing);
            let zero_collapse = if row_count_before == 0 {
                0.0
            } else {
                collapsed_rows as f64 / row_count_before as f64
            };

            metrics.push(BitZeroCollapseMetric {
                bit,
                bit_index,
                row_count_before,
                row_count_after_zeroing,
                collapsed_rows,
                zero_collapse,
            });
        }

        metrics
    }

    #[test]
    fn compute_bit_zero_collapse_metrics_measures_row_collapse() {
        let table = Table {
            bits: vec![3, 5],
            rows: vec![0b00, 0b01, 0b10, 0b11],
        };

        let metrics = compute_bit_zero_collapse_metrics(&table);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].bit, 3);
        assert_eq!(metrics[0].row_count_after_zeroing, 2);
        assert_eq!(metrics[0].collapsed_rows, 2);
        assert!((metrics[0].zero_collapse - 0.5).abs() < 1e-12);
        assert_eq!(metrics[1].bit, 5);
        assert_eq!(metrics[1].row_count_after_zeroing, 2);
        assert_eq!(metrics[1].collapsed_rows, 2);
        assert!((metrics[1].zero_collapse - 0.5).abs() < 1e-12);
    }

    #[test]
    fn compute_bit_zero_collapse_metrics_handles_irrelevant_bit() {
        let table = Table {
            bits: vec![7, 9],
            rows: vec![0b00, 0b01],
        };

        let metrics = compute_bit_zero_collapse_metrics(&table);
        assert_eq!(metrics[0].collapsed_rows, 1);
        assert_eq!(metrics[0].row_count_after_zeroing, 1);
        assert!((metrics[0].zero_collapse - 0.5).abs() < 1e-12);
        assert_eq!(metrics[1].collapsed_rows, 0);
        assert_eq!(metrics[1].row_count_after_zeroing, 2);
        assert_eq!(metrics[1].zero_collapse, 0.0);
    }

    #[test]
    fn build_table_bit_zero_collapse_report_summarizes_metrics() {
        let table = Table {
            bits: vec![1],
            rows: vec![0, 1],
        };

        let report = build_table_bit_zero_collapse_report(&table);
        assert_eq!(report.metric_name, "zero-collapse");
        assert_eq!(report.bit_count, 1);
        assert_eq!(report.row_count_before, 2);
        assert!((report.max_zero_collapse - 0.5).abs() < 1e-12);
        assert!((report.mean_zero_collapse - 0.5).abs() < 1e-12);
    }

    #[test]
    fn fast_metrics_match_naive_metrics_for_all_subsets_of_arity_three() {
        for subset_mask in 0u32..(1u32 << 8) {
            let mut rows = Vec::new();
            for row in 0u32..8 {
                if ((subset_mask >> row) & 1) != 0 {
                    rows.push(row);
                }
            }

            let table = Table {
                bits: vec![10, 20, 30],
                rows,
            };

            let actual = compute_bit_zero_collapse_metrics(&table);
            let expected = compute_bit_zero_collapse_metrics_naive(&table);
            assert_eq!(actual, expected, "subset_mask={subset_mask}");
        }
    }
}
