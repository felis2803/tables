use std::collections::BTreeMap;

use serde::Serialize;

use crate::common::{is_full_row_set, Table};

#[derive(Clone, Debug, Default, Serialize)]
pub struct TautologyFilterInfo {
    pub removed_tables: usize,
    pub removed_rows: usize,
    pub removed_zero_bit_tables: usize,
    pub removed_by_arity: BTreeMap<String, usize>,
}

pub fn filter_tautologies(tables: Vec<Table>) -> (Vec<Table>, TautologyFilterInfo) {
    let mut kept = Vec::with_capacity(tables.len());
    let mut info = TautologyFilterInfo::default();

    for table in tables {
        if is_full_row_set(table.rows.len(), table.bits.len()) {
            info.removed_tables += 1;
            info.removed_rows += table.rows.len();
            if table.bits.is_empty() {
                info.removed_zero_bit_tables += 1;
            }
            *info
                .removed_by_arity
                .entry(table.bits.len().to_string())
                .or_insert(0) += 1;
            continue;
        }

        kept.push(table);
    }

    (kept, info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_tautologies_removes_full_row_sets() {
        let tables = vec![
            Table {
                bits: vec![1],
                rows: vec![0, 1],
            },
            Table {
                bits: vec![2, 3],
                rows: vec![0, 1, 2],
            },
            Table {
                bits: vec![],
                rows: vec![0],
            },
        ];

        let (kept, info) = filter_tautologies(tables);

        assert_eq!(
            kept,
            vec![Table {
                bits: vec![2, 3],
                rows: vec![0, 1, 2],
            }]
        );
        assert_eq!(info.removed_tables, 2);
        assert_eq!(info.removed_rows, 3);
        assert_eq!(info.removed_zero_bit_tables, 1);
        assert_eq!(info.removed_by_arity.get("0"), Some(&1));
        assert_eq!(info.removed_by_arity.get("1"), Some(&1));
    }
}
