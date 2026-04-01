#![allow(dead_code)]

use std::collections::HashMap;

#[cfg(test)]
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table32 {
    pub bits: Vec<u32>,
    pub rows: Vec<u32>,
}

#[derive(Clone, Copy, Debug)]
struct LocalToUnion {
    union_pos: u8,
}

#[derive(Clone, Debug)]
struct MergeShape {
    union_bits: Vec<u32>,
    left_to_union: Vec<LocalToUnion>,
    right_to_union: Vec<LocalToUnion>,
    left_shared_indices: Vec<u8>,
    right_shared_indices: Vec<u8>,
    left_unique_mask: u32,
    right_unique_mask: u32,
}

fn full_mask(width: usize) -> Result<u32, String> {
    if width > 32 {
        return Err(format!("arity {width} exceeds uint32 width"));
    }
    Ok(if width == 32 {
        u32::MAX
    } else {
        (1u32 << width) - 1
    })
}

fn validate_table_slices(bits: &[u32], rows: &[u32]) -> Result<(), String> {
    if bits.is_empty() {
        return Err("table must contain at least one bit".to_string());
    }
    if bits.len() > 32 {
        return Err(format!(
            "table arity {} exceeds uint32 row width limit",
            bits.len()
        ));
    }
    if bits.windows(2).any(|window| window[0] >= window[1]) {
        return Err(format!("bits are not strictly increasing: {:?}", bits));
    }

    let mask = full_mask(bits.len())?;
    let mut previous: Option<u32> = None;
    for &row in rows {
        if row & !mask != 0 {
            return Err(format!(
                "row value {row} exceeds arity {} mask {mask}",
                bits.len()
            ));
        }
        if previous == Some(row) {
            return Err(format!("rows are not deduplicated in {:?}", bits));
        }
        if let Some(prev) = previous {
            if row < prev {
                return Err(format!("rows are not sorted in {:?}", bits));
            }
        }
        previous = Some(row);
    }
    Ok(())
}

fn validate_table(table: &Table32) -> Result<(), String> {
    validate_table_slices(&table.bits, &table.rows)
}

fn build_merge_shape(left_bits: &[u32], right_bits: &[u32]) -> Result<MergeShape, String> {
    let mut union_bits = Vec::with_capacity(left_bits.len() + right_bits.len());
    let mut left_to_union = vec![LocalToUnion { union_pos: 0 }; left_bits.len()];
    let mut right_to_union = vec![LocalToUnion { union_pos: 0 }; right_bits.len()];
    let mut left_shared_indices = Vec::new();
    let mut right_shared_indices = Vec::new();
    let mut left_unique_mask = 0u32;
    let mut right_unique_mask = 0u32;

    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut union_index = 0usize;

    while left_index < left_bits.len() || right_index < right_bits.len() {
        if union_index >= 32 {
            return Err(format!(
                "merged arity exceeds uint32 width: {} + {} bits",
                left_bits.len(),
                right_bits.len()
            ));
        }

        match (
            left_bits.get(left_index).copied(),
            right_bits.get(right_index).copied(),
        ) {
            (Some(left_bit), Some(right_bit)) if left_bit == right_bit => {
                union_bits.push(left_bit);
                left_to_union[left_index].union_pos = union_index as u8;
                right_to_union[right_index].union_pos = union_index as u8;
                left_shared_indices.push(left_index as u8);
                right_shared_indices.push(right_index as u8);
                left_index += 1;
                right_index += 1;
                union_index += 1;
            }
            (Some(left_bit), Some(right_bit)) if left_bit < right_bit => {
                union_bits.push(left_bit);
                left_to_union[left_index].union_pos = union_index as u8;
                left_unique_mask |= 1u32 << left_index;
                left_index += 1;
                union_index += 1;
            }
            (Some(_), Some(right_bit)) => {
                union_bits.push(right_bit);
                right_to_union[right_index].union_pos = union_index as u8;
                right_unique_mask |= 1u32 << right_index;
                right_index += 1;
                union_index += 1;
            }
            (Some(left_bit), None) => {
                union_bits.push(left_bit);
                left_to_union[left_index].union_pos = union_index as u8;
                left_unique_mask |= 1u32 << left_index;
                left_index += 1;
                union_index += 1;
            }
            (None, Some(right_bit)) => {
                union_bits.push(right_bit);
                right_to_union[right_index].union_pos = union_index as u8;
                right_unique_mask |= 1u32 << right_index;
                right_index += 1;
                union_index += 1;
            }
            (None, None) => break,
        }
    }

    Ok(MergeShape {
        union_bits,
        left_to_union,
        right_to_union,
        left_shared_indices,
        right_shared_indices,
        left_unique_mask,
        right_unique_mask,
    })
}

fn project_bits(row: u32, indices: &[u8]) -> u32 {
    let mut projected = 0u32;
    for (new_pos, &old_pos) in indices.iter().enumerate() {
        projected |= ((row >> old_pos) & 1) << new_pos;
    }
    projected
}

fn remap_row(mut row: u32, local_to_union: &[LocalToUnion]) -> u32 {
    let mut remapped = 0u32;
    while row != 0 {
        let local_pos = row.trailing_zeros() as usize;
        remapped |= 1u32 << local_to_union[local_pos].union_pos;
        row &= row - 1;
    }
    remapped
}

fn merge_with_dense_buckets(
    build_rows: &[u32],
    build_shared_indices: &[u8],
    build_unique_mask: u32,
    build_to_union: &[LocalToUnion],
    probe_rows: &[u32],
    probe_shared_indices: &[u8],
    probe_to_union: &[LocalToUnion],
) -> Vec<u32> {
    let bucket_count = 1usize << build_shared_indices.len();
    let mut buckets: Vec<Vec<u32>> = (0..bucket_count).map(|_| Vec::new()).collect();

    for &row in build_rows {
        let key = project_bits(row, build_shared_indices) as usize;
        let payload = remap_row(row & build_unique_mask, build_to_union);
        buckets[key].push(payload);
    }

    let mut merged_rows = Vec::new();
    for &row in probe_rows {
        let key = project_bits(row, probe_shared_indices) as usize;
        let base = remap_row(row, probe_to_union);
        for &payload in &buckets[key] {
            merged_rows.push(base | payload);
        }
    }
    merged_rows
}

fn merge_with_sparse_buckets(
    build_rows: &[u32],
    build_shared_indices: &[u8],
    build_unique_mask: u32,
    build_to_union: &[LocalToUnion],
    probe_rows: &[u32],
    probe_shared_indices: &[u8],
    probe_to_union: &[LocalToUnion],
) -> Vec<u32> {
    let mut buckets: HashMap<u32, Vec<u32>> = HashMap::new();
    for &row in build_rows {
        let key = project_bits(row, build_shared_indices);
        let payload = remap_row(row & build_unique_mask, build_to_union);
        buckets.entry(key).or_default().push(payload);
    }

    let mut merged_rows = Vec::new();
    for &row in probe_rows {
        let key = project_bits(row, probe_shared_indices);
        let base = remap_row(row, probe_to_union);
        if let Some(payloads) = buckets.get(&key) {
            for &payload in payloads {
                merged_rows.push(base | payload);
            }
        }
    }
    merged_rows
}

pub fn merge_tables_fast_from_slices(
    left_bits: &[u32],
    left_rows: &[u32],
    right_bits: &[u32],
    right_rows: &[u32],
) -> Result<Table32, String> {
    validate_table_slices(left_bits, left_rows)?;
    validate_table_slices(right_bits, right_rows)?;

    let shape = build_merge_shape(left_bits, right_bits)?;
    let shared_count = shape.left_shared_indices.len();

    let mut rows = if left_rows.len() <= right_rows.len() {
        if shared_count <= 16 {
            merge_with_dense_buckets(
                left_rows,
                &shape.left_shared_indices,
                shape.left_unique_mask,
                &shape.left_to_union,
                right_rows,
                &shape.right_shared_indices,
                &shape.right_to_union,
            )
        } else {
            merge_with_sparse_buckets(
                left_rows,
                &shape.left_shared_indices,
                shape.left_unique_mask,
                &shape.left_to_union,
                right_rows,
                &shape.right_shared_indices,
                &shape.right_to_union,
            )
        }
    } else if shared_count <= 16 {
        merge_with_dense_buckets(
            right_rows,
            &shape.right_shared_indices,
            shape.right_unique_mask,
            &shape.right_to_union,
            left_rows,
            &shape.left_shared_indices,
            &shape.left_to_union,
        )
    } else {
        merge_with_sparse_buckets(
            right_rows,
            &shape.right_shared_indices,
            shape.right_unique_mask,
            &shape.right_to_union,
            left_rows,
            &shape.left_shared_indices,
            &shape.left_to_union,
        )
    };

    rows.sort_unstable();
    rows.dedup();

    Ok(Table32 {
        bits: shape.union_bits,
        rows,
    })
}

pub fn merge_tables_fast(left: &Table32, right: &Table32) -> Result<Table32, String> {
    validate_table(left)?;
    validate_table(right)?;
    merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(bits: &[u32], rows: &[u32]) -> Table32 {
        let mut sorted_rows = rows.to_vec();
        sorted_rows.sort_unstable();
        sorted_rows.dedup();
        Table32 {
            bits: bits.to_vec(),
            rows: sorted_rows,
        }
    }

    fn project_to_bits(row: u32, source_bits: &[u32], target_bits: &[u32]) -> u32 {
        let mut result = 0u32;
        for (target_pos, &bit) in target_bits.iter().enumerate() {
            let source_pos = source_bits.binary_search(&bit).unwrap();
            result |= ((row >> source_pos) & 1) << target_pos;
        }
        result
    }

    fn merge_tables_bruteforce(left: &Table32, right: &Table32) -> Table32 {
        let union_bits: Vec<u32> = left
            .bits
            .iter()
            .chain(right.bits.iter())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let mut rows = BTreeSet::new();
        for &left_row in &left.rows {
            for &right_row in &right.rows {
                let mut consistent = true;
                for &bit in &left.bits {
                    if right.bits.binary_search(&bit).is_ok() {
                        let left_value = (left_row >> left.bits.binary_search(&bit).unwrap()) & 1;
                        let right_value =
                            (right_row >> right.bits.binary_search(&bit).unwrap()) & 1;
                        if left_value != right_value {
                            consistent = false;
                            break;
                        }
                    }
                }
                if !consistent {
                    continue;
                }

                let mut merged_row = 0u32;
                for (union_pos, &bit) in union_bits.iter().enumerate() {
                    let value = if let Ok(pos) = left.bits.binary_search(&bit) {
                        (left_row >> pos) & 1
                    } else {
                        let pos = right.bits.binary_search(&bit).unwrap();
                        (right_row >> pos) & 1
                    };
                    merged_row |= value << union_pos;
                }
                rows.insert(merged_row);
            }
        }

        Table32 {
            bits: union_bits,
            rows: rows.into_iter().collect(),
        }
    }

    #[test]
    fn merge_disjoint_tables_is_cartesian_product() {
        let left = table(&[1, 4], &[0b00, 0b10]);
        let right = table(&[2], &[0b0, 0b1]);

        let merged = merge_tables_fast(&left, &right).unwrap();

        assert_eq!(merged.bits, vec![1, 2, 4]);
        assert_eq!(merged.rows, vec![0b000, 0b010, 0b100, 0b110]);
    }

    #[test]
    fn merge_over_shared_bit_filters_inconsistent_pairs() {
        let left = table(&[1, 3], &[0b00, 0b01, 0b11]);
        let right = table(&[3, 5], &[0b00, 0b10, 0b11]);

        let merged = merge_tables_fast(&left, &right).unwrap();

        assert_eq!(merged.bits, vec![1, 3, 5]);
        assert_eq!(merged.rows, vec![0b000, 0b001, 0b100, 0b101, 0b111]);
    }

    #[test]
    fn merge_identical_schemas_is_row_intersection() {
        let left = table(&[2, 7, 9], &[0b000, 0b011, 0b101, 0b111]);
        let right = table(&[2, 7, 9], &[0b011, 0b110, 0b111]);

        let merged = merge_tables_fast(&left, &right).unwrap();

        assert_eq!(merged.bits, vec![2, 7, 9]);
        assert_eq!(merged.rows, vec![0b011, 0b111]);
    }

    #[test]
    fn merge_can_produce_empty_table() {
        let left = table(&[1], &[0b0]);
        let right = table(&[1], &[0b1]);

        let merged = merge_tables_fast(&left, &right).unwrap();

        assert_eq!(merged.bits, vec![1]);
        assert!(merged.rows.is_empty());
    }

    #[test]
    fn merge_matches_bruteforce_on_handcrafted_case() {
        let left = table(&[0, 2, 4], &[0b000, 0b011, 0b101, 0b111]);
        let right = table(&[1, 2, 5], &[0b000, 0b010, 0b111]);

        let fast = merge_tables_fast(&left, &right).unwrap();
        let slow = merge_tables_bruteforce(&left, &right);

        assert_eq!(fast, slow);
    }

    struct XorShift64 {
        state: u64,
    }

    impl XorShift64 {
        fn new(seed: u64) -> Self {
            Self { state: seed.max(1) }
        }

        fn next_u32(&mut self) -> u32 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            (x >> 16) as u32
        }

        fn gen_range(&mut self, bound: u32) -> u32 {
            if bound == 0 {
                0
            } else {
                self.next_u32() % bound
            }
        }
    }

    fn random_bits(rng: &mut XorShift64, universe: usize, max_len: usize) -> Vec<u32> {
        let target_len = 1 + rng.gen_range(max_len as u32) as usize;
        let mut set = BTreeSet::new();
        while set.len() < target_len {
            set.insert(rng.gen_range(universe as u32));
        }
        set.into_iter().collect()
    }

    fn random_rows(rng: &mut XorShift64, arity: usize) -> Vec<u32> {
        let full = 1u32 << arity;
        let target_len = 1 + rng.gen_range(full.min(8));
        let mut set = BTreeSet::new();
        while set.len() < target_len as usize {
            set.insert(rng.gen_range(full));
        }
        set.into_iter().collect()
    }

    #[test]
    fn merge_matches_bruteforce_on_random_small_tables() {
        let mut rng = XorShift64::new(0xC0FFEE);

        for _case in 0..200 {
            let left_bits = random_bits(&mut rng, 8, 5);
            let right_bits = random_bits(&mut rng, 8, 5);
            let left = Table32 {
                rows: random_rows(&mut rng, left_bits.len()),
                bits: left_bits,
            };
            let right = Table32 {
                rows: random_rows(&mut rng, right_bits.len()),
                bits: right_bits,
            };

            let fast = merge_tables_fast(&left, &right).unwrap();
            let slow = merge_tables_bruteforce(&left, &right);

            assert_eq!(fast, slow);
        }
    }

    #[test]
    fn merged_rows_project_back_to_inputs() {
        let left = table(&[2, 4, 8], &[0b001, 0b010, 0b111]);
        let right = table(&[1, 4, 9], &[0b000, 0b010, 0b111]);
        let merged = merge_tables_fast(&left, &right).unwrap();

        for &row in &merged.rows {
            let left_projection = project_to_bits(row, &merged.bits, &left.bits);
            let right_projection = project_to_bits(row, &merged.bits, &right.bits);
            assert!(left.rows.binary_search(&left_projection).is_ok());
            assert!(right.rows.binary_search(&right_projection).is_ok());
        }
    }
}
