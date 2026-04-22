use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde::Serialize;

use crate::common::{for_each_combination, is_full_row_set, project_row, Table};
use crate::rank_stats::compute_rank;
use crate::table_merge_fast::merge_tables_fast_from_slices;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TableSummary {
    pub bits: Vec<u32>,
    pub bit_count: usize,
    pub row_count: usize,
    pub rank: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RoundtripCheck {
    pub name: String,
    pub factor_count: usize,
    pub factor_arity_distribution: BTreeMap<String, usize>,
    pub factor_tautology_count: usize,
    pub reconstructed_summary: Option<TableSummary>,
    pub matches_source: bool,
}

#[derive(Clone, Debug)]
pub struct NamedTablePool {
    pub name: String,
    pub factors: Vec<Table>,
    pub reconstructed: Option<Table>,
    pub check: RoundtripCheck,
}

#[derive(Clone, Debug)]
pub struct ProgressiveRoundtripResult {
    pub extracted_by_arity: BTreeMap<usize, Vec<Table>>,
    pub selected_by_arity: BTreeMap<usize, Vec<Table>>,
    pub two_bit_non_taut: Vec<Table>,
    pub pools: Vec<NamedTablePool>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectiveStageStats {
    pub arity: usize,
    pub candidate_bitset_count: usize,
    pub evaluated_candidate_count: usize,
    pub selected_factor_count: usize,
    pub selected_factor_bits: Vec<Vec<u32>>,
    pub missing_bits_before: usize,
    pub missing_bits_after: usize,
    pub extra_rows_before: usize,
    pub extra_rows_after: usize,
}

#[derive(Clone, Debug)]
pub struct SelectiveRoundtripResult {
    pub two_bit_all: Vec<Table>,
    pub two_bit_non_taut: Vec<Table>,
    pub selected_by_arity: BTreeMap<usize, Vec<Table>>,
    pub pools: Vec<NamedTablePool>,
    pub stage_stats: Vec<SelectiveStageStats>,
}

pub fn summarize_table(table: &Table) -> TableSummary {
    TableSummary {
        bits: table.bits.clone(),
        bit_count: table.bits.len(),
        row_count: table.rows.len(),
        rank: compute_rank(table.rows.len(), table.bits.len()),
    }
}

pub fn extract_exact_subtables(source: &Table, subtable_arity: usize) -> Vec<Table> {
    if subtable_arity == 0 || subtable_arity > source.bits.len() {
        return Vec::new();
    }

    let mut subtables = Vec::new();
    for_each_combination(source.bits.len(), subtable_arity, |indices| {
        let bits: Vec<u32> = indices.iter().map(|&index| source.bits[index]).collect();
        let mut rows: Vec<u32> = source
            .rows
            .iter()
            .copied()
            .map(|row| project_row(row, indices))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        subtables.push(Table { bits, rows });
    });
    subtables
}

pub fn filter_non_tautologies(tables: &[Table]) -> Vec<Table> {
    tables
        .iter()
        .filter(|table| !is_full_row_set(table.rows.len(), table.bits.len()))
        .cloned()
        .collect()
}

pub fn reconstruct_join(factors: &[Table]) -> Result<Option<Table>> {
    let Some((first_index, _)) = factors
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| compare_initial_factor(left, right))
    else {
        return Ok(None);
    };

    let mut remaining: Vec<Table> = factors.to_vec();
    let mut current = remaining.swap_remove(first_index);

    while !remaining.is_empty() {
        let next_index = remaining
            .iter()
            .position(|factor| have_shared_bits(&current.bits, &factor.bits))
            .unwrap_or(0);
        let next = remaining.swap_remove(next_index);
        current = merge_exact(&current, &next)?;
        if current.rows.is_empty() {
            bail!("join of subtable factors became contradictory");
        }
    }

    Ok(Some(current))
}

pub fn build_roundtrip_check(
    name: &str,
    source: &Table,
    factors: &[Table],
) -> Result<NamedTablePool> {
    let mut factor_arity_distribution = BTreeMap::new();
    let mut factor_tautology_count = 0usize;
    for factor in factors {
        *factor_arity_distribution
            .entry(factor.bits.len().to_string())
            .or_insert(0) += 1;
        if is_full_row_set(factor.rows.len(), factor.bits.len()) {
            factor_tautology_count += 1;
        }
    }

    let reconstructed = reconstruct_join(factors)?;
    let matches_source = reconstructed
        .as_ref()
        .is_some_and(|table| table.bits == source.bits && table.rows == source.rows);

    Ok(NamedTablePool {
        name: name.to_string(),
        factors: factors.to_vec(),
        reconstructed: reconstructed.clone(),
        check: RoundtripCheck {
            name: name.to_string(),
            factor_count: factors.len(),
            factor_arity_distribution,
            factor_tautology_count,
            reconstructed_summary: reconstructed.as_ref().map(summarize_table),
            matches_source,
        },
    })
}

pub fn run_progressive_roundtrip(
    source: &Table,
    max_subtable_arity: usize,
) -> Result<ProgressiveRoundtripResult> {
    if max_subtable_arity < 2 {
        bail!(
            "progressive subtable roundtrip requires max_subtable_arity >= 2, got {}",
            max_subtable_arity
        );
    }

    let effective_max_subtable_arity = max_subtable_arity.min(source.bits.len());

    let mut extracted_by_arity = BTreeMap::new();
    let mut selected_by_arity = BTreeMap::new();
    let two_bit_all = extract_exact_subtables(source, 2);
    let two_bit_non_taut = filter_non_tautologies(&two_bit_all);
    extracted_by_arity.insert(2usize, two_bit_all);
    selected_by_arity.insert(2usize, two_bit_non_taut.clone());

    let mut pools = Vec::new();
    let mut current_pool = two_bit_non_taut.clone();
    pools.push(build_roundtrip_check("2", source, &current_pool)?);

    for subtable_arity in 3..=effective_max_subtable_arity {
        if pools.last().is_some_and(|pool| pool.check.matches_source) {
            break;
        }

        let subtables = extract_exact_subtables(source, subtable_arity);
        let selected = filter_non_tautologies(&subtables);
        current_pool.extend(selected.iter().cloned());
        extracted_by_arity.insert(subtable_arity, subtables);
        selected_by_arity.insert(subtable_arity, selected);

        let name = build_pool_name(subtable_arity);
        pools.push(build_roundtrip_check(&name, source, &current_pool)?);
    }

    Ok(ProgressiveRoundtripResult {
        extracted_by_arity,
        selected_by_arity,
        two_bit_non_taut,
        pools,
    })
}

pub fn run_selective_roundtrip(
    source: &Table,
    max_subtable_arity: usize,
) -> Result<SelectiveRoundtripResult> {
    if max_subtable_arity < 2 {
        bail!(
            "selective subtable roundtrip requires max_subtable_arity >= 2, got {}",
            max_subtable_arity
        );
    }

    let effective_max_subtable_arity = max_subtable_arity.min(source.bits.len());

    let two_bit_all = extract_exact_subtables(source, 2);
    let two_bit_non_taut = filter_non_tautologies(&two_bit_all);

    let mut selected_by_arity = BTreeMap::new();
    selected_by_arity.insert(2usize, two_bit_non_taut.clone());

    let mut current_pool = two_bit_non_taut.clone();
    let mut pools = vec![build_roundtrip_check("2", source, &current_pool)?];
    let mut stage_stats = Vec::new();
    let mut selected_bitsets: HashSet<Vec<u32>> =
        current_pool.iter().map(|factor| factor.bits.clone()).collect();
    let mut projection_cache = SourceProjectionCache::new(source);

    for subtable_arity in 3..=effective_max_subtable_arity {
        if pools.last().is_some_and(|pool| pool.check.matches_source) {
            break;
        }

        let (missing_bits_before, extra_rows_before) = gap_metrics(
            source,
            pools.last().and_then(|pool| pool.reconstructed.as_ref()),
            &mut projection_cache,
        )?;

        let mut selected_here = Vec::new();
        let mut selected_bits_here = Vec::new();
        let mut candidate_bitset_count = 0usize;
        let mut evaluated_candidate_count = 0usize;
        let mut current_reconstructed = pools.last().and_then(|pool| pool.reconstructed.clone());

        loop {
            let candidate_bitsets = generate_selective_candidate_bitsets(
                source,
                current_reconstructed.as_ref(),
                subtable_arity,
                &selected_bitsets,
            )?;
            if candidate_bitsets.is_empty() {
                break;
            }
            candidate_bitset_count += candidate_bitsets.len();

            let (missing_before, extra_before) =
                gap_metrics(source, current_reconstructed.as_ref(), &mut projection_cache)?;
            let Some(best) = choose_best_selective_candidate(
                source,
                current_reconstructed.as_ref(),
                &candidate_bitsets,
                missing_before,
                extra_before,
                &mut projection_cache,
                &mut evaluated_candidate_count,
            )?
            else {
                break;
            };

            selected_bits_here.push(best.factor.bits.clone());
            selected_bitsets.insert(best.factor.bits.clone());
            current_pool.push(best.factor.clone());
            selected_here.push(best.factor.clone());
            current_reconstructed = Some(best.reconstructed);

            if current_reconstructed.as_ref().is_some_and(|table| {
                table.bits == source.bits && table.rows == source.rows
            }) {
                break;
            }
        }

        selected_by_arity.insert(subtable_arity, selected_here);
        let pool_name = build_pool_name(subtable_arity);
        let pool = build_roundtrip_check(&pool_name, source, &current_pool)?;
        let (missing_bits_after, extra_rows_after) = gap_metrics(
            source,
            pool.reconstructed.as_ref(),
            &mut projection_cache,
        )?;
        stage_stats.push(SelectiveStageStats {
            arity: subtable_arity,
            candidate_bitset_count,
            evaluated_candidate_count,
            selected_factor_count: selected_bits_here.len(),
            selected_factor_bits: selected_bits_here,
            missing_bits_before,
            missing_bits_after,
            extra_rows_before,
            extra_rows_after,
        });
        pools.push(pool);
    }

    Ok(SelectiveRoundtripResult {
        two_bit_all,
        two_bit_non_taut,
        selected_by_arity,
        pools,
        stage_stats,
    })
}

fn build_pool_name(max_subtable_arity: usize) -> String {
    (2..=max_subtable_arity)
        .map(|arity| arity.to_string())
        .collect::<Vec<_>>()
        .join("+")
}

#[derive(Clone)]
struct SourceProjectionCache<'a> {
    source: &'a Table,
    positions: HashMap<u32, usize>,
    cache: HashMap<Vec<u32>, Table>,
}

impl<'a> SourceProjectionCache<'a> {
    fn new(source: &'a Table) -> Self {
        let positions = source
            .bits
            .iter()
            .copied()
            .enumerate()
            .map(|(index, bit)| (bit, index))
            .collect();
        Self {
            source,
            positions,
            cache: HashMap::new(),
        }
    }

    fn exact_projection(&mut self, bits: &[u32]) -> Result<Table> {
        if bits == self.source.bits {
            return Ok(self.source.clone());
        }
        if let Some(table) = self.cache.get(bits) {
            return Ok(table.clone());
        }

        let indices: Vec<usize> = bits
            .iter()
            .map(|bit| {
                self.positions
                    .get(bit)
                    .copied()
                    .ok_or_else(|| anyhow!("bit {} is absent from source table", bit))
            })
            .collect::<Result<_>>()?;

        let mut rows: Vec<u32> = self
            .source
            .rows
            .iter()
            .copied()
            .map(|row| project_row(row, &indices))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        let table = Table {
            bits: bits.to_vec(),
            rows,
        };
        self.cache.insert(bits.to_vec(), table.clone());
        Ok(table)
    }
}

#[derive(Clone)]
struct SelectiveCandidateChoice {
    factor: Table,
    reconstructed: Table,
}

fn choose_best_selective_candidate(
    source: &Table,
    current: Option<&Table>,
    candidate_bitsets: &[Vec<u32>],
    missing_before: usize,
    extra_before: usize,
    projection_cache: &mut SourceProjectionCache<'_>,
    evaluated_candidate_count: &mut usize,
) -> Result<Option<SelectiveCandidateChoice>> {
    let mut best: Option<(usize, usize, usize, usize, Vec<u32>, Table, Table)> = None;

    if let Some(current) = current {
        if missing_before == 0 && current.bits == source.bits {
            let mut best_local: Option<(usize, usize, Vec<u32>, Table)> = None;

            for bits in candidate_bitsets {
                let factor = projection_cache.exact_projection(bits)?;
                if factor.bits.len() >= 3 && is_full_row_set(factor.rows.len(), factor.bits.len()) {
                    continue;
                }

                *evaluated_candidate_count += 1;

                let current_projection = project_table_to_bits(current, bits)?;
                let local_extra = count_extra_rows(&current_projection.rows, &factor.rows);
                if local_extra == 0 {
                    continue;
                }

                let candidate = (local_extra, factor.rows.len(), bits.clone(), factor);
                if best_local
                    .as_ref()
                    .is_none_or(|best_choice| compare_local_selective_choice(&candidate, best_choice).is_lt())
                {
                    best_local = Some(candidate);
                }
            }

            return Ok(best_local.map(|(_, _, _, factor)| {
                let reconstructed = merge_exact(current, &factor)
                    .expect("selected local candidate must merge successfully");
                SelectiveCandidateChoice {
                    factor,
                    reconstructed,
                }
            }));
        }
    }

    for bits in candidate_bitsets {
        let factor = projection_cache.exact_projection(bits)?;
        if factor.bits.len() >= 3 && is_full_row_set(factor.rows.len(), factor.bits.len()) {
            continue;
        }

        *evaluated_candidate_count += 1;

        let reconstructed = match current {
            Some(current) => merge_exact(current, &factor)?,
            None => factor.clone(),
        };
        if reconstructed.rows.is_empty() {
            continue;
        }

        let exact_projection = projection_cache.exact_projection(&reconstructed.bits)?;
        let missing_after = source.bits.len().saturating_sub(reconstructed.bits.len());
        let extra_after = count_extra_rows(&reconstructed.rows, &exact_projection.rows);
        let improves = missing_after < missing_before
            || (missing_after == missing_before && extra_after < extra_before);
        if !improves {
            continue;
        }

        let candidate = (
            missing_after,
            extra_after,
            reconstructed.rows.len(),
            factor.rows.len(),
            bits.clone(),
            factor,
            reconstructed,
        );
        if best
            .as_ref()
            .is_none_or(|best_choice| compare_selective_choice(&candidate, best_choice).is_lt())
        {
            best = Some(candidate);
        }
    }

    Ok(best.map(
        |(_, _, _, _, _, factor, reconstructed)| SelectiveCandidateChoice {
            factor,
            reconstructed,
        },
    ))
}

fn compare_selective_choice(
    left: &(usize, usize, usize, usize, Vec<u32>, Table, Table),
    right: &(usize, usize, usize, usize, Vec<u32>, Table, Table),
) -> std::cmp::Ordering {
    left.0
        .cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(&right.2))
        .then_with(|| left.3.cmp(&right.3))
        .then_with(|| left.4.cmp(&right.4))
}

fn compare_local_selective_choice(
    left: &(usize, usize, Vec<u32>, Table),
    right: &(usize, usize, Vec<u32>, Table),
) -> std::cmp::Ordering {
    right
        .0
        .cmp(&left.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(&right.2))
}

fn generate_selective_candidate_bitsets(
    source: &Table,
    current: Option<&Table>,
    subtable_arity: usize,
    selected_bitsets: &HashSet<Vec<u32>>,
) -> Result<Vec<Vec<u32>>> {
    if current.is_none() {
        return Ok(generate_all_candidate_bitsets(
            source,
            subtable_arity,
            selected_bitsets,
        ));
    }

    let current = current.unwrap();
    let missing = collect_missing_bits(&source.bits, &current.bits);
    let candidates = if !missing.is_empty() {
        generate_missing_bit_candidates(source, current, subtable_arity, selected_bitsets)
    } else {
        generate_extra_row_witness_candidates(source, current, subtable_arity, selected_bitsets)?
    };

    if candidates.is_empty() {
        return Ok(generate_all_candidate_bitsets(
            source,
            subtable_arity,
            selected_bitsets,
        ));
    }
    Ok(candidates)
}

fn generate_all_candidate_bitsets(
    source: &Table,
    subtable_arity: usize,
    selected_bitsets: &HashSet<Vec<u32>>,
) -> Vec<Vec<u32>> {
    let mut candidates = Vec::new();
    for_each_combination(source.bits.len(), subtable_arity, |indices| {
        let bits: Vec<u32> = indices.iter().map(|&index| source.bits[index]).collect();
        if !selected_bitsets.contains(&bits) {
            candidates.push(bits);
        }
    });
    candidates
}

fn generate_missing_bit_candidates(
    source: &Table,
    current: &Table,
    subtable_arity: usize,
    selected_bitsets: &HashSet<Vec<u32>>,
) -> Vec<Vec<u32>> {
    let current_bits: HashSet<u32> = current.bits.iter().copied().collect();
    let missing_bits_list = collect_missing_bits(&source.bits, &current.bits);
    let missing_bits: HashSet<u32> = missing_bits_list.iter().copied().collect();
    let required_current = 1usize;
    let missing_per_candidate = missing_bits_list
        .len()
        .min(subtable_arity.saturating_sub(required_current));
    if missing_per_candidate == 0 {
        return Vec::new();
    }

    let mut candidates = Vec::new();
    for_each_combination(source.bits.len(), subtable_arity, |indices| {
        let mut missing_count = 0usize;
        let mut has_current = false;
        for &index in indices {
            let bit = source.bits[index];
            missing_count += usize::from(missing_bits.contains(&bit));
            has_current |= current_bits.contains(&bit);
        }

        if missing_count == missing_per_candidate && has_current {
            let bits: Vec<u32> = indices.iter().map(|&index| source.bits[index]).collect();
            if !selected_bitsets.contains(&bits) {
                candidates.push(bits);
            }
        }
    });
    candidates
}

fn generate_extra_row_witness_candidates(
    source: &Table,
    current: &Table,
    subtable_arity: usize,
    selected_bitsets: &HashSet<Vec<u32>>,
) -> Result<Vec<Vec<u32>>> {
    if current.bits != source.bits {
        bail!(
            "witness-based selective candidates require current bits to equal source bits"
        );
    }

    let extra_rows = collect_extra_rows(&current.rows, &source.rows);
    let mut unique = HashSet::new();
    let mut candidates = Vec::new();

    for extra_row in extra_rows {
        let Some(bits) = build_extra_row_witness_bits(source, extra_row, subtable_arity) else {
            continue;
        };
        if selected_bitsets.contains(&bits) || !unique.insert(bits.clone()) {
            continue;
        }
        candidates.push(bits);
    }

    candidates.sort_unstable();
    Ok(candidates)
}

fn build_extra_row_witness_bits(
    source: &Table,
    extra_row: u32,
    subtable_arity: usize,
) -> Option<Vec<u32>> {
    let mut selected_positions = Vec::new();
    let mut used = vec![false; source.bits.len()];
    let mut matching_rows = source.rows.clone();

    while !matching_rows.is_empty() && selected_positions.len() < subtable_arity {
        let mut best_position = None;
        let mut best_remaining = matching_rows.len();
        let mut best_rows = Vec::new();

        for position in 0..source.bits.len() {
            if used[position] {
                continue;
            }

            let value = (extra_row >> position) & 1;
            let remaining: Vec<u32> = matching_rows
                .iter()
                .copied()
                .filter(|row| ((row >> position) & 1) == value)
                .collect();

            if remaining.len() < best_remaining {
                best_remaining = remaining.len();
                best_position = Some(position);
                best_rows = remaining;
            }
        }

        let position = best_position?;
        used[position] = true;
        selected_positions.push(position);
        matching_rows = best_rows;
    }

    if !matching_rows.is_empty() {
        return None;
    }

    if selected_positions.len() < subtable_arity {
        for position in 0..source.bits.len() {
            if used[position] {
                continue;
            }
            selected_positions.push(position);
            if selected_positions.len() == subtable_arity {
                break;
            }
        }
    }

    if selected_positions.len() != subtable_arity {
        return None;
    }

    selected_positions.sort_unstable();
    Some(
        selected_positions
            .into_iter()
            .map(|position| source.bits[position])
            .collect(),
    )
}

fn gap_metrics(
    source: &Table,
    reconstructed: Option<&Table>,
    projection_cache: &mut SourceProjectionCache<'_>,
) -> Result<(usize, usize)> {
    let Some(reconstructed) = reconstructed else {
        return Ok((source.bits.len(), 0));
    };

    let exact_projection = projection_cache.exact_projection(&reconstructed.bits)?;
    Ok((
        source.bits.len().saturating_sub(reconstructed.bits.len()),
        count_extra_rows(&reconstructed.rows, &exact_projection.rows),
    ))
}

fn project_table_to_bits(table: &Table, bits: &[u32]) -> Result<Table> {
    if bits == table.bits {
        return Ok(table.clone());
    }

    let positions: HashMap<u32, usize> = table
        .bits
        .iter()
        .copied()
        .enumerate()
        .map(|(index, bit)| (bit, index))
        .collect();
    let indices: Vec<usize> = bits
        .iter()
        .map(|bit| {
            positions
                .get(bit)
                .copied()
                .ok_or_else(|| anyhow!("bit {} is absent from projected table", bit))
        })
        .collect::<Result<_>>()?;

    let mut rows: Vec<u32> = table
        .rows
        .iter()
        .copied()
        .map(|row| project_row(row, &indices))
        .collect();
    rows.sort_unstable();
    rows.dedup();
    Ok(Table {
        bits: bits.to_vec(),
        rows,
    })
}

fn count_extra_rows(rows: &[u32], exact_rows: &[u32]) -> usize {
    let mut rows_index = 0usize;
    let mut exact_index = 0usize;
    let mut extra = 0usize;

    while rows_index < rows.len() && exact_index < exact_rows.len() {
        match rows[rows_index].cmp(&exact_rows[exact_index]) {
            std::cmp::Ordering::Less => {
                extra += 1;
                rows_index += 1;
            }
            std::cmp::Ordering::Greater => exact_index += 1,
            std::cmp::Ordering::Equal => {
                rows_index += 1;
                exact_index += 1;
            }
        }
    }

    extra + (rows.len() - rows_index)
}

fn collect_extra_rows(rows: &[u32], exact_rows: &[u32]) -> Vec<u32> {
    let mut rows_index = 0usize;
    let mut exact_index = 0usize;
    let mut extra = Vec::new();

    while rows_index < rows.len() && exact_index < exact_rows.len() {
        match rows[rows_index].cmp(&exact_rows[exact_index]) {
            std::cmp::Ordering::Less => {
                extra.push(rows[rows_index]);
                rows_index += 1;
            }
            std::cmp::Ordering::Greater => exact_index += 1,
            std::cmp::Ordering::Equal => {
                rows_index += 1;
                exact_index += 1;
            }
        }
    }

    extra.extend_from_slice(&rows[rows_index..]);
    extra
}

fn collect_missing_bits(source_bits: &[u32], current_bits: &[u32]) -> Vec<u32> {
    let mut source_index = 0usize;
    let mut current_index = 0usize;
    let mut missing = Vec::new();

    while source_index < source_bits.len() && current_index < current_bits.len() {
        match source_bits[source_index].cmp(&current_bits[current_index]) {
            std::cmp::Ordering::Less => {
                missing.push(source_bits[source_index]);
                source_index += 1;
            }
            std::cmp::Ordering::Greater => current_index += 1,
            std::cmp::Ordering::Equal => {
                source_index += 1;
                current_index += 1;
            }
        }
    }

    missing.extend_from_slice(&source_bits[source_index..]);
    missing
}

fn merge_exact(left: &Table, right: &Table) -> Result<Table> {
    let merged = merge_tables_fast_from_slices(&left.bits, &left.rows, &right.bits, &right.rows)
        .map_err(|error| anyhow!(error))?;
    Ok(Table {
        bits: merged.bits,
        rows: merged.rows,
    })
}

fn compare_initial_factor(left: &Table, right: &Table) -> std::cmp::Ordering {
    right
        .bits
        .len()
        .cmp(&left.bits.len())
        .then_with(|| {
            compute_rank(left.rows.len(), left.bits.len())
                .total_cmp(&compute_rank(right.rows.len(), right.bits.len()))
        })
        .then_with(|| left.rows.len().cmp(&right.rows.len()))
        .then_with(|| left.bits.cmp(&right.bits))
}

fn have_shared_bits(left: &[u32], right: &[u32]) -> bool {
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::{
        extract_exact_subtables, filter_non_tautologies, reconstruct_join,
        run_progressive_roundtrip, run_selective_roundtrip, summarize_table,
    };
    use crate::common::Table;

    fn table(bits: &[u32], rows: &[u32]) -> Table {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
        rows.dedup();
        Table {
            bits: bits.to_vec(),
            rows,
        }
    }

    #[test]
    fn extracts_all_two_bit_subtables() {
        let source = table(&[10, 20, 30], &[0b000, 0b011, 0b101, 0b110]);
        let subtables = extract_exact_subtables(&source, 2);
        assert_eq!(subtables.len(), 3);
        assert_eq!(subtables[0].bits, vec![10, 20]);
        assert_eq!(subtables[1].bits, vec![10, 30]);
        assert_eq!(subtables[2].bits, vec![20, 30]);
    }

    #[test]
    fn drops_two_bit_tautologies() {
        let factors = vec![
            table(&[1, 2], &[0, 1, 2, 3]),
            table(&[1, 3], &[0, 3]),
        ];
        let filtered = filter_non_tautologies(&factors);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].bits, vec![1, 3]);
    }

    #[test]
    fn reconstruct_join_from_projection_factors() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b100, 0b111]);
        let factors = vec![
            table(&[1, 2], &[0b00, 0b11]),
            table(&[2, 3], &[0b00, 0b01, 0b10, 0b11]),
        ];
        let reconstructed = reconstruct_join(&factors).unwrap().unwrap();
        assert_eq!(reconstructed, source);
    }

    #[test]
    fn progressive_roundtrip_adds_three_bit_pool_when_two_bit_pool_fails() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let result = run_progressive_roundtrip(&source, 4).unwrap();
        assert_eq!(result.pools.len(), 2);
        assert!(!result.pools[0].check.matches_source);
        assert!(result.pools[1].check.matches_source);
        assert_eq!(result.pools[1].name, "2+3");
        assert_eq!(result.selected_by_arity[&3].len(), 1);
    }

    #[test]
    fn progressive_roundtrip_skips_higher_arity_tautologies_in_pool() {
        let source = table(
            &[1, 2, 3],
            &[0b000, 0b001, 0b010, 0b011, 0b100, 0b101, 0b110, 0b111],
        );
        let result = run_progressive_roundtrip(&source, 3).unwrap();
        assert_eq!(result.extracted_by_arity[&2].len(), 3);
        assert_eq!(result.selected_by_arity[&2].len(), 0);
        assert_eq!(result.extracted_by_arity[&3].len(), 1);
        assert_eq!(result.selected_by_arity[&3].len(), 0);
        assert_eq!(result.pools[1].check.factor_count, 0);
        assert_eq!(result.pools[1].check.factor_tautology_count, 0);
    }

    #[test]
    fn progressive_roundtrip_caps_requested_max_at_source_arity() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let result = run_progressive_roundtrip(&source, 6).unwrap();
        assert_eq!(result.extracted_by_arity.len(), 2);
        assert!(result.extracted_by_arity.contains_key(&2));
        assert!(result.extracted_by_arity.contains_key(&3));
        assert!(!result.extracted_by_arity.contains_key(&4));
    }

    #[test]
    fn selective_roundtrip_recovers_parity_with_one_three_bit_factor() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let result = run_selective_roundtrip(&source, 4).unwrap();
        assert_eq!(result.two_bit_non_taut.len(), 0);
        assert_eq!(result.stage_stats.len(), 1);
        assert_eq!(result.stage_stats[0].arity, 3);
        assert_eq!(result.stage_stats[0].selected_factor_count, 1);
        assert!(result.pools.last().unwrap().check.matches_source);
    }

    #[test]
    fn selective_roundtrip_reports_candidate_and_selection_stats() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let selective = run_selective_roundtrip(&source, 4).unwrap();
        assert_eq!(selective.pools.last().unwrap().check.matches_source, true);
        assert!(selective.stage_stats[0].candidate_bitset_count >= 1);
        assert!(selective.stage_stats[0].evaluated_candidate_count >= 1);
        assert!(selective.stage_stats[0].selected_factor_count >= 1);
    }

    #[test]
    fn selective_roundtrip_caps_requested_max_at_source_arity() {
        let source = table(&[1, 2, 3], &[0b000, 0b011, 0b101, 0b110]);
        let result = run_selective_roundtrip(&source, 6).unwrap();
        assert_eq!(result.selected_by_arity.len(), 2);
        assert!(result.selected_by_arity.contains_key(&2));
        assert!(result.selected_by_arity.contains_key(&3));
        assert!(!result.selected_by_arity.contains_key(&4));
    }

    #[test]
    fn summarize_table_reports_rank_inputs() {
        let summary = summarize_table(&table(&[1, 2], &[0b00, 0b11]));
        assert_eq!(summary.bit_count, 2);
        assert_eq!(summary.row_count, 2);
    }
}
