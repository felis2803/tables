use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::process;
use std::thread;

use tables::table_merge_fast::{merge_tables_fast, Table32};

#[derive(Clone, Debug, Default)]
struct SystemSummary {
    table_count: usize,
    bit_count: usize,
    row_count: usize,
    arity_distribution: BTreeMap<usize, usize>,
}

#[derive(Clone, Debug, Default)]
struct RankSummary {
    table_count: usize,
    min_rank: f64,
    max_rank: f64,
    mean_rank: f64,
    median_rank: f64,
}

#[derive(Clone, Debug, Default)]
struct MergeStats {
    bitpair_key_count: usize,
    raw_pair_hits_over_bitpairs: usize,
    candidate_pair_count: usize,
    empty_merge_count: usize,
    skipped_tautology_merges: usize,
    produced_nonempty_merges: usize,
    merged_duplicate_tables_collapsed: usize,
    combined_duplicate_tables_collapsed: usize,
}

struct Parser<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    fn skip_ws(&mut self) {
        while self.index < self.bytes.len() && self.bytes[self.index].is_ascii_whitespace() {
            self.index += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.bytes.get(self.index).copied()
    }

    fn expect(&mut self, ch: u8) -> Result<(), String> {
        self.skip_ws();
        match self.bytes.get(self.index).copied() {
            Some(found) if found == ch => {
                self.index += 1;
                Ok(())
            }
            Some(found) => Err(format!(
                "expected '{}' at byte {}, found '{}'",
                ch as char, self.index, found as char
            )),
            None => Err(format!(
                "expected '{}' at byte {}, found end of input",
                ch as char, self.index
            )),
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.skip_ws();
        if self.bytes.get(self.index).copied() != Some(b'"') {
            return Err(format!("expected string at byte {}", self.index));
        }
        self.index += 1;
        let start = self.index;
        while self.index < self.bytes.len() && self.bytes[self.index] != b'"' {
            if self.bytes[self.index] == b'\\' {
                return Err(format!(
                    "unsupported escape sequence at byte {}",
                    self.index
                ));
            }
            self.index += 1;
        }
        if self.index >= self.bytes.len() {
            return Err("unterminated string".to_string());
        }
        let value = std::str::from_utf8(&self.bytes[start..self.index])
            .map_err(|err| format!("invalid utf8 in string: {err}"))?
            .to_string();
        self.index += 1;
        Ok(value)
    }

    fn parse_u32(&mut self) -> Result<u32, String> {
        self.skip_ws();
        let start = self.index;
        while self.index < self.bytes.len() && self.bytes[self.index].is_ascii_digit() {
            self.index += 1;
        }
        if start == self.index {
            return Err(format!("expected integer at byte {}", start));
        }
        let value = std::str::from_utf8(&self.bytes[start..self.index])
            .map_err(|err| format!("invalid integer utf8: {err}"))?
            .parse::<u64>()
            .map_err(|err| format!("invalid integer at byte {start}: {err}"))?;
        u32::try_from(value).map_err(|_| format!("integer {value} does not fit into u32"))
    }

    fn parse_u32_array(&mut self) -> Result<Vec<u32>, String> {
        self.expect(b'[')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b']') => {
                    self.index += 1;
                    return Ok(values);
                }
                Some(_) => {
                    values.push(self.parse_u32()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.index += 1,
                        Some(b']') => {
                            self.index += 1;
                            return Ok(values);
                        }
                        Some(found) => {
                            return Err(format!(
                                "expected ',' or ']' at byte {}, found '{}'",
                                self.index, found as char
                            ))
                        }
                        None => return Err("unterminated array".to_string()),
                    }
                }
                None => return Err("unterminated array".to_string()),
            }
        }
    }

    fn parse_table(&mut self) -> Result<Table32, String> {
        self.expect(b'{')?;
        let mut bits: Option<Vec<u32>> = None;
        let mut rows: Option<Vec<u32>> = None;

        loop {
            self.skip_ws();
            if self.peek() == Some(b'}') {
                self.index += 1;
                break;
            }

            let key = self.parse_string()?;
            self.expect(b':')?;
            match key.as_str() {
                "bits" => bits = Some(self.parse_u32_array()?),
                "rows" => rows = Some(self.parse_u32_array()?),
                other => return Err(format!("unexpected key '{other}' at byte {}", self.index)),
            }

            self.skip_ws();
            match self.peek() {
                Some(b',') => self.index += 1,
                Some(b'}') => {
                    self.index += 1;
                    break;
                }
                Some(found) => {
                    return Err(format!(
                        "expected ',' or '}}' at byte {}, found '{}'",
                        self.index, found as char
                    ))
                }
                None => return Err("unterminated object".to_string()),
            }
        }

        let bits = bits.ok_or_else(|| "table is missing 'bits'".to_string())?;
        let mut rows = rows.ok_or_else(|| "table is missing 'rows'".to_string())?;
        rows.sort_unstable();
        rows.dedup();

        Ok(Table32 { bits, rows })
    }

    fn parse_tables(&mut self) -> Result<Vec<Table32>, String> {
        self.expect(b'[')?;
        let mut tables = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b']') => {
                    self.index += 1;
                    break;
                }
                Some(_) => {
                    tables.push(self.parse_table()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.index += 1,
                        Some(b']') => {
                            self.index += 1;
                            break;
                        }
                        Some(found) => {
                            return Err(format!(
                                "expected ',' or ']' at byte {}, found '{}'",
                                self.index, found as char
                            ))
                        }
                        None => return Err("unterminated top-level array".to_string()),
                    }
                }
                None => return Err("unterminated top-level array".to_string()),
            }
        }

        self.skip_ws();
        if self.index != self.bytes.len() {
            return Err(format!("unexpected trailing data at byte {}", self.index));
        }
        Ok(tables)
    }
}

fn parse_args() -> Result<(String, String, String, String), String> {
    let mut input = "data/derived/tables.common_node_fixed_point.json".to_string();
    let mut merged_output =
        "data/derived/tables.common_node_fixed_point.pairwise_merges.json".to_string();
    let mut combined_output =
        "data/derived/tables.common_node_fixed_point.with_pairwise_merges.json".to_string();
    let mut report = "data/reports/report.common_node_fixed_point.pairwise_merges.json".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input = args.next().ok_or_else(|| "missing value for --input".to_string())?,
            "--merged-output" => {
                merged_output = args
                    .next()
                    .ok_or_else(|| "missing value for --merged-output".to_string())?
            }
            "--combined-output" => {
                combined_output = args
                    .next()
                    .ok_or_else(|| "missing value for --combined-output".to_string())?
            }
            "--report" => report = args.next().ok_or_else(|| "missing value for --report".to_string())?,
            "--help" | "-h" => {
                return Err(
                    "usage: pairwise_merge_generate --input <path> --merged-output <path> --combined-output <path> --report <path>"
                        .to_string(),
                )
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok((input, merged_output, combined_output, report))
}

fn worker_count() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

fn chunk_ranges(len: usize, mut workers: usize) -> Vec<(usize, usize)> {
    if len == 0 {
        return Vec::new();
    }
    workers = workers.max(1).min(len);
    let base = len / workers;
    let extra = len % workers;
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0usize;
    for index in 0..workers {
        let size = base + usize::from(index < extra);
        let end = start + size;
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn summarize_system(tables: &[Table32]) -> SystemSummary {
    let max_bit = tables
        .iter()
        .flat_map(|table| table.bits.iter().copied())
        .max()
        .map(|bit| bit as usize + 1)
        .unwrap_or(0);
    let mut seen = vec![false; max_bit];
    let mut summary = SystemSummary {
        table_count: tables.len(),
        ..SystemSummary::default()
    };

    for table in tables {
        *summary
            .arity_distribution
            .entry(table.bits.len())
            .or_insert(0) += 1;
        summary.row_count += table.rows.len();
        for &bit in &table.bits {
            seen[bit as usize] = true;
        }
    }
    summary.bit_count = seen.into_iter().filter(|value| *value).count();
    summary
}

fn summarize_ranks(tables: &[Table32]) -> RankSummary {
    let mut ranks = Vec::with_capacity(tables.len());
    for table in tables {
        if table.bits.is_empty() || table.rows.is_empty() {
            continue;
        }
        ranks.push((table.rows.len() as f64).powf(1.0 / table.bits.len() as f64));
    }
    if ranks.is_empty() {
        return RankSummary::default();
    }
    ranks.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let len = ranks.len();
    let sum: f64 = ranks.iter().sum();
    let median = if len % 2 == 0 {
        (ranks[(len / 2) - 1] + ranks[len / 2]) / 2.0
    } else {
        ranks[len / 2]
    };
    RankSummary {
        table_count: len,
        min_rank: ranks[0],
        max_rank: ranks[len - 1],
        mean_rank: sum / len as f64,
        median_rank: median,
    }
}

fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut result = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

fn canonicalize_tables(tables: Vec<Table32>) -> Result<(Vec<Table32>, usize), String> {
    let input_count = tables.len();
    let mut by_bits: HashMap<Vec<u32>, Vec<u32>> = HashMap::with_capacity(input_count);

    for table in tables {
        match by_bits.get_mut(&table.bits) {
            Some(existing_rows) => {
                let reduced = intersect_sorted(existing_rows, &table.rows);
                if reduced.is_empty() {
                    return Err(format!(
                        "contradiction while intersecting equal bitsets {:?}",
                        table.bits
                    ));
                }
                *existing_rows = reduced;
            }
            None => {
                by_bits.insert(table.bits, table.rows);
            }
        }
    }

    let mut output: Vec<Table32> = by_bits
        .into_iter()
        .map(|(bits, rows)| Table32 { bits, rows })
        .collect();
    output.sort_by(|left, right| {
        left.bits
            .len()
            .cmp(&right.bits.len())
            .then_with(|| left.bits.cmp(&right.bits))
    });
    let output_len = output.len();
    Ok((output, input_count - output_len))
}

fn generate_candidate_pairs(tables: &[Table32]) -> (Vec<(u32, u32)>, usize, usize) {
    let mut bitpair_to_tables: HashMap<(u32, u32), Vec<u32>> = HashMap::new();
    for (table_index, table) in tables.iter().enumerate() {
        for left in 0..table.bits.len() {
            for right in (left + 1)..table.bits.len() {
                bitpair_to_tables
                    .entry((table.bits[left], table.bits[right]))
                    .or_default()
                    .push(table_index as u32);
            }
        }
    }

    let mut raw_pair_hits = 0usize;
    let mut seen_pairs: HashSet<u64> = HashSet::new();
    let mut pairs = Vec::new();
    for table_ids in bitpair_to_tables.values() {
        let n = table_ids.len();
        raw_pair_hits += n.saturating_sub(1) * n / 2;
        for left in 0..n {
            for right in (left + 1)..n {
                let a = table_ids[left].min(table_ids[right]);
                let b = table_ids[left].max(table_ids[right]);
                let key = ((a as u64) << 32) | b as u64;
                if seen_pairs.insert(key) {
                    pairs.push((a, b));
                }
            }
        }
    }
    pairs.sort_unstable();
    (pairs, bitpair_to_tables.len(), raw_pair_hits)
}

fn is_tautology(table: &Table32) -> bool {
    (table.rows.len() as u64) == (1u64 << table.bits.len())
}

fn merge_candidate_pairs(
    tables: &[Table32],
    pairs: &[(u32, u32)],
) -> Result<(Vec<Table32>, usize, usize), String> {
    let ranges = chunk_ranges(pairs.len(), worker_count());
    let mut merged_tables = Vec::new();
    let mut empty_count = 0usize;
    let mut tautology_count = 0usize;

    thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::new();
        for (start, end) in ranges {
            let chunk = &pairs[start..end];
            handles.push(
                scope.spawn(move || -> Result<(Vec<Table32>, usize, usize), String> {
                    let mut partial_tables = Vec::with_capacity(chunk.len());
                    let mut partial_empty = 0usize;
                    let mut partial_tautology = 0usize;
                    for &(left_index, right_index) in chunk {
                        let merged = merge_tables_fast(
                            &tables[left_index as usize],
                            &tables[right_index as usize],
                        )?;
                        if merged.rows.is_empty() {
                            partial_empty += 1;
                        } else if is_tautology(&merged) {
                            partial_tautology += 1;
                        } else {
                            partial_tables.push(merged);
                        }
                    }
                    Ok((partial_tables, partial_empty, partial_tautology))
                }),
            );
        }

        for handle in handles {
            let joined = handle
                .join()
                .map_err(|_| "merge worker thread panicked".to_string())?;
            let (partial_tables, partial_empty, partial_tautology) = joined?;
            merged_tables.extend(partial_tables);
            empty_count += partial_empty;
            tautology_count += partial_tautology;
        }
        Ok(())
    })?;

    Ok((merged_tables, empty_count, tautology_count))
}

fn write_tables_json(path: &str, tables: &[Table32]) -> Result<(), String> {
    let mut output = String::with_capacity(tables.len() * 48);
    output.push('[');
    for (table_index, table) in tables.iter().enumerate() {
        if table_index > 0 {
            output.push(',');
        }
        output.push_str("{\"bits\":[");
        for (index, bit) in table.bits.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            write!(output, "{bit}").unwrap();
        }
        output.push_str("],\"rows\":[");
        for (index, row) in table.rows.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            write!(output, "{row}").unwrap();
        }
        output.push_str("]}");
    }
    output.push_str("]\n");
    fs::write(path, output).map_err(|err| format!("failed to write {path}: {err}"))
}

fn push_indent(output: &mut String, indent: usize) {
    for _ in 0..indent {
        output.push(' ');
    }
}

fn write_distribution_json(output: &mut String, map: &BTreeMap<usize, usize>, indent: usize) {
    output.push_str("{\n");
    for (index, (arity, count)) in map.iter().enumerate() {
        push_indent(output, indent + 2);
        write!(output, "\"{arity}\": {count}").unwrap();
        if index + 1 != map.len() {
            output.push(',');
        }
        output.push('\n');
    }
    push_indent(output, indent);
    output.push('}');
}

fn write_rank_summary_json(output: &mut String, summary: &RankSummary, indent: usize) {
    output.push_str("{\n");
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"metric\": \"rank = row_count ** (1 / bit_count)\","
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"table_count\": {},", summary.table_count).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"min_rank\": {:.15},", summary.min_rank).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"max_rank\": {:.15},", summary.max_rank).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"mean_rank\": {:.15},", summary.mean_rank).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"median_rank\": {:.15}", summary.median_rank).unwrap();
    push_indent(output, indent);
    output.push('}');
}

fn write_report_json(
    path: &str,
    input_path: &str,
    merged_output_path: &str,
    combined_output_path: &str,
    input_summary: &SystemSummary,
    input_rank_summary: &RankSummary,
    merged_summary: &SystemSummary,
    merged_rank_summary: &RankSummary,
    combined_summary: &SystemSummary,
    combined_rank_summary: &RankSummary,
    stats: &MergeStats,
) -> Result<(), String> {
    let mut output = String::new();
    output.push_str("{\n");
    output.push_str(
        "  \"method\": \"pairwise natural join of all table pairs with more than one shared bit, followed by canonicalization of merged tables and canonicalization of original plus merged tables\",\n",
    );
    writeln!(output, "  \"input\": \"{input_path}\",").unwrap();
    writeln!(output, "  \"merged_output\": \"{merged_output_path}\",").unwrap();
    writeln!(output, "  \"combined_output\": \"{combined_output_path}\",").unwrap();
    writeln!(
        output,
        "  \"input_table_count\": {},",
        input_summary.table_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"input_bit_count\": {},",
        input_summary.bit_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"input_row_count\": {},",
        input_summary.row_count
    )
    .unwrap();
    output.push_str("  \"input_arity_distribution\": ");
    write_distribution_json(&mut output, &input_summary.arity_distribution, 2);
    output.push_str(",\n");
    output.push_str("  \"input_rank_summary\": ");
    write_rank_summary_json(&mut output, input_rank_summary, 2);
    output.push_str(",\n");
    writeln!(
        output,
        "  \"bitpair_key_count\": {},",
        stats.bitpair_key_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"raw_pair_hits_over_bitpairs\": {},",
        stats.raw_pair_hits_over_bitpairs
    )
    .unwrap();
    writeln!(
        output,
        "  \"candidate_pair_count\": {},",
        stats.candidate_pair_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"empty_merge_count\": {},",
        stats.empty_merge_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"skipped_tautology_merges\": {},",
        stats.skipped_tautology_merges
    )
    .unwrap();
    writeln!(
        output,
        "  \"produced_nonempty_merges\": {},",
        stats.produced_nonempty_merges
    )
    .unwrap();
    writeln!(
        output,
        "  \"merged_duplicate_tables_collapsed\": {},",
        stats.merged_duplicate_tables_collapsed
    )
    .unwrap();
    writeln!(
        output,
        "  \"combined_duplicate_tables_collapsed\": {},",
        stats.combined_duplicate_tables_collapsed
    )
    .unwrap();
    writeln!(
        output,
        "  \"merged_table_count\": {},",
        merged_summary.table_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"merged_bit_count\": {},",
        merged_summary.bit_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"merged_row_count\": {},",
        merged_summary.row_count
    )
    .unwrap();
    output.push_str("  \"merged_arity_distribution\": ");
    write_distribution_json(&mut output, &merged_summary.arity_distribution, 2);
    output.push_str(",\n");
    output.push_str("  \"merged_rank_summary\": ");
    write_rank_summary_json(&mut output, merged_rank_summary, 2);
    output.push_str(",\n");
    writeln!(
        output,
        "  \"combined_table_count\": {},",
        combined_summary.table_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"combined_bit_count\": {},",
        combined_summary.bit_count
    )
    .unwrap();
    writeln!(
        output,
        "  \"combined_row_count\": {},",
        combined_summary.row_count
    )
    .unwrap();
    output.push_str("  \"combined_arity_distribution\": ");
    write_distribution_json(&mut output, &combined_summary.arity_distribution, 2);
    output.push_str(",\n");
    output.push_str("  \"combined_rank_summary\": ");
    write_rank_summary_json(&mut output, combined_rank_summary, 2);
    output.push_str("\n");
    output.push_str("}\n");
    fs::write(path, output).map_err(|err| format!("failed to write {path}: {err}"))
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (input_path, merged_output_path, combined_output_path, report_path) = parse_args()?;
    let bytes =
        fs::read(&input_path).map_err(|err| format!("failed to read {input_path}: {err}"))?;
    let mut parser = Parser::new(&bytes);
    let tables = parser.parse_tables()?;

    let input_summary = summarize_system(&tables);
    let input_rank_summary = summarize_ranks(&tables);
    let (pairs, bitpair_key_count, raw_pair_hits_over_bitpairs) = generate_candidate_pairs(&tables);
    let (merged_raw, empty_merge_count, skipped_tautology_merges) =
        merge_candidate_pairs(&tables, &pairs)?;
    let produced_nonempty_merges = merged_raw.len();

    let (merged_tables, merged_duplicate_tables_collapsed) = canonicalize_tables(merged_raw)?;
    let mut combined_input = tables.clone();
    combined_input.extend(merged_tables.iter().cloned());
    let (combined_tables, combined_duplicate_tables_collapsed) =
        canonicalize_tables(combined_input)?;

    let merged_summary = summarize_system(&merged_tables);
    let merged_rank_summary = summarize_ranks(&merged_tables);
    let combined_summary = summarize_system(&combined_tables);
    let combined_rank_summary = summarize_ranks(&combined_tables);
    let stats = MergeStats {
        bitpair_key_count,
        raw_pair_hits_over_bitpairs,
        candidate_pair_count: pairs.len(),
        empty_merge_count,
        skipped_tautology_merges,
        produced_nonempty_merges,
        merged_duplicate_tables_collapsed,
        combined_duplicate_tables_collapsed,
    };

    write_tables_json(&merged_output_path, &merged_tables)?;
    write_tables_json(&combined_output_path, &combined_tables)?;
    write_report_json(
        &report_path,
        &input_path,
        &merged_output_path,
        &combined_output_path,
        &input_summary,
        &input_rank_summary,
        &merged_summary,
        &merged_rank_summary,
        &combined_summary,
        &combined_rank_summary,
        &stats,
    )?;

    println!("candidate pairs: {}", pairs.len());
    println!("empty merges: {}", empty_merge_count);
    println!("skipped tautology merges: {}", skipped_tautology_merges);
    println!(
        "merged tables after canonicalization: {}",
        merged_tables.len()
    );
    println!(
        "combined tables after canonicalization: {}",
        combined_tables.len()
    );
    println!("merged output: {merged_output_path}");
    println!("combined output: {combined_output_path}");
    println!("report: {report_path}");
    Ok(())
}
