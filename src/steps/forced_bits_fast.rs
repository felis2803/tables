use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::process;
use std::thread;

#[derive(Clone, Debug)]
struct Table {
    bits: Vec<u32>,
    rows: Vec<u32>,
}

#[derive(Clone, Debug, Default)]
struct SystemSummary {
    table_count: usize,
    bit_count: usize,
    row_count: usize,
    arity_distribution: BTreeMap<usize, usize>,
}

#[derive(Clone, Debug, Default)]
struct PropagationStats {
    affected_tables: usize,
    changed_tables: usize,
    removed_rows: usize,
    removed_tautologies: usize,
    collapsed_duplicate_tables: usize,
}

#[derive(Clone, Debug)]
struct RoundInfo {
    round: usize,
    input: SystemSummary,
    forced_bits: usize,
    forced_one_bits: usize,
    forced_zero_bits: usize,
    forced_occurrences: usize,
    propagation: PropagationStats,
    output: SystemSummary,
    changed: bool,
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
                return Err(format!("unsupported escape sequence at byte {}", self.index));
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
                        Some(b',') => {
                            self.index += 1;
                        }
                        Some(b']') => {
                            self.index += 1;
                            return Ok(values);
                        }
                        Some(found) => {
                            return Err(format!(
                                "expected ',' or ']' at byte {}, found '{}'",
                                self.index, found as char
                            ));
                        }
                        None => return Err("unterminated array".to_string()),
                    }
                }
                None => return Err("unterminated array".to_string()),
            }
        }
    }

    fn parse_table(&mut self) -> Result<Table, String> {
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
                Some(b',') => {
                    self.index += 1;
                }
                Some(b'}') => {
                    self.index += 1;
                    break;
                }
                Some(found) => {
                    return Err(format!(
                        "expected ',' or '}}' at byte {}, found '{}'",
                        self.index, found as char
                    ));
                }
                None => return Err("unterminated object".to_string()),
            }
        }

        let bits = bits.ok_or_else(|| "table is missing 'bits'".to_string())?;
        let mut rows = rows.ok_or_else(|| "table is missing 'rows'".to_string())?;

        if bits.len() > 32 {
            return Err(format!(
                "table arity {} exceeds uint32 row width limit",
                bits.len()
            ));
        }
        if bits.windows(2).any(|window| window[0] >= window[1]) {
            return Err(format!("bits are not strictly increasing: {:?}", bits));
        }

        rows.sort_unstable();
        rows.dedup();
        let full_mask = full_mask(bits.len())?;
        for &row in &rows {
            if row & !full_mask != 0 {
                return Err(format!(
                    "row value {row} exceeds arity {} mask {full_mask}",
                    bits.len()
                ));
            }
        }
        if rows.is_empty() {
            return Err(format!("table {:?} has no rows", bits));
        }

        Ok(Table { bits, rows })
    }

    fn parse_tables(&mut self) -> Result<Vec<Table>, String> {
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
                        Some(b',') => {
                            self.index += 1;
                        }
                        Some(b']') => {
                            self.index += 1;
                            break;
                        }
                        Some(found) => {
                            return Err(format!(
                                "expected ',' or ']' at byte {}, found '{}'",
                                self.index, found as char
                            ));
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
    let mut input = "data/derived/tables.subset_pruned.json".to_string();
    let mut output = "data/derived/tables.subset_pruned.forced.json".to_string();
    let mut report = "data/reports/report.subset_pruned.forced.json".to_string();
    let mut forced = "data/derived/bits.subset_pruned.forced.json".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input = args.next().ok_or_else(|| "missing value for --input".to_string())?,
            "--output" => output = args.next().ok_or_else(|| "missing value for --output".to_string())?,
            "--report" => report = args.next().ok_or_else(|| "missing value for --report".to_string())?,
            "--forced" => forced = args.next().ok_or_else(|| "missing value for --forced".to_string())?,
            "--help" | "-h" => {
                return Err(
                    "usage: reduce_forced_bits --input <path> --output <path> --report <path> --forced <path>"
                        .to_string(),
                );
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok((input, output, report, forced))
}

fn full_mask(width: usize) -> Result<u32, String> {
    if width > 32 {
        return Err(format!("arity {width} exceeds uint32 width"));
    }
    Ok(if width == 32 {
        u32::MAX
    } else if width == 0 {
        0
    } else {
        (1u32 << width) - 1
    })
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

fn worker_count() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

fn summarize_system(tables: &[Table]) -> SystemSummary {
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

fn canonicalize_tables(tables: Vec<Table>) -> Result<(Vec<Table>, usize), String> {
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

    let mut output: Vec<Table> = by_bits
        .into_iter()
        .map(|(bits, rows)| Table { bits, rows })
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

fn collect_forced_bits(tables: &[Table]) -> Result<(HashMap<u32, u8>, usize), String> {
    let ranges = chunk_ranges(tables.len(), worker_count());
    let mut merged: HashMap<u32, u8> = HashMap::new();
    let mut occurrences = 0usize;

    thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::new();
        for (start, end) in ranges {
            let slice = &tables[start..end];
            handles.push(scope.spawn(move || -> Result<(Vec<(u32, u8)>, usize), String> {
                let mut assignments = Vec::new();
                let mut local_occurrences = 0usize;

                for table in slice {
                    let width = table.bits.len();
                    if width == 0 {
                        continue;
                    }
                    let mut and_mask = full_mask(width)?;
                    let mut or_mask = 0u32;
                    for &row in &table.rows {
                        and_mask &= row;
                        or_mask |= row;
                    }
                    let zero_mask = full_mask(width)? & !or_mask;
                    for offset in 0..width {
                        let mask = 1u32 << offset;
                        if (and_mask & mask) != 0 {
                            assignments.push((table.bits[offset], 1));
                            local_occurrences += 1;
                        } else if (zero_mask & mask) != 0 {
                            assignments.push((table.bits[offset], 0));
                            local_occurrences += 1;
                        }
                    }
                }

                Ok((assignments, local_occurrences))
            }));
        }

        for handle in handles {
            let joined = handle
                .join()
                .map_err(|_| "forced-bit worker thread panicked".to_string())?;
            let (assignments, local_occurrences) = joined?;
            occurrences += local_occurrences;
            for (bit, value) in assignments {
                match merged.get(&bit).copied() {
                    Some(existing) if existing != value => {
                        return Err(format!("conflicting forced values for bit {bit}"));
                    }
                    Some(_) => {}
                    None => {
                        merged.insert(bit, value);
                    }
                }
            }
        }
        Ok(())
    })?;

    Ok((merged, occurrences))
}

fn propagate_forced_bits(
    tables: &[Table],
    forced: &HashMap<u32, u8>,
) -> Result<(Vec<Table>, PropagationStats), String> {
    let ranges = chunk_ranges(tables.len(), worker_count());
    let mut projected = Vec::with_capacity(tables.len());
    let mut stats = PropagationStats::default();

    thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::new();
        for (start, end) in ranges {
            let slice = &tables[start..end];
            handles.push(scope.spawn(move || -> Result<(Vec<Table>, PropagationStats), String> {
                let mut partial_tables = Vec::with_capacity(slice.len());
                let mut partial_stats = PropagationStats::default();

                for table in slice {
                    let touches_forced = table.bits.iter().any(|bit| forced.contains_key(bit));
                    if touches_forced {
                        partial_stats.affected_tables += 1;
                    } else {
                        partial_tables.push(table.clone());
                        continue;
                    }

                    let mut kept_bits = Vec::with_capacity(table.bits.len());
                    let mut kept_indices = Vec::with_capacity(table.bits.len());
                    for (index, &bit) in table.bits.iter().enumerate() {
                        if !forced.contains_key(&bit) {
                            kept_bits.push(bit);
                            kept_indices.push(index);
                        }
                    }

                    let mut new_rows = Vec::with_capacity(table.rows.len());
                    for &row in &table.rows {
                        let mut consistent = true;
                        for (index, &bit) in table.bits.iter().enumerate() {
                            if let Some(&forced_value) = forced.get(&bit) {
                                if ((row >> index) & 1) != forced_value as u32 {
                                    consistent = false;
                                    break;
                                }
                            }
                        }
                        if !consistent {
                            continue;
                        }

                        let mut projected_row = 0u32;
                        for (new_index, &old_index) in kept_indices.iter().enumerate() {
                            if ((row >> old_index) & 1) != 0 {
                                projected_row |= 1u32 << new_index;
                            }
                        }
                        new_rows.push(projected_row);
                    }

                    if new_rows.is_empty() {
                        return Err(format!(
                            "contradiction after forcing table with bits {:?}",
                            table.bits
                        ));
                    }

                    new_rows.sort_unstable();
                    new_rows.dedup();
                    partial_stats.removed_rows += table.rows.len() - new_rows.len();

                    if kept_bits != table.bits || new_rows != table.rows {
                        partial_stats.changed_tables += 1;
                    }

                    if kept_bits.is_empty() {
                        if new_rows == [0] {
                            partial_stats.removed_tautologies += 1;
                            continue;
                        }
                        return Err(format!(
                            "non-tautological zero-bit table after forcing {:?}",
                            table.bits
                        ));
                    }

                    let full_count = 1usize << kept_bits.len();
                    if new_rows.len() == full_count {
                        partial_stats.removed_tautologies += 1;
                        continue;
                    }

                    partial_tables.push(Table {
                        bits: kept_bits,
                        rows: new_rows,
                    });
                }

                Ok((partial_tables, partial_stats))
            }));
        }

        for handle in handles {
            let joined = handle
                .join()
                .map_err(|_| "propagation worker thread panicked".to_string())?;
            let (partial_tables, partial_stats) = joined?;
            projected.extend(partial_tables);
            stats.affected_tables += partial_stats.affected_tables;
            stats.changed_tables += partial_stats.changed_tables;
            stats.removed_rows += partial_stats.removed_rows;
            stats.removed_tautologies += partial_stats.removed_tautologies;
        }
        Ok(())
    })?;

    let (canonical, duplicate_count) = canonicalize_tables(projected)?;
    stats.collapsed_duplicate_tables = duplicate_count;
    Ok((canonical, stats))
}

fn write_tables_json(path: &str, tables: &[Table]) -> Result<(), String> {
    let mut output = String::with_capacity(tables.len() * 32);
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

fn write_forced_bits_json(path: &str, forced: &BTreeMap<u32, u8>) -> Result<(), String> {
    let mut output = String::new();
    output.push_str("[\n");
    for (index, (&bit, &value)) in forced.iter().enumerate() {
        output.push_str("  {\n");
        write!(output, "    \"bit\": {bit},\n").unwrap();
        write!(output, "    \"value\": {value}\n").unwrap();
        output.push_str("  }");
        if index + 1 != forced.len() {
            output.push(',');
        }
        output.push('\n');
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

fn write_round_json(output: &mut String, round: &RoundInfo, indent: usize) {
    output.push_str("{\n");

    push_indent(output, indent + 2);
    writeln!(output, "\"round\": {},", round.round).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"input_table_count\": {},", round.input.table_count).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"input_bit_count\": {},", round.input.bit_count).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"input_row_count\": {},", round.input.row_count).unwrap();
    push_indent(output, indent + 2);
    output.push_str("\"input_arity_distribution\": ");
    write_distribution_json(output, &round.input.arity_distribution, indent + 2);
    output.push_str(",\n");
    push_indent(output, indent + 2);
    writeln!(output, "\"forced_bits\": {},", round.forced_bits).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"forced_one_bits\": {},", round.forced_one_bits).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"forced_zero_bits\": {},", round.forced_zero_bits).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"forced_occurrences\": {},", round.forced_occurrences).unwrap();
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"affected_tables\": {},",
        round.propagation.affected_tables
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"changed_tables\": {},",
        round.propagation.changed_tables
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"removed_rows\": {},",
        round.propagation.removed_rows
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"removed_tautologies\": {},",
        round.propagation.removed_tautologies
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(
        output,
        "\"collapsed_duplicate_tables\": {},",
        round.propagation.collapsed_duplicate_tables
    )
    .unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"output_table_count\": {},", round.output.table_count).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"output_bit_count\": {},", round.output.bit_count).unwrap();
    push_indent(output, indent + 2);
    writeln!(output, "\"output_row_count\": {},", round.output.row_count).unwrap();
    push_indent(output, indent + 2);
    output.push_str("\"output_arity_distribution\": ");
    write_distribution_json(output, &round.output.arity_distribution, indent + 2);
    output.push_str(",\n");
    push_indent(output, indent + 2);
    writeln!(output, "\"changed\": {}", round.changed).unwrap();

    push_indent(output, indent);
    output.push('}');
}

fn write_report_json(
    path: &str,
    input_path: &str,
    output_path: &str,
    forced_path: &str,
    initial: &SystemSummary,
    final_summary: &SystemSummary,
    rounds: &[RoundInfo],
    forced: &BTreeMap<u32, u8>,
) -> Result<(), String> {
    let one_count = forced.values().filter(|&&value| value == 1).count();
    let zero_count = forced.len() - one_count;
    let productive_rounds = rounds.iter().filter(|round| round.changed).count();
    let total_forced_occurrences: usize = rounds.iter().map(|round| round.forced_occurrences).sum();
    let total_affected_tables: usize = rounds.iter().map(|round| round.propagation.affected_tables).sum();
    let total_changed_tables: usize = rounds.iter().map(|round| round.propagation.changed_tables).sum();
    let total_removed_rows: usize = rounds.iter().map(|round| round.propagation.removed_rows).sum();
    let total_removed_tautologies: usize =
        rounds.iter().map(|round| round.propagation.removed_tautologies).sum();
    let total_collapsed_duplicate_tables: usize = rounds
        .iter()
        .map(|round| round.propagation.collapsed_duplicate_tables)
        .sum();

    let mut output = String::new();
    output.push_str("{\n");
    output.push_str(
        "  \"method\": \"iterative AND/OR-based fixed-bit detection, propagation, tautology removal, and equal-bitset intersection until no further fixed bits\",\n",
    );
    writeln!(output, "  \"input\": \"{input_path}\",").unwrap();
    writeln!(output, "  \"output\": \"{output_path}\",").unwrap();
    writeln!(output, "  \"forced_output\": \"{forced_path}\",").unwrap();
    writeln!(output, "  \"initial_table_count\": {},", initial.table_count).unwrap();
    writeln!(output, "  \"initial_bit_count\": {},", initial.bit_count).unwrap();
    writeln!(output, "  \"initial_row_count\": {},", initial.row_count).unwrap();
    writeln!(output, "  \"final_table_count\": {},", final_summary.table_count).unwrap();
    writeln!(output, "  \"final_bit_count\": {},", final_summary.bit_count).unwrap();
    writeln!(output, "  \"final_row_count\": {},", final_summary.row_count).unwrap();
    writeln!(output, "  \"productive_round_count\": {},", productive_rounds).unwrap();
    writeln!(
        output,
        "  \"round_count_including_final_check\": {},",
        rounds.len()
    )
    .unwrap();
    writeln!(output, "  \"final_forced_bit_count\": {},", forced.len()).unwrap();
    writeln!(output, "  \"final_one_bit_count\": {},", one_count).unwrap();
    writeln!(output, "  \"final_zero_bit_count\": {},", zero_count).unwrap();
    writeln!(
        output,
        "  \"total_forced_occurrences\": {},",
        total_forced_occurrences
    )
    .unwrap();
    writeln!(output, "  \"total_affected_tables\": {},", total_affected_tables).unwrap();
    writeln!(output, "  \"total_changed_tables\": {},", total_changed_tables).unwrap();
    writeln!(output, "  \"total_removed_rows\": {},", total_removed_rows).unwrap();
    writeln!(
        output,
        "  \"total_removed_tautologies\": {},",
        total_removed_tautologies
    )
    .unwrap();
    writeln!(
        output,
        "  \"total_collapsed_duplicate_tables\": {},",
        total_collapsed_duplicate_tables
    )
    .unwrap();
    output.push_str("  \"final_arity_distribution\": ");
    write_distribution_json(&mut output, &final_summary.arity_distribution, 2);
    output.push_str(",\n");
    output.push_str("  \"rounds\": [\n");
    for (index, round) in rounds.iter().enumerate() {
        push_indent(&mut output, 4);
        write_round_json(&mut output, round, 4);
        if index + 1 != rounds.len() {
            output.push(',');
        }
        output.push('\n');
    }
    output.push_str("  ]\n");
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
    let (input_path, output_path, report_path, forced_path) = parse_args()?;
    let bytes = fs::read(&input_path).map_err(|err| format!("failed to read {input_path}: {err}"))?;
    let mut parser = Parser::new(&bytes);
    let mut tables = parser.parse_tables()?;

    let initial_summary = summarize_system(&tables);
    let mut all_forced: BTreeMap<u32, u8> = BTreeMap::new();
    let mut rounds = Vec::new();
    let mut round_number = 1usize;

    loop {
        let input_summary = summarize_system(&tables);
        let (forced_current, forced_occurrences) = collect_forced_bits(&tables)?;
        let forced_one_bits = forced_current.values().filter(|&&value| value == 1).count();
        let forced_zero_bits = forced_current.len() - forced_one_bits;

        let (output_tables, propagation) = if forced_current.is_empty() {
            (tables.clone(), PropagationStats::default())
        } else {
            for (&bit, &value) in &forced_current {
                match all_forced.get(&bit).copied() {
                    Some(existing) if existing != value => {
                        return Err(format!("conflicting global forced values for bit {bit}"));
                    }
                    Some(_) => {}
                    None => {
                        all_forced.insert(bit, value);
                    }
                }
            }
            propagate_forced_bits(&tables, &forced_current)?
        };

        let output_summary = summarize_system(&output_tables);
        let changed = !forced_current.is_empty();
        rounds.push(RoundInfo {
            round: round_number,
            input: input_summary,
            forced_bits: forced_current.len(),
            forced_one_bits,
            forced_zero_bits,
            forced_occurrences,
            propagation,
            output: output_summary,
            changed,
        });

        tables = output_tables;
        if !changed {
            break;
        }
        round_number += 1;
    }

    let final_summary = summarize_system(&tables);
    write_tables_json(&output_path, &tables)?;
    write_forced_bits_json(&forced_path, &all_forced)?;
    write_report_json(
        &report_path,
        &input_path,
        &output_path,
        &forced_path,
        &initial_summary,
        &final_summary,
        &rounds,
        &all_forced,
    )?;

    let one_count = all_forced.values().filter(|&&value| value == 1).count();
    let zero_count = all_forced.len() - one_count;
    println!("rounds including final check: {}", rounds.len());
    println!(
        "productive rounds: {}",
        rounds.iter().filter(|round| round.changed).count()
    );
    println!("forced bits: {}", all_forced.len());
    println!("ones: {one_count}");
    println!("zeros: {zero_count}");
    println!("final tables: {}", final_summary.table_count);
    println!("final bits: {}", final_summary.bit_count);
    println!("final rows: {}", final_summary.row_count);
    println!("output: {output_path}");
    println!("forced: {forced_path}");
    println!("report: {report_path}");

    Ok(())
}
