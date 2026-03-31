import argparse
import json
from collections import Counter, defaultdict, deque
from pathlib import Path

from collapse_bit_pairs import build_rewrite_map, extract_relations, rewrite_tables
from merge_subset_tables import collapse_equal_bitsets, merge_subsets, prune_included_tables
from reduce_with_forced_bits_fixed_point import collect_bits, collect_forced_bits


def write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def tables_from_dict(tables_by_bits: dict[tuple[int, ...], set[int]]) -> list[dict]:
    return [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(tables_by_bits.items(), key=lambda item: (len(item[0]), item[0]))
    ]


def exact_local_bucket_forced(
    tables: list[dict],
    max_bucket_bits: int,
) -> tuple[dict[int, int], dict]:
    bit_to_tables: dict[int, list[int]] = defaultdict(list)
    for table_index, table in enumerate(tables):
        for bit in table["bits"]:
            bit_to_tables[bit].append(table_index)

    forced: dict[int, int] = {}
    checked = 0
    bucket_histogram: Counter = Counter()

    for bit, table_indices in bit_to_tables.items():
        union_bits: set[int] = set()
        for table_index in table_indices:
            union_bits.update(tables[table_index]["bits"])

        bucket_size = len(union_bits)
        if bucket_size > max_bucket_bits:
            continue

        checked += 1
        bucket_histogram[bucket_size] += 1
        ordered_bits = sorted(union_bits)
        bit_index = {bucket_bit: index for index, bucket_bit in enumerate(ordered_bits)}
        target_index = bit_index[bit]
        support_mask = 0

        for assignment in range(1 << bucket_size):
            for table_index in table_indices:
                table = tables[table_index]
                for row in table["rows"]:
                    for offset, table_bit in enumerate(table["bits"]):
                        if ((assignment >> bit_index[table_bit]) & 1) != ((row >> offset) & 1):
                            break
                    else:
                        break
                else:
                    break
            else:
                support_mask |= 1 << ((assignment >> target_index) & 1)
                if support_mask == 3:
                    break

        if support_mask in (1, 2):
            forced[bit] = 0 if support_mask == 1 else 1

    return forced, {
        "checked_bit_count": checked,
        "max_bucket_bits": max_bucket_bits,
        "bucket_histogram": {
            str(size): bucket_histogram[size]
            for size in sorted(bucket_histogram)
        },
    }


def compact_radius2_forced(
    tables: list[dict],
    max_bits: int,
    max_tables: int,
) -> tuple[dict[int, int], dict]:
    bit_to_tables: dict[int, set[int]] = defaultdict(set)
    for table_index, table in enumerate(tables):
        for bit in table["bits"]:
            bit_to_tables[bit].add(table_index)

    candidates: list[tuple[int, list[int]]] = []
    for bit, direct_tables in bit_to_tables.items():
        if len(direct_tables) != 1:
            continue

        neighborhood_tables = set(direct_tables)
        touched_bits: set[int] = set()
        for table_index in list(neighborhood_tables):
            touched_bits.update(tables[table_index]["bits"])

        for neighbor_bit in list(touched_bits):
            neighborhood_tables.update(bit_to_tables[neighbor_bit])

        neighborhood_bits: set[int] = set()
        for table_index in neighborhood_tables:
            neighborhood_bits.update(tables[table_index]["bits"])

        if len(neighborhood_bits) <= max_bits and len(neighborhood_tables) <= max_tables:
            candidates.append((bit, sorted(neighborhood_tables)))

    def satisfiable_with_value(target_bit: int, target_value: int, table_indices: list[int]) -> bool:
        local_tables = []
        for table_index in table_indices:
            table = tables[table_index]
            rows = table["rows"]
            if target_bit in table["bits"]:
                target_offset = table["bits"].index(target_bit)
                rows = [
                    row
                    for row in rows
                    if ((row >> target_offset) & 1) == target_value
                ]
            if not rows:
                return False
            local_tables.append((table["bits"], rows))

        local_tables.sort(key=lambda item: (len(item[1]), len(item[0])))

        def dfs(index: int, assignment: dict[int, int]) -> bool:
            if index == len(local_tables):
                return True

            bits, rows = local_tables[index]
            for row in rows:
                added = []
                for offset, bit in enumerate(bits):
                    value = (row >> offset) & 1
                    previous = assignment.get(bit)
                    if previous is None:
                        assignment[bit] = value
                        added.append(bit)
                    elif previous != value:
                        break
                else:
                    if dfs(index + 1, assignment):
                        return True
                for bit in added:
                    assignment.pop(bit, None)

            return False

        return dfs(0, {target_bit: target_value})

    forced: dict[int, int] = {}
    for bit, neighborhood_tables in candidates:
        sat0 = satisfiable_with_value(bit, 0, neighborhood_tables)
        sat1 = satisfiable_with_value(bit, 1, neighborhood_tables)
        if sat0 ^ sat1:
            forced[bit] = 1 if sat1 else 0

    return forced, {
        "candidate_count": len(candidates),
        "max_radius2_bits": max_bits,
        "max_radius2_tables": max_tables,
    }


class SmallArityEliminator:
    def __init__(self, tables: list[dict], max_output_arity: int) -> None:
        self.active: dict[int, tuple[tuple[int, ...], set[int]]] = {}
        self.by_bits: dict[tuple[int, ...], int] = {}
        self.bit_to_tables: dict[int, set[int]] = defaultdict(set)
        self.next_table_id = 0
        self.max_output_arity = max_output_arity

        for table in tables:
            self.register_table(tuple(table["bits"]), set(table["rows"]))

    def register_table(self, bits: tuple[int, ...], rows: set[int]) -> int | None:
        if not bits:
            if rows == {0}:
                return None
            raise RuntimeError("contradiction on empty-bit table")

        if len(rows) == (1 << len(bits)):
            return None

        existing = self.by_bits.get(bits)
        if existing is not None:
            existing_bits, existing_rows = self.active[existing]
            reduced_rows = existing_rows & rows
            if not reduced_rows:
                raise RuntimeError("contradiction while intersecting equal bitsets")
            if reduced_rows != existing_rows:
                self.active[existing] = (existing_bits, reduced_rows)
            return existing

        table_id = self.next_table_id
        self.next_table_id += 1
        self.active[table_id] = (bits, rows)
        self.by_bits[bits] = table_id
        for bit in bits:
            self.bit_to_tables[bit].add(table_id)
        return table_id

    def unregister_table(self, table_id: int) -> None:
        if table_id not in self.active:
            return

        bits, _ = self.active.pop(table_id)
        self.by_bits.pop(bits, None)
        for bit in bits:
            holder = self.bit_to_tables[bit]
            holder.discard(table_id)
            if not holder:
                self.bit_to_tables.pop(bit, None)

    @staticmethod
    def project_remove_bit(
        bits: tuple[int, ...],
        rows: set[int],
        bit: int,
    ) -> tuple[tuple[int, ...], set[int]]:
        remove_index = bits.index(bit)
        output_bits = tuple(bucket_bit for bucket_bit in bits if bucket_bit != bit)
        output_rows: set[int] = set()

        for row in rows:
            projected = 0
            new_index = 0
            for old_index, bucket_bit in enumerate(bits):
                if old_index == remove_index:
                    continue
                if (row >> old_index) & 1:
                    projected |= 1 << new_index
                new_index += 1
            output_rows.add(projected)

        return output_bits, output_rows

    @staticmethod
    def join_project_bit(
        bits1: tuple[int, ...],
        rows1: set[int],
        bits2: tuple[int, ...],
        rows2: set[int],
        removed_bit: int,
    ) -> tuple[tuple[int, ...], set[int]]:
        index1 = {bit: offset for offset, bit in enumerate(bits1)}
        index2 = {bit: offset for offset, bit in enumerate(bits2)}
        common_bits = sorted(set(bits1) & set(bits2))
        common_offsets1 = [index1[bit] for bit in common_bits]
        common_offsets2 = [index2[bit] for bit in common_bits]
        buckets: dict[int, list[int]] = defaultdict(list)

        for row2 in rows2:
            key = 0
            for position, offset in enumerate(common_offsets2):
                key |= ((row2 >> offset) & 1) << position
            buckets[key].append(row2)

        output_bits = tuple(sorted((set(bits1) | set(bits2)) - {removed_bit}))
        output_index = {bit: offset for offset, bit in enumerate(output_bits)}
        output_rows: set[int] = set()

        for row1 in rows1:
            key = 0
            for position, offset in enumerate(common_offsets1):
                key |= ((row1 >> offset) & 1) << position

            for row2 in buckets.get(key, ()):
                joined = 0
                for bit in output_bits:
                    if bit in index1:
                        value = (row1 >> index1[bit]) & 1
                    else:
                        value = (row2 >> index2[bit]) & 1
                    if value:
                        joined |= 1 << output_index[bit]
                output_rows.add(joined)

        return output_bits, output_rows

    def eliminate(self) -> dict:
        queue_degree_1 = deque(bit for bit, tables in self.bit_to_tables.items() if len(tables) == 1)
        queue_degree_2 = deque(bit for bit, tables in self.bit_to_tables.items() if len(tables) == 2)
        queued_1 = set(queue_degree_1)
        queued_2 = set(queue_degree_2)
        leaf_ops = 0
        degree2_ops = 0

        while queue_degree_1 or queue_degree_2:
            if queue_degree_1:
                bit = queue_degree_1.popleft()
                queued_1.discard(bit)
                holder = self.bit_to_tables.get(bit)
                if holder is None or len(holder) != 1:
                    continue

                table_id = next(iter(holder))
                bits, rows = self.active[table_id]
                output_bits, output_rows = self.project_remove_bit(bits, rows, bit)
                affected = set(bits)
                self.unregister_table(table_id)
                self.register_table(output_bits, output_rows)
                leaf_ops += 1
                changed_bits = affected | set(output_bits)
            else:
                bit = queue_degree_2.popleft()
                queued_2.discard(bit)
                holder = self.bit_to_tables.get(bit)
                if holder is None or len(holder) != 2:
                    continue

                table_id_1, table_id_2 = sorted(holder)
                bits1, rows1 = self.active[table_id_1]
                bits2, rows2 = self.active[table_id_2]
                output_arity = len((set(bits1) | set(bits2)) - {bit})
                if output_arity > self.max_output_arity:
                    continue

                output_bits, output_rows = self.join_project_bit(bits1, rows1, bits2, rows2, bit)
                affected = set(bits1) | set(bits2)
                self.unregister_table(table_id_1)
                self.unregister_table(table_id_2)
                self.register_table(output_bits, output_rows)
                degree2_ops += 1
                changed_bits = affected | set(output_bits)

            for changed_bit in changed_bits:
                degree = len(self.bit_to_tables.get(changed_bit, ()))
                if degree == 1 and changed_bit not in queued_1:
                    queue_degree_1.append(changed_bit)
                    queued_1.add(changed_bit)
                if degree == 2 and changed_bit not in queued_2:
                    queue_degree_2.append(changed_bit)
                    queued_2.add(changed_bit)

        materialized = [
            {"bits": list(bits), "rows": sorted(rows)}
            for bits, rows in self.active.values()
        ]
        return {
            "tables": materialized,
            "leaf_ops": leaf_ops,
            "degree2_ops": degree2_ops,
        }


def elimination_cleanup_forced(
    tables: list[dict],
    max_output_arity: int,
) -> tuple[dict[int, int], dict]:
    eliminator = SmallArityEliminator(tables, max_output_arity)
    elimination = eliminator.eliminate()
    reduced_tables = elimination["tables"]
    tables_by_bits, duplicate_count = collapse_equal_bitsets(reduced_tables)
    merge_stats, pair_details = merge_subsets(tables_by_bits)
    tables_by_bits, dropped_tables = prune_included_tables(tables_by_bits, pair_details)
    cleaned_tables = tables_from_dict(tables_by_bits)
    relations = extract_relations(cleaned_tables)
    relation_count = len(relations)
    if relations:
        rewrite_map, _ = build_rewrite_map(relations)
        cleaned_tables, _ = rewrite_tables(cleaned_tables, rewrite_map)

    forced, forced_occurrences = collect_forced_bits(cleaned_tables)
    return forced, {
        "max_degree2_output_arity": max_output_arity,
        "leaf_elimination_ops": elimination["leaf_ops"],
        "degree2_elimination_ops": elimination["degree2_ops"],
        "table_count_after_elimination": len(reduced_tables),
        "bit_count_after_elimination": len(collect_bits(reduced_tables)),
        "collapsed_duplicate_tables": duplicate_count,
        "subset_row_deletions": merge_stats["row_deletions"],
        "dropped_included_tables": len(dropped_tables),
        "pair_relation_pairs_found": relation_count,
        "forced_bit_occurrences_after_cleanup": forced_occurrences,
        "final_table_count_after_cleanup": len(cleaned_tables),
        "final_bit_count_after_cleanup": len(collect_bits(cleaned_tables)),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables_triple_round_2_fixed_point.json")
    parser.add_argument("--output", default="forced_bits_no_sat.json")
    parser.add_argument("--report", default="forced_bits_no_sat_report.json")
    parser.add_argument("--local-bucket-bits", type=int, default=12)
    parser.add_argument("--radius2-bits", type=int, default=24)
    parser.add_argument("--radius2-tables", type=int, default=6)
    parser.add_argument("--degree2-output-arity", type=int, default=10)
    args = parser.parse_args()

    with Path(args.input).open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    direct_forced, direct_occurrences = collect_forced_bits(tables)
    local_bucket_forced, local_bucket_stats = exact_local_bucket_forced(
        tables,
        args.local_bucket_bits,
    )
    radius2_forced, radius2_stats = compact_radius2_forced(
        tables,
        args.radius2_bits,
        args.radius2_tables,
    )
    elimination_forced, elimination_stats = elimination_cleanup_forced(
        tables,
        args.degree2_output_arity,
    )

    method_results = [
        ("direct", direct_forced),
        ("local_bucket", local_bucket_forced),
        ("compact_radius2", radius2_forced),
        ("degree1_degree2_elimination", elimination_forced),
    ]

    combined: dict[int, int] = {}
    discovered_by: dict[int, list[str]] = defaultdict(list)
    for method_name, forced in method_results:
        for bit, value in forced.items():
            previous = combined.get(bit)
            if previous is not None and previous != value:
                raise RuntimeError(f"conflicting values for bit {bit}")
            combined[bit] = value
            discovered_by[bit].append(method_name)

    ones = [bit for bit, value in sorted(combined.items()) if value == 1]
    zeros = [bit for bit, value in sorted(combined.items()) if value == 0]

    payload = {
        "method": "exact non-SAT search for forced bits using direct scan, exact local buckets, compact radius-2 neighborhoods, and exact degree-1/degree-2 elimination",
        "input": args.input,
        "fixed_count": len(combined),
        "one_count": len(ones),
        "zero_count": len(zeros),
        "ones": ones,
        "zeros": zeros,
        "details": [
            {
                "bit": bit,
                "value": combined[bit],
                "discovered_by": discovered_by[bit],
            }
            for bit in sorted(combined)
        ],
    }

    report = {
        "input": args.input,
        "table_count": len(tables),
        "bit_count": len(collect_bits(tables)),
        "direct_forced_bit_count": len(direct_forced),
        "direct_forced_occurrences": direct_occurrences,
        "local_bucket_forced_bit_count": len(local_bucket_forced),
        "local_bucket_stats": local_bucket_stats,
        "compact_radius2_forced_bit_count": len(radius2_forced),
        "compact_radius2_stats": radius2_stats,
        "degree1_degree2_elimination_forced_bit_count": len(elimination_forced),
        "degree1_degree2_elimination_stats": elimination_stats,
        "total_unique_forced_bits": len(combined),
    }

    write_json(Path(args.output), payload)
    write_json(Path(args.report), report)

    print(f"forced bits: {len(combined)}")
    print(f"ones: {len(ones)}")
    print(f"zeros: {len(zeros)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
