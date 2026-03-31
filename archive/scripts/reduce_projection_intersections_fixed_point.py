import argparse
import json
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

from merge_subset_tables import collapse_equal_bitsets
from reduce_with_forced_bits_fixed_point import arity_distribution, collect_bits, tables_from_dict, write_json


def project_row(row: int, subset_indices: tuple[int, ...]) -> int:
    projected = 0
    for new_offset, old_offset in enumerate(subset_indices):
        if (row >> old_offset) & 1:
            projected |= 1 << new_offset
    return projected


def build_projection_intersections(
    tables_by_bits: dict[tuple[int, ...], set[int]],
    subset_size_min: int,
    subset_size_max: int,
) -> tuple[dict[tuple[int, ...], set[int]], dict[tuple[int, ...], int], dict[str, int]]:
    intersections: dict[tuple[int, ...], set[int]] = {}
    support_counts: dict[tuple[int, ...], int] = defaultdict(int)
    projection_count = 0
    subset_size_histogram: Counter = Counter()

    for bits, rows in tables_by_bits.items():
        bit_count = len(bits)
        upper = min(subset_size_max, bit_count)
        for subset_size in range(subset_size_min, upper + 1):
            for subset_indices in combinations(range(bit_count), subset_size):
                subset_bits = tuple(bits[index] for index in subset_indices)
                subset_rows = {project_row(row, subset_indices) for row in rows}
                if subset_bits in intersections:
                    intersections[subset_bits].intersection_update(subset_rows)
                else:
                    intersections[subset_bits] = subset_rows
                support_counts[subset_bits] += 1
                projection_count += 1
                subset_size_histogram[subset_size] += 1

    return intersections, support_counts, {
        "projection_count": projection_count,
        "subset_size_histogram": {
            str(size): subset_size_histogram[size]
            for size in sorted(subset_size_histogram)
        },
    }


def filter_tables_by_projection_intersections(
    tables_by_bits: dict[tuple[int, ...], set[int]],
    intersections: dict[tuple[int, ...], set[int]],
    support_counts: dict[tuple[int, ...], int],
    subset_size_min: int,
    subset_size_max: int,
) -> dict[str, int]:
    changed_tables = 0
    row_deletions = 0
    emptied_tables = 0
    effective_projection_count = 0
    restrictive_projection_count = 0

    for bits in sorted(tables_by_bits, key=lambda item: (len(item), item)):
        rows = tables_by_bits[bits]
        bit_count = len(bits)
        upper = min(subset_size_max, bit_count)
        changed_here = False

        for subset_size in range(subset_size_min, upper + 1):
            for subset_indices in combinations(range(bit_count), subset_size):
                subset_bits = tuple(bits[index] for index in subset_indices)
                if support_counts.get(subset_bits, 0) <= 1:
                    continue

                allowed_rows = intersections[subset_bits]
                effective_projection_count += 1
                if len(allowed_rows) < (1 << subset_size):
                    restrictive_projection_count += 1

                filtered_rows = {
                    row
                    for row in rows
                    if project_row(row, subset_indices) in allowed_rows
                }
                if len(filtered_rows) != len(rows):
                    row_deletions += len(rows) - len(filtered_rows)
                    rows = filtered_rows
                    tables_by_bits[bits] = rows
                    changed_here = True
                    if not rows:
                        emptied_tables += 1
                        break

            if not rows:
                break

        if changed_here:
            changed_tables += 1

    return {
        "effective_projection_count": effective_projection_count,
        "restrictive_projection_count": restrictive_projection_count,
        "changed_tables": changed_tables,
        "row_deletions": row_deletions,
        "emptied_tables": emptied_tables,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables_quadruple_functional_minctx_pilot_fixed_point.json")
    parser.add_argument("--output", default="tables_projection_intersections_fixed_point.json")
    parser.add_argument("--report", default="projection_intersections_fixed_point_report.json")
    parser.add_argument("--subset-size-min", type=int, default=2)
    parser.add_argument("--subset-size-max", type=int, default=3)
    args = parser.parse_args()

    with Path(args.input).open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    initial_tables = len(tables)
    initial_bits = collect_bits(tables)
    rounds = []
    productive_rounds = 0
    total_duplicate_collapse = 0
    total_row_deletions = 0
    round_index = 1

    while True:
        changed = False
        round_info = {
            "round": round_index,
            "input_table_count": len(tables),
            "input_bit_count": len(collect_bits(tables)),
            "input_arity_distribution": arity_distribution(tables),
        }

        tables_by_bits, duplicate_count = collapse_equal_bitsets(tables)
        round_info["collapsed_duplicate_tables"] = duplicate_count
        round_info["canonical_table_count"] = len(tables_by_bits)
        total_duplicate_collapse += duplicate_count
        if duplicate_count:
            changed = True

        intersections, support_counts, projection_stats = build_projection_intersections(
            tables_by_bits,
            args.subset_size_min,
            args.subset_size_max,
        )
        round_info["projection_build"] = projection_stats

        filter_stats = filter_tables_by_projection_intersections(
            tables_by_bits,
            intersections,
            support_counts,
            args.subset_size_min,
            args.subset_size_max,
        )
        round_info["projection_filter"] = filter_stats
        total_row_deletions += filter_stats["row_deletions"]
        if filter_stats["row_deletions"] or filter_stats["changed_tables"]:
            changed = True
        if filter_stats["emptied_tables"]:
            raise RuntimeError("projection intersection reduction emptied a table")

        tables = tables_from_dict(tables_by_bits)
        round_info["output_table_count"] = len(tables)
        round_info["output_bit_count"] = len(collect_bits(tables))
        round_info["output_arity_distribution"] = arity_distribution(tables)
        round_info["changed"] = changed
        rounds.append(round_info)

        if changed:
            productive_rounds += 1
            round_index += 1
        else:
            break

    report = {
        "method": "repeat global projection intersections on subset bitsets and filter tables by the resulting shared allowed projections until no further change",
        "input": args.input,
        "output": args.output,
        "subset_size_min": args.subset_size_min,
        "subset_size_max": args.subset_size_max,
        "initial_table_count": initial_tables,
        "initial_bit_count": len(initial_bits),
        "final_table_count": len(tables),
        "final_bit_count": len(collect_bits(tables)),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": len(rounds),
        "total_duplicate_collapse": total_duplicate_collapse,
        "total_row_deletions": total_row_deletions,
        "final_arity_distribution": arity_distribution(tables),
        "rounds": rounds,
    }

    write_json(Path(args.output), tables)
    write_json(Path(args.report), report)

    print(f"productive rounds: {productive_rounds}")
    print(f"rounds including final check: {len(rounds)}")
    print(f"final tables: {len(tables)}")
    print(f"final bits: {len(collect_bits(tables))}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
