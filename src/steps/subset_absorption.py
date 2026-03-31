from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.artifacts import (
    STAGE_SUBSET_MERGED,
    STAGE_SUBSET_PRUNED,
    dropped_tables_artifact,
    report_artifact,
    subset_pairs_artifact,
    table_artifact,
)
from src.utils.paths import RAW_DIR
from src.utils.rank_stats import summarize_table_ranks, summarize_tables_by_bits_ranks


def canonicalize_table(table: dict) -> tuple[tuple[int, ...], set[int]]:
    bits = table["bits"]
    order = sorted(range(len(bits)), key=bits.__getitem__)
    sorted_bits = tuple(bits[index] for index in order)

    rows = set()
    for row in table["rows"]:
        mapped = 0
        for new_offset, old_offset in enumerate(order):
            if (row >> old_offset) & 1:
                mapped |= 1 << new_offset
        rows.add(mapped)

    return sorted_bits, rows


def collapse_equal_bitsets(tables: list[dict]) -> tuple[dict[tuple[int, ...], set[int]], int]:
    merged: dict[tuple[int, ...], set[int]] = {}
    duplicate_count = 0

    for table in tables:
        bits, rows = canonicalize_table(table)
        if bits in merged:
            merged[bits].intersection_update(rows)
            duplicate_count += 1
        else:
            merged[bits] = set(rows)

    return merged, duplicate_count


def project_row(row: int, subset_indices: tuple[int, ...]) -> int:
    projected = 0
    for new_offset, old_offset in enumerate(subset_indices):
        if (row >> old_offset) & 1:
            projected |= 1 << new_offset
    return projected


def merge_subsets(
    tables_by_bits: dict[tuple[int, ...], set[int]]
) -> tuple[dict[str, int], list[dict]]:
    lengths_present = {len(bits) for bits in tables_by_bits}
    pair_count = 0
    changed_tables = 0
    row_deletions = 0
    emptied_tables = 0
    pair_details = []

    for sup_bits in sorted(tables_by_bits, key=lambda bits: (len(bits), bits)):
        sup_rows = tables_by_bits[sup_bits]
        original_size = len(sup_rows)
        changed_here = False
        bit_count = len(sup_bits)

        for subset_size in sorted(length for length in lengths_present if 0 < length < bit_count):
            for subset_indices in combinations(range(bit_count), subset_size):
                subset_bits = tuple(sup_bits[index] for index in subset_indices)
                subset_rows = tables_by_bits.get(subset_bits)
                if subset_rows is None:
                    continue

                pair_count += 1
                before_count = len(sup_rows)
                filtered_rows = {
                    row
                    for row in sup_rows
                    if project_row(row, subset_indices) in subset_rows
                }
                removed = before_count - len(filtered_rows)
                pair_details.append(
                    {
                        "subset_bits": list(subset_bits),
                        "superset_bits": list(sup_bits),
                        "rows_removed": removed,
                    }
                )

                if len(filtered_rows) != len(sup_rows):
                    row_deletions += removed
                    sup_rows = filtered_rows
                    tables_by_bits[sup_bits] = sup_rows
                    changed_here = True
                    if not sup_rows:
                        emptied_tables += 1
                        break

            if not sup_rows:
                break

        if changed_here:
            changed_tables += 1

    return (
        {
            "pair_count": pair_count,
            "changed_tables": changed_tables,
            "row_deletions": row_deletions,
            "emptied_tables": emptied_tables,
        },
        pair_details,
    )


def write_tables(path: Path, tables_by_bits: dict[tuple[int, ...], set[int]]) -> None:
    payload = [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(tables_by_bits.items(), key=lambda item: (len(item[0]), item[0]))
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
        fh.write("\n")


def prune_included_tables(
    tables_by_bits: dict[tuple[int, ...], set[int]],
    pair_details: list[dict],
) -> tuple[dict[tuple[int, ...], set[int]], list[list[int]]]:
    subset_keys = {
        tuple(pair["subset_bits"])
        for pair in pair_details
    }
    pruned = {
        bits: rows
        for bits, rows in tables_by_bits.items()
        if bits not in subset_keys
    }
    dropped = [list(bits) for bits in sorted(subset_keys) if bits in tables_by_bits]
    return pruned, dropped


def write_report(
    path: Path,
    input_tables: list[dict],
    original_count: int,
    duplicate_count: int,
    canonical_count_before_prune: int,
    tables_by_bits: dict[tuple[int, ...], set[int]],
    merge_stats: dict[str, int],
    pair_details: list[dict],
    dropped_tables: list[list[int]],
) -> None:
    row_counts = [len(rows) for rows in tables_by_bits.values()]
    report = {
        "method": "canonicalize bit order, intersect equal bitsets, merge strict subset tables into supersets",
        "original_table_count": original_count,
        "canonical_table_count_before_prune": canonical_count_before_prune,
        "final_table_count": len(tables_by_bits),
        "collapsed_duplicate_tables": duplicate_count,
        "subset_superset_pairs": merge_stats["pair_count"],
        "effective_pairs": sum(1 for pair in pair_details if pair["rows_removed"] > 0),
        "dropped_included_tables": len(dropped_tables),
        "changed_tables": merge_stats["changed_tables"],
        "row_deletions": merge_stats["row_deletions"],
        "emptied_tables": merge_stats["emptied_tables"],
        "min_rows_after_merge": min(row_counts) if row_counts else 0,
        "max_rows_after_merge": max(row_counts) if row_counts else 0,
        "input_rank_summary": summarize_table_ranks(input_tables),
        "final_rank_summary": summarize_tables_by_bits_ranks(tables_by_bits),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def write_pairs(path: Path, pair_details: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(pair_details, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def write_dropped_tables(path: Path, dropped_tables: list[list[int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(dropped_tables, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(RAW_DIR / "tables.json"))
    parser.add_argument("--output")
    parser.add_argument("--report")
    parser.add_argument("--pairs")
    parser.add_argument("--dropped")
    parser.add_argument("--prune-included", action="store_true")
    args = parser.parse_args()

    stage = STAGE_SUBSET_PRUNED if args.prune_included else STAGE_SUBSET_MERGED
    output_path = Path(args.output) if args.output else table_artifact(stage)
    report_path = Path(args.report) if args.report else report_artifact(stage)
    pairs_path = Path(args.pairs) if args.pairs else subset_pairs_artifact(stage)
    dropped_path = Path(args.dropped) if args.dropped else dropped_tables_artifact(stage)

    input_path = Path(args.input)
    with input_path.open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    tables_by_bits, duplicate_count = collapse_equal_bitsets(tables)
    merge_stats, pair_details = merge_subsets(tables_by_bits)
    canonical_count_before_prune = len(tables_by_bits)
    dropped_tables: list[list[int]] = []

    if args.prune_included:
        tables_by_bits, dropped_tables = prune_included_tables(tables_by_bits, pair_details)

    write_tables(output_path, tables_by_bits)
    write_report(
        report_path,
        input_tables=tables,
        original_count=len(tables),
        duplicate_count=duplicate_count,
        canonical_count_before_prune=canonical_count_before_prune,
        tables_by_bits=tables_by_bits,
        merge_stats=merge_stats,
        pair_details=pair_details,
        dropped_tables=dropped_tables,
    )
    write_pairs(pairs_path, pair_details)
    write_dropped_tables(dropped_path, dropped_tables)

    print(f"original tables: {len(tables)}")
    print(f"canonical tables before prune: {canonical_count_before_prune}")
    print(f"final tables: {len(tables_by_bits)}")
    print(f"duplicate tables collapsed: {duplicate_count}")
    print(f"subset/superset pairs: {merge_stats['pair_count']}")
    print(f"changed tables: {merge_stats['changed_tables']}")
    print(f"row deletions: {merge_stats['row_deletions']}")
    print(f"emptied tables: {merge_stats['emptied_tables']}")
    print(f"dropped included tables: {len(dropped_tables)}")
    print(f"output: {output_path}")
    print(f"report: {report_path}")
    print(f"pairs: {pairs_path}")
    print(f"dropped: {dropped_path}")


if __name__ == "__main__":
    main()
