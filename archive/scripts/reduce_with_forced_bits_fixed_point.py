import argparse
import json
from collections import Counter
from pathlib import Path

from collapse_bit_pairs import (
    build_rewrite_map,
    extract_relations,
    rewrite_tables,
    update_original_mapping,
)
from merge_subset_tables import collapse_equal_bitsets, merge_subsets, prune_included_tables


def arity_distribution(tables: list[dict]) -> dict[str, int]:
    return {
        str(arity): count
        for arity, count in sorted(Counter(len(table["bits"]) for table in tables).items())
    }


def collect_bits(tables: list[dict]) -> list[int]:
    return sorted({bit for table in tables for bit in table["bits"]})


def tables_from_dict(tables_by_bits: dict[tuple[int, ...], set[int]]) -> list[dict]:
    return [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(tables_by_bits.items(), key=lambda item: (len(item[0]), item[0]))
    ]


def collect_forced_bits(tables: list[dict]) -> tuple[dict[int, int], int]:
    forced: dict[int, int] = {}
    occurrences = 0

    for table in tables:
        bits = table["bits"]
        rows = table["rows"]

        for index, bit in enumerate(bits):
            values = {((row >> index) & 1) for row in rows}
            if len(values) != 1:
                continue

            value = next(iter(values))
            current = forced.get(bit)
            if current is not None and current != value:
                raise RuntimeError(f"conflicting forced values for bit {bit}")

            forced[bit] = value
            occurrences += 1

    return forced, occurrences


def propagate_forced_bits(
    tables: list[dict],
    forced: dict[int, int],
) -> tuple[list[dict], dict]:
    projected = []
    affected_tables = 0
    changed_tables = 0
    removed_rows = 0
    removed_tautologies = 0

    for table in tables:
        bits = table["bits"]
        rows = table["rows"]
        touches_forced = any(bit in forced for bit in bits)
        if touches_forced:
            affected_tables += 1

        kept_bits = []
        kept_indices = []
        for index, bit in enumerate(bits):
            if bit not in forced:
                kept_bits.append(bit)
                kept_indices.append(index)

        new_rows = set()
        for row in rows:
            for index, bit in enumerate(bits):
                if bit in forced and ((row >> index) & 1) != forced[bit]:
                    break
            else:
                new_row = 0
                for new_index, old_index in enumerate(kept_indices):
                    if (row >> old_index) & 1:
                        new_row |= 1 << new_index
                new_rows.add(new_row)

        removed_rows += len(rows) - len(new_rows)
        if touches_forced and (kept_bits != bits or new_rows != set(rows)):
            changed_tables += 1

        if not kept_bits:
            if new_rows == {0}:
                removed_tautologies += 1
                continue
            raise RuntimeError(f"contradiction after forcing table {table}")

        if len(new_rows) == (1 << len(kept_bits)):
            removed_tautologies += 1
            continue

        projected.append({"bits": kept_bits, "rows": sorted(new_rows)})

    canonical, duplicate_count = collapse_equal_bitsets(projected)
    output_tables = tables_from_dict(canonical)

    return output_tables, {
        "affected_tables": affected_tables,
        "changed_tables": changed_tables,
        "removed_rows": removed_rows,
        "removed_tautologies": removed_tautologies,
        "collapsed_duplicate_tables": duplicate_count,
    }


def update_original_forced(
    original_mapping: dict[int, tuple[int, int]],
    original_forced: dict[int, int],
    forced_current: dict[int, int],
) -> None:
    for bit, (current, inverted) in original_mapping.items():
        if current not in forced_current:
            continue
        value = forced_current[current] ^ inverted
        existing = original_forced.get(bit)
        if existing is not None and existing != value:
            raise RuntimeError(f"conflicting final value for original bit {bit}")
        original_forced[bit] = value


def write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables_fixed_point_unary.json")
    parser.add_argument("--output", default="tables_forced_fixed_point.json")
    parser.add_argument("--report", default="forced_fixed_point_report.json")
    parser.add_argument("--forced", default="forced_bits_propagated.json")
    parser.add_argument("--mapping", default="forced_fixed_point_bit_rewrite_map.json")
    args = parser.parse_args()

    with Path(args.input).open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    initial_tables = len(tables)
    initial_bits = collect_bits(tables)
    original_mapping = {bit: (bit, 0) for bit in initial_bits}
    original_forced: dict[int, int] = {}

    rounds = []
    total_forced_bits = 0
    total_forced_occurrences = 0
    total_removed_tautologies = 0
    total_subset_row_deletions = 0
    total_dropped_tables = 0
    total_pair_relations = 0
    total_pair_replaced_bits = 0
    productive_rounds = 0
    round_index = 1

    while True:
        changed = False
        round_info = {
            "round": round_index,
            "input_table_count": len(tables),
            "input_bit_count": len(collect_bits(tables)),
            "input_arity_distribution": arity_distribution(tables),
        }

        forced_current, forced_occurrences = collect_forced_bits(tables)
        round_info["forced_bits"] = len(forced_current)
        round_info["forced_occurrences"] = forced_occurrences
        total_forced_bits += len(forced_current)
        total_forced_occurrences += forced_occurrences

        if forced_current:
            update_original_forced(original_mapping, original_forced, forced_current)
            tables, forced_stats = propagate_forced_bits(tables, forced_current)
            round_info["forced_stats"] = forced_stats
            total_removed_tautologies += forced_stats["removed_tautologies"]
            if any(forced_stats.values()):
                changed = True
        else:
            round_info["forced_stats"] = {
                "affected_tables": 0,
                "changed_tables": 0,
                "removed_rows": 0,
                "removed_tautologies": 0,
                "collapsed_duplicate_tables": 0,
            }

        tables_by_bits, duplicate_count = collapse_equal_bitsets(tables)
        round_info["collapsed_duplicate_tables"] = duplicate_count
        round_info["canonical_table_count"] = len(tables_by_bits)
        if duplicate_count:
            changed = True

        merge_stats, pair_details = merge_subsets(tables_by_bits)
        round_info["subset_superset_pairs"] = merge_stats["pair_count"]
        round_info["effective_subset_pairs"] = sum(
            1 for pair in pair_details if pair["rows_removed"] > 0
        )
        round_info["subset_row_deletions"] = merge_stats["row_deletions"]
        round_info["subset_changed_tables"] = merge_stats["changed_tables"]
        total_subset_row_deletions += merge_stats["row_deletions"]
        if merge_stats["row_deletions"] or merge_stats["changed_tables"]:
            changed = True

        tables_by_bits, dropped_tables = prune_included_tables(tables_by_bits, pair_details)
        round_info["dropped_included_tables"] = len(dropped_tables)
        total_dropped_tables += len(dropped_tables)
        if dropped_tables:
            changed = True

        tables = tables_from_dict(tables_by_bits)

        pair_iterations = []
        pair_iteration_index = 1
        while True:
            relations = extract_relations(tables)
            if not relations:
                break

            rewrite_map, component_stats = build_rewrite_map(relations)
            original_mapping = update_original_mapping(original_mapping, rewrite_map)
            tables, rewrite_stats = rewrite_tables(tables, rewrite_map)

            pair_iterations.append(
                {
                    "iteration": pair_iteration_index,
                    "relation_pair_count": len(relations),
                    "bits_involved": component_stats["bits_involved"],
                    "component_count": component_stats["component_count"],
                    "replaced_bit_count": component_stats["replaced_bit_count"],
                    "changed_tables": rewrite_stats["changed_tables"],
                    "reduced_arity_tables": rewrite_stats["reduced_arity_tables"],
                    "same_arity_changed_tables": rewrite_stats["same_arity_changed_tables"],
                    "removed_rows": rewrite_stats["removed_rows"],
                    "collapsed_duplicate_tables": rewrite_stats["collapsed_duplicate_tables"],
                    "table_count_after_iteration": len(tables),
                    "bit_count_after_iteration": len(collect_bits(tables)),
                }
            )

            total_pair_relations += len(relations)
            total_pair_replaced_bits += component_stats["replaced_bit_count"]
            changed = True
            pair_iteration_index += 1

        round_info["pair_iterations"] = pair_iterations
        round_info["pair_relation_pairs_total"] = sum(
            item["relation_pair_count"] for item in pair_iterations
        )
        round_info["pair_replaced_bits_total"] = sum(
            item["replaced_bit_count"] for item in pair_iterations
        )
        round_info["output_table_count"] = len(tables)
        round_info["output_bit_count"] = len(collect_bits(tables))
        round_info["output_arity_distribution"] = arity_distribution(tables)
        round_info["changed"] = changed
        rounds.append(round_info)

        if changed:
            productive_rounds += 1
        else:
            break

        round_index += 1

    forced_rows = [
        {"bit": bit, "value": value}
        for bit, value in sorted(original_forced.items())
    ]
    rewrite_rows = [
        {
            "bit": bit,
            "representative": representative,
            "inverted": bool(inverted),
        }
        for bit, (representative, inverted) in sorted(original_mapping.items())
        if bit not in original_forced and (bit != representative or inverted)
    ]

    report = {
        "method": "repeat explicit forced-bit propagation, subset inclusion merge/prune, and equal/opposite pair collapse until no further change",
        "input": args.input,
        "output": args.output,
        "initial_table_count": initial_tables,
        "initial_bit_count": len(initial_bits),
        "final_table_count": len(tables),
        "final_bit_count": len(collect_bits(tables)),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": len(rounds),
        "total_forced_bits_detected_across_rounds": total_forced_bits,
        "total_forced_occurrences": total_forced_occurrences,
        "total_removed_tautologies": total_removed_tautologies,
        "total_subset_row_deletions": total_subset_row_deletions,
        "total_dropped_included_tables": total_dropped_tables,
        "total_pair_relation_pairs_found": total_pair_relations,
        "total_pair_replaced_bits": total_pair_replaced_bits,
        "final_forced_original_bits": len(forced_rows),
        "final_rewritten_original_bits": len(rewrite_rows),
        "final_arity_distribution": arity_distribution(tables),
        "rounds": rounds,
    }

    write_json(Path(args.output), tables)
    write_json(Path(args.report), report)
    write_json(Path(args.forced), forced_rows)
    write_json(Path(args.mapping), rewrite_rows)

    print(f"productive rounds: {productive_rounds}")
    print(f"rounds including final check: {len(rounds)}")
    print(f"final tables: {len(tables)}")
    print(f"final bits: {len(collect_bits(tables))}")
    print(f"forced original bits: {len(forced_rows)}")
    print(f"rewritten original bits: {len(rewrite_rows)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
