import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from collapse_bit_pairs import (
    build_rewrite_map,
    extract_relations,
    rewrite_tables,
    update_original_mapping,
)
from merge_subset_tables import collapse_equal_bitsets, merge_subsets, prune_included_tables


def tables_from_dict(tables_by_bits: dict[tuple[int, ...], set[int]]) -> list[dict]:
    return [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(tables_by_bits.items(), key=lambda item: (len(item[0]), item[0]))
    ]


def collect_bits(tables: list[dict]) -> list[int]:
    return sorted({bit for table in tables for bit in table["bits"]})


def arity_distribution(tables: list[dict]) -> dict[str, int]:
    return {
        str(arity): count
        for arity, count in sorted(Counter(len(table["bits"]) for table in tables).items())
    }


def write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def build_final_components(
    original_mapping: dict[int, tuple[int, int]],
) -> list[dict]:
    grouped: dict[int, list[dict]] = defaultdict(list)

    for bit, (representative, inverted) in sorted(original_mapping.items()):
        grouped[representative].append(
            {
                "bit": bit,
                "representative": representative,
                "inverted": bool(inverted),
            }
        )

    components = [
        {
            "representative": representative,
            "size": len(members),
            "members": members,
        }
        for representative, members in sorted(grouped.items())
        if len(members) > 1
    ]

    components.sort(key=lambda item: (-item["size"], item["representative"]))
    return components


def build_rewrite_rows(
    original_mapping: dict[int, tuple[int, int]],
) -> list[dict]:
    return [
        {
            "bit": bit,
            "representative": representative,
            "inverted": bool(inverted),
        }
        for bit, (representative, inverted) in sorted(original_mapping.items())
        if bit != representative or inverted
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables.json")
    parser.add_argument("--output", default="tables_fixed_point.json")
    parser.add_argument("--report", default="fixed_point_report.json")
    parser.add_argument("--mapping", default="fixed_point_bit_rewrite_map.json")
    parser.add_argument("--components", default="fixed_point_bit_components.json")
    parser.add_argument("--dropped", default="fixed_point_dropped_tables.json")
    args = parser.parse_args()

    input_path = Path(args.input)
    with input_path.open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    initial_tables = len(tables)
    initial_bits = collect_bits(tables)
    original_mapping = {bit: (bit, 0) for bit in initial_bits}
    dropped_tables_history = []
    rounds = []
    total_duplicate_collapse = 0
    total_row_deletions = 0
    total_dropped_tables = 0
    total_pair_relations = 0
    total_replaced_bits = 0
    productive_rounds = 0
    round_index = 1

    while True:
        round_changed = False
        input_bit_count = len(collect_bits(tables))
        round_info = {
            "round": round_index,
            "input_table_count": len(tables),
            "input_bit_count": input_bit_count,
            "input_arity_distribution": arity_distribution(tables),
        }

        tables_by_bits, duplicate_count = collapse_equal_bitsets(tables)
        round_info["collapsed_duplicate_tables"] = duplicate_count
        round_info["canonical_table_count"] = len(tables_by_bits)
        total_duplicate_collapse += duplicate_count
        if duplicate_count:
            round_changed = True

        merge_stats, pair_details = merge_subsets(tables_by_bits)
        effective_pairs = sum(1 for pair in pair_details if pair["rows_removed"] > 0)
        round_info["subset_superset_pairs"] = merge_stats["pair_count"]
        round_info["effective_subset_pairs"] = effective_pairs
        round_info["subset_changed_tables"] = merge_stats["changed_tables"]
        round_info["subset_row_deletions"] = merge_stats["row_deletions"]
        round_info["emptied_tables_during_subset_merge"] = merge_stats["emptied_tables"]
        total_row_deletions += merge_stats["row_deletions"]
        if merge_stats["row_deletions"] or merge_stats["changed_tables"]:
            round_changed = True

        tables_by_bits, dropped_tables = prune_included_tables(tables_by_bits, pair_details)
        round_info["dropped_included_tables"] = len(dropped_tables)
        total_dropped_tables += len(dropped_tables)
        if dropped_tables:
            dropped_tables_history.extend(
                {
                    "round": round_index,
                    "bits": bits,
                }
                for bits in dropped_tables
            )
            round_changed = True

        tables = tables_from_dict(tables_by_bits)

        pair_iterations = []
        pair_iteration_index = 1
        pair_changed_this_round = False

        while True:
            relations = extract_relations(tables)
            relation_count = len(relations)
            if not relation_count:
                break

            rewrite_map, component_stats = build_rewrite_map(relations)
            original_mapping = update_original_mapping(original_mapping, rewrite_map)
            tables, rewrite_stats = rewrite_tables(tables, rewrite_map)

            pair_iterations.append(
                {
                    "iteration": pair_iteration_index,
                    "relation_pair_count": relation_count,
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

            total_pair_relations += relation_count
            total_replaced_bits += component_stats["replaced_bit_count"]
            round_changed = True
            pair_changed_this_round = True
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
        round_info["changed"] = round_changed
        rounds.append(round_info)

        if round_changed:
            productive_rounds += 1

        if not round_changed:
            break

        round_index += 1

    final_bits = collect_bits(tables)
    rewrite_rows = build_rewrite_rows(original_mapping)
    final_components = build_final_components(original_mapping)
    report = {
        "method": "repeat canonicalization, subset inclusion merge/prune, and equal/opposite pair collapse until no further change",
        "input": args.input,
        "output": args.output,
        "initial_table_count": initial_tables,
        "initial_bit_count": len(initial_bits),
        "final_table_count": len(tables),
        "final_bit_count": len(final_bits),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": len(rounds),
        "total_duplicate_collapse": total_duplicate_collapse,
        "total_subset_row_deletions": total_row_deletions,
        "total_dropped_included_tables": total_dropped_tables,
        "total_pair_relation_pairs_found": total_pair_relations,
        "total_replaced_bits_across_pair_reductions": total_replaced_bits,
        "rewritten_original_bits": len(rewrite_rows),
        "final_components_with_rewrites": len(final_components),
        "final_arity_distribution": arity_distribution(tables),
        "rounds": rounds,
    }

    write_json(Path(args.output), tables)
    write_json(Path(args.report), report)
    write_json(Path(args.mapping), rewrite_rows)
    write_json(Path(args.components), final_components)
    write_json(Path(args.dropped), dropped_tables_history)

    print(f"productive rounds: {productive_rounds}")
    print(f"rounds including final check: {len(rounds)}")
    print(f"final tables: {len(tables)}")
    print(f"final bits: {len(final_bits)}")
    print(f"rewritten original bits: {len(rewrite_rows)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
