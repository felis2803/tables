from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Callable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.steps.forced_bits import (
    collect_forced_bits_bitwise,
    propagate_forced_bits,
    update_original_forced,
)
from src.steps.pair_reduction import (
    build_rewrite_map,
    extract_relations,
    rewrite_tables,
    update_original_mapping,
)
from src.steps.node_filter import build_nodes, filter_tables_with_nodes, serialize_nodes
from src.steps.subset_absorption import collapse_equal_bitsets, merge_subsets, prune_included_tables
from src.utils.artifacts import (
    STAGE_COMMON_NODE_FIXED_POINT,
    components_artifact,
    dropped_tables_artifact,
    forced_bits_artifact,
    nodes_artifact,
    pair_relations_artifact,
    report_artifact,
    rewrite_map_artifact,
    table_artifact,
)
from src.utils.paths import RAW_DIR
from src.utils.rank_stats import summarize_table_ranks


PipelineState = dict[str, object]
PipelineStep = Callable[[list[dict], PipelineState, int], tuple[list[dict], dict, bool]]


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


def initialize_pipeline_state(tables: list[dict]) -> PipelineState:
    initial_bits = collect_bits(tables)
    return {
        "original_mapping": {bit: (bit, 0) for bit in initial_bits},
        "original_forced": {},
        "dropped_tables_history": [],
        "pair_relations_history": [],
        "final_nodes": [],
    }


def step_subset_absorption(
    tables: list[dict],
    state: PipelineState,
    round_index: int,
) -> tuple[list[dict], dict, bool]:
    tables_by_bits, duplicate_count = collapse_equal_bitsets(tables)
    merge_stats, pair_details = merge_subsets(tables_by_bits)
    effective_pairs = sum(1 for pair in pair_details if pair["rows_removed"] > 0)
    tables_by_bits, dropped_tables = prune_included_tables(tables_by_bits, pair_details)

    if dropped_tables:
        history = state["dropped_tables_history"]
        assert isinstance(history, list)
        history.extend(
            {
                "round": round_index,
                "bits": bits,
            }
            for bits in dropped_tables
        )

    info = {
        "collapsed_duplicate_tables": duplicate_count,
        "canonical_table_count": len(tables_by_bits),
        "subset_superset_pairs": merge_stats["pair_count"],
        "effective_subset_pairs": effective_pairs,
        "subset_changed_tables": merge_stats["changed_tables"],
        "subset_row_deletions": merge_stats["row_deletions"],
        "emptied_tables_during_subset_merge": merge_stats["emptied_tables"],
        "dropped_included_tables": len(dropped_tables),
    }
    changed = any(
        [
            duplicate_count,
            merge_stats["row_deletions"],
            merge_stats["changed_tables"],
            dropped_tables,
        ]
    )
    return tables_from_dict(tables_by_bits), info, changed


def step_forced_bits(
    tables: list[dict],
    state: PipelineState,
    _round_index: int,
) -> tuple[list[dict], dict, bool]:
    forced_current, forced_occurrences = collect_forced_bits_bitwise(tables)
    original_mapping = state["original_mapping"]
    original_forced = state["original_forced"]
    assert isinstance(original_mapping, dict)
    assert isinstance(original_forced, dict)

    if forced_current:
        update_original_forced(original_mapping, original_forced, forced_current)
        tables, forced_stats = propagate_forced_bits(tables, forced_current)
    else:
        forced_stats = {
            "affected_tables": 0,
            "changed_tables": 0,
            "removed_rows": 0,
            "removed_tautologies": 0,
            "collapsed_duplicate_tables": 0,
        }

    info = {
        "forced_bits": len(forced_current),
        "forced_one_bits": sum(1 for value in forced_current.values() if value == 1),
        "forced_zero_bits": sum(1 for value in forced_current.values() if value == 0),
        "forced_occurrences": forced_occurrences,
        "stats": forced_stats,
    }
    return tables, info, bool(forced_current)


def step_pair_reduction(
    tables: list[dict],
    state: PipelineState,
    round_index: int,
) -> tuple[list[dict], dict, bool]:
    original_mapping = state["original_mapping"]
    relation_history = state["pair_relations_history"]
    assert isinstance(original_mapping, dict)
    assert isinstance(relation_history, list)

    iterations = []
    iteration_index = 1
    changed = False

    while True:
        relations = extract_relations(tables)
        if not relations:
            break

        rewrite_map, component_stats = build_rewrite_map(relations)
        updated_mapping = update_original_mapping(original_mapping, rewrite_map)
        original_mapping.clear()
        original_mapping.update(updated_mapping)
        tables, rewrite_stats = rewrite_tables(tables, rewrite_map)

        relation_history.extend(
            {
                "round": round_index,
                "iteration": iteration_index,
                "left": relation["left"],
                "right": relation["right"],
                "equal": relation["relation"] == 0,
                "inverted": relation["relation"] == 1,
                "support": relation["support"],
                "source_arities": sorted(relation["sources"]),
            }
            for relation in relations
        )

        iterations.append(
            {
                "iteration": iteration_index,
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

        changed = True
        iteration_index += 1

    info = {
        "iterations": iterations,
        "pair_relation_pairs_total": sum(item["relation_pair_count"] for item in iterations),
        "pair_replaced_bits_total": sum(item["replaced_bit_count"] for item in iterations),
    }
    return tables, info, changed


def step_node_filter(
    tables: list[dict],
    state: PipelineState,
    _round_index: int,
) -> tuple[list[dict], dict, bool]:
    mutable_tables = [
        {"bits": table["bits"], "rows": set(table["rows"])}
        for table in tables
    ]

    nodes, table_to_nodes, node_build_stats = build_nodes(mutable_tables)
    filter_stats = filter_tables_with_nodes(mutable_tables, nodes, table_to_nodes)
    output_tables = [
        {"bits": table["bits"], "rows": sorted(table["rows"])}
        for table in mutable_tables
    ]
    state["final_nodes"] = serialize_nodes(nodes)

    info = {
        "node_build": node_build_stats,
        "filter": filter_stats,
    }
    changed = bool(filter_stats["row_deletions"] or filter_stats["changed_tables"])
    return output_tables, info, changed


def run_reduction_pipeline(
    tables: list[dict],
    steps: list[tuple[str, PipelineStep]],
    state: PipelineState,
) -> tuple[list[dict], list[dict], int]:
    rounds = []
    productive_rounds = 0
    round_index = 1

    while True:
        changed = False
        round_info = {
            "round": round_index,
            "input_table_count": len(tables),
            "input_bit_count": len(collect_bits(tables)),
            "input_arity_distribution": arity_distribution(tables),
            "input_rank_summary": summarize_table_ranks(tables),
        }

        for step_name, step in steps:
            tables, info, step_changed = step(tables, state, round_index)
            round_info[step_name] = info
            changed = changed or step_changed

        round_info["output_table_count"] = len(tables)
        round_info["output_bit_count"] = len(collect_bits(tables))
        round_info["output_arity_distribution"] = arity_distribution(tables)
        round_info["output_rank_summary"] = summarize_table_ranks(tables)
        round_info["changed"] = changed
        rounds.append(round_info)

        if not changed:
            break

        productive_rounds += 1
        round_index += 1

    return tables, rounds, productive_rounds


def build_rewrite_rows(
    original_mapping: dict[int, tuple[int, int]],
    original_forced: dict[int, int],
) -> list[dict]:
    return [
        {
            "bit": bit,
            "representative": representative,
            "inverted": bool(inverted),
        }
        for bit, (representative, inverted) in sorted(original_mapping.items())
        if bit not in original_forced and (bit != representative or inverted)
    ]


def build_final_components(
    original_mapping: dict[int, tuple[int, int]],
    original_forced: dict[int, int],
) -> list[dict]:
    grouped: dict[int, list[dict]] = defaultdict(list)

    for bit, (representative, inverted) in sorted(original_mapping.items()):
        if bit in original_forced:
            continue
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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(RAW_DIR / "tables.json"))
    parser.add_argument(
        "--output",
        default=str(table_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--report",
        default=str(report_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--forced",
        default=str(forced_bits_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--mapping",
        default=str(rewrite_map_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--components",
        default=str(components_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--dropped",
        default=str(dropped_tables_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--relations",
        default=str(pair_relations_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument(
        "--nodes",
        default=str(nodes_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    args = parser.parse_args()

    with Path(args.input).open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    initial_table_count = len(tables)
    initial_bits = collect_bits(tables)
    initial_rank_summary = summarize_table_ranks(tables)
    state = initialize_pipeline_state(tables)
    steps: list[tuple[str, PipelineStep]] = [
        ("subset_absorption", step_subset_absorption),
        ("forced_bits", step_forced_bits),
        ("pair_reduction", step_pair_reduction),
        ("node_filter", step_node_filter),
    ]

    tables, rounds, productive_rounds = run_reduction_pipeline(tables, steps, state)

    original_mapping = state["original_mapping"]
    original_forced = state["original_forced"]
    dropped_tables_history = state["dropped_tables_history"]
    pair_relations_history = state["pair_relations_history"]
    final_nodes = state["final_nodes"]
    assert isinstance(original_mapping, dict)
    assert isinstance(original_forced, dict)
    assert isinstance(dropped_tables_history, list)
    assert isinstance(pair_relations_history, list)
    assert isinstance(final_nodes, list)

    forced_rows = [
        {"bit": bit, "value": value}
        for bit, value in sorted(original_forced.items())
    ]
    rewrite_rows = build_rewrite_rows(original_mapping, original_forced)
    final_components = build_final_components(original_mapping, original_forced)

    report = {
        "method": "repeat subset absorption, AND/OR fixed-bit propagation/removal, equal/opposite pair reduction, and node-based projection intersection filtering until no further change",
        "steps": [name for name, _ in steps],
        "input": args.input,
        "output": args.output,
        "nodes_output": args.nodes,
        "initial_table_count": initial_table_count,
        "initial_bit_count": len(initial_bits),
        "initial_rank_summary": initial_rank_summary,
        "final_table_count": len(tables),
        "final_bit_count": len(collect_bits(tables)),
        "final_rank_summary": summarize_table_ranks(tables),
        "productive_round_count": productive_rounds,
        "round_count_including_final_check": len(rounds),
        "total_collapsed_duplicate_tables_in_subset_step": sum(
            round_info["subset_absorption"]["collapsed_duplicate_tables"] for round_info in rounds
        ),
        "total_subset_row_deletions": sum(
            round_info["subset_absorption"]["subset_row_deletions"] for round_info in rounds
        ),
        "total_dropped_included_tables": len(dropped_tables_history),
        "total_forced_bits_detected_across_rounds": sum(
            round_info["forced_bits"]["forced_bits"] for round_info in rounds
        ),
        "total_forced_occurrences": sum(
            round_info["forced_bits"]["forced_occurrences"] for round_info in rounds
        ),
        "total_removed_rows_in_forced_step": sum(
            round_info["forced_bits"]["stats"]["removed_rows"] for round_info in rounds
        ),
        "total_removed_tautologies": sum(
            round_info["forced_bits"]["stats"]["removed_tautologies"] for round_info in rounds
        ),
        "final_forced_original_bits": len(forced_rows),
        "total_pair_relation_pairs_found": len(pair_relations_history),
        "total_pair_replaced_bits": sum(
            round_info["pair_reduction"]["pair_replaced_bits_total"] for round_info in rounds
        ),
        "total_nodes_built": sum(
            round_info["node_filter"]["node_build"]["node_count"] for round_info in rounds
        ),
        "total_initial_restrictive_nodes": sum(
            round_info["node_filter"]["node_build"]["restrictive_node_count"] for round_info in rounds
        ),
        "total_node_changed_tables": sum(
            round_info["node_filter"]["filter"]["changed_tables"] for round_info in rounds
        ),
        "total_node_row_deletions": sum(
            round_info["node_filter"]["filter"]["row_deletions"] for round_info in rounds
        ),
        "total_node_recomputations": sum(
            round_info["node_filter"]["filter"]["node_recomputations"] for round_info in rounds
        ),
        "total_node_tightenings": sum(
            round_info["node_filter"]["filter"]["node_tightenings"] for round_info in rounds
        ),
        "final_rewritten_original_bits": len(rewrite_rows),
        "final_components_with_rewrites": len(final_components),
        "final_node_count": len(final_nodes),
        "final_arity_distribution": arity_distribution(tables),
        "rounds": rounds,
    }

    write_json(Path(args.output), tables)
    write_json(Path(args.report), report)
    write_json(Path(args.forced), forced_rows)
    write_json(Path(args.mapping), rewrite_rows)
    write_json(Path(args.components), final_components)
    write_json(Path(args.dropped), dropped_tables_history)
    write_json(Path(args.relations), pair_relations_history)
    write_json(Path(args.nodes), final_nodes)

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
