from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict, deque
from itertools import combinations
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.artifacts import (
    STAGE_COMMON_FIXED_POINT,
    STAGE_COMMON_FIXED_POINT_NODE_FILTERED,
    nodes_artifact,
    report_artifact,
    table_artifact,
)


def arity_distribution(tables: list[dict]) -> dict[str, int]:
    return {
        str(arity): count
        for arity, count in sorted(Counter(len(table["bits"]) for table in tables).items())
    }


def collect_bits(tables: list[dict]) -> list[int]:
    return sorted({bit for table in tables for bit in table["bits"]})


def total_rows(tables: list[dict]) -> int:
    return sum(len(table["rows"]) for table in tables)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def project_row(row: int, subset_indices: tuple[int, ...]) -> int:
    projected = 0
    for new_offset, old_offset in enumerate(subset_indices):
        if (row >> old_offset) & 1:
            projected |= 1 << new_offset
    return projected


def build_subset_support(tables: list[dict]) -> dict[tuple[int, ...], list[int]]:
    subset_to_tables: dict[tuple[int, ...], list[int]] = defaultdict(list)

    for table_index, table in enumerate(tables):
        bits = table["bits"]
        for subset_size in range(2, len(bits) + 1):
            for subset_indices in combinations(range(len(bits)), subset_size):
                subset_bits = tuple(bits[index] for index in subset_indices)
                subset_to_tables[subset_bits].append(table_index)

    return subset_to_tables


def exact_intersection_members(
    subset_bits: tuple[int, ...],
    support_tables: list[int],
    tables: list[dict],
) -> set[int]:
    subset_bit_set = set(subset_bits)
    extras = []
    has_exact_table = False

    for table_index in support_tables:
        extra_bits = tuple(bit for bit in tables[table_index]["bits"] if bit not in subset_bit_set)
        extras.append((table_index, set(extra_bits)))
        if not extra_bits:
            has_exact_table = True

    if has_exact_table:
        return set(support_tables)

    members: set[int] = set()
    for left_index in range(len(extras)):
        left_table, left_extra = extras[left_index]
        for right_index in range(left_index + 1, len(extras)):
            right_table, right_extra = extras[right_index]
            if left_extra.isdisjoint(right_extra):
                members.add(left_table)
                members.add(right_table)

    return members


def compute_allowed_rows(
    node: dict,
    tables: list[dict],
) -> set[int]:
    allowed_rows: set[int] | None = None

    for table_index in node["members"]:
        subset_indices = node["member_indices"][table_index]
        projected_rows = {
            project_row(row, subset_indices)
            for row in tables[table_index]["rows"]
        }
        if allowed_rows is None:
            allowed_rows = projected_rows
        else:
            allowed_rows.intersection_update(projected_rows)

        if not allowed_rows:
            break

    if not allowed_rows:
        raise RuntimeError(f"empty node intersection for bits {node['bits']}")

    return allowed_rows


def build_nodes(
    tables: list[dict],
) -> tuple[list[dict], list[list[int]], dict[str, int]]:
    subset_to_tables = build_subset_support(tables)
    table_to_nodes = [[] for _ in tables]
    nodes = []
    support_histogram: Counter = Counter()
    restrictive_nodes = 0

    for subset_bits, support_tables in sorted(subset_to_tables.items(), key=lambda item: (len(item[0]), item[0])):
        if len(support_tables) < 2:
            continue

        members = sorted(exact_intersection_members(subset_bits, support_tables, tables))
        if len(members) < 2:
            continue

        member_indices = {}
        for table_index in members:
            index_by_bit = {
                bit: offset
                for offset, bit in enumerate(tables[table_index]["bits"])
            }
            member_indices[table_index] = tuple(index_by_bit[bit] for bit in subset_bits)

        node = {
            "bits": subset_bits,
            "members": members,
            "member_indices": member_indices,
        }
        allowed_rows = compute_allowed_rows(node, tables)
        node["rows"] = allowed_rows
        node["full_row_count"] = 1 << len(subset_bits)
        node["is_restrictive"] = len(allowed_rows) < node["full_row_count"]
        if node["is_restrictive"]:
            restrictive_nodes += 1

        node_index = len(nodes)
        nodes.append(node)
        for table_index in members:
            table_to_nodes[table_index].append(node_index)
        support_histogram[len(members)] += 1

    return nodes, table_to_nodes, {
        "node_count": len(nodes),
        "restrictive_node_count": restrictive_nodes,
        "support_histogram": {
            str(size): support_histogram[size]
            for size in sorted(support_histogram)
        },
    }


def filter_tables_with_nodes(
    tables: list[dict],
    nodes: list[dict],
    table_to_nodes: list[list[int]],
) -> dict[str, int]:
    queue = deque()
    queued = bytearray(len(nodes))
    for node_index, node in enumerate(nodes):
        if node["is_restrictive"]:
            queue.append(node_index)
            queued[node_index] = 1

    changed_tables = 0
    row_deletions = 0
    node_recomputations = 0
    node_tightenings = 0
    touched_tables = set()

    while queue:
        node_index = queue.popleft()
        queued[node_index] = 0
        node = nodes[node_index]
        changed_here = []

        for table_index in node["members"]:
            subset_indices = node["member_indices"][table_index]
            rows = tables[table_index]["rows"]
            filtered_rows = {
                row
                for row in rows
                if project_row(row, subset_indices) in node["rows"]
            }
            if not filtered_rows:
                raise RuntimeError(
                    f"node filtering emptied table {table_index} for node bits {list(node['bits'])}"
                )
            if len(filtered_rows) != len(rows):
                tables[table_index]["rows"] = filtered_rows
                changed_here.append(table_index)
                touched_tables.add(table_index)
                row_deletions += len(rows) - len(filtered_rows)

        if not changed_here:
            continue

        affected_nodes = set()
        for table_index in changed_here:
            affected_nodes.update(table_to_nodes[table_index])

        for affected_node_index in affected_nodes:
            affected_node = nodes[affected_node_index]
            old_rows = affected_node["rows"]
            new_rows = compute_allowed_rows(affected_node, tables)
            node_recomputations += 1
            if new_rows != old_rows:
                affected_node["rows"] = new_rows
                affected_node["is_restrictive"] = len(new_rows) < affected_node["full_row_count"]
                node_tightenings += 1
                if not queued[affected_node_index]:
                    queue.append(affected_node_index)
                    queued[affected_node_index] = 1

    changed_tables = len(touched_tables)
    restrictive_nodes = sum(1 for node in nodes if node["is_restrictive"])
    return {
        "changed_tables": changed_tables,
        "row_deletions": row_deletions,
        "node_recomputations": node_recomputations,
        "node_tightenings": node_tightenings,
        "final_restrictive_node_count": restrictive_nodes,
    }


def serialize_nodes(nodes: list[dict]) -> list[dict]:
    return [
        {
            "bits": list(node["bits"]),
            "rows": sorted(node["rows"]),
            "members": node["members"],
        }
        for node in nodes
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(table_artifact(STAGE_COMMON_FIXED_POINT)))
    parser.add_argument(
        "--output",
        default=str(table_artifact(STAGE_COMMON_FIXED_POINT_NODE_FILTERED)),
    )
    parser.add_argument(
        "--report",
        default=str(report_artifact(STAGE_COMMON_FIXED_POINT_NODE_FILTERED)),
    )
    parser.add_argument("--nodes", default=str(nodes_artifact(STAGE_COMMON_FIXED_POINT_NODE_FILTERED)))
    args = parser.parse_args()

    with Path(args.input).open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    initial_table_count = len(tables)
    initial_bit_count = len(collect_bits(tables))
    initial_row_count = total_rows(tables)
    initial_arity = arity_distribution(tables)

    tables = [
        {"bits": table["bits"], "rows": set(table["rows"])}
        for table in tables
    ]

    nodes, table_to_nodes, node_build_stats = build_nodes(tables)
    filter_stats = filter_tables_with_nodes(tables, nodes, table_to_nodes)

    output_tables = [
        {"bits": table["bits"], "rows": sorted(table["rows"])}
        for table in tables
    ]
    output_nodes = serialize_nodes(nodes)

    report = {
        "method": "build nodes from exact pairwise bitset intersections, intersect projected subtables inside each node, and propagate node constraints until no further row deletions",
        "input": args.input,
        "output": args.output,
        "nodes_output": args.nodes,
        "initial_table_count": initial_table_count,
        "initial_bit_count": initial_bit_count,
        "initial_row_count": initial_row_count,
        "final_table_count": len(output_tables),
        "final_bit_count": len(collect_bits(output_tables)),
        "final_row_count": total_rows(output_tables),
        "initial_arity_distribution": initial_arity,
        "final_arity_distribution": arity_distribution(output_tables),
        "node_build": node_build_stats,
        "filter": filter_stats,
    }

    write_json(Path(args.output), output_tables)
    write_json(Path(args.nodes), output_nodes)
    write_json(Path(args.report), report)

    print(f"nodes: {node_build_stats['node_count']}")
    print(f"restrictive nodes: {node_build_stats['restrictive_node_count']}")
    print(f"changed tables: {filter_stats['changed_tables']}")
    print(f"row deletions: {filter_stats['row_deletions']}")
    print(f"final rows: {total_rows(output_tables)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
