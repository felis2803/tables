from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.artifacts import (
    STAGE_SUBSET_PRUNED_FORCED,
    STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED,
    components_artifact,
    pair_relations_artifact,
    report_artifact,
    rewrite_map_artifact,
    table_artifact,
)


EQUAL_MASKS = {1, 8, 9}
OPPOSITE_MASKS = {2, 4, 6}


class ParityUnionFind:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}
        self.rank: dict[int, int] = {}
        self.parity: dict[int, int] = {}

    def add(self, bit: int) -> None:
        if bit not in self.parent:
            self.parent[bit] = bit
            self.rank[bit] = 0
            self.parity[bit] = 0

    def find(self, bit: int) -> tuple[int, int]:
        self.add(bit)
        parent = self.parent[bit]
        if parent != bit:
            root, parent_parity = self.find(parent)
            self.parity[bit] ^= parent_parity
            self.parent[bit] = root
        return self.parent[bit], self.parity[bit]

    def union(self, left: int, right: int, relation: int) -> bool:
        left_root, left_parity = self.find(left)
        right_root, right_parity = self.find(right)

        if left_root == right_root:
            return (left_parity ^ right_parity) == relation

        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
            left_parity, right_parity = right_parity, left_parity

        self.parent[right_root] = left_root
        self.parity[right_root] = left_parity ^ right_parity ^ relation

        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1

        return True


def load_tables(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def extract_relations(tables: list[dict]) -> list[dict]:
    relation_map: dict[tuple[int, int], dict] = {}

    for table in tables:
        bits = table["bits"]
        rows = table["rows"]
        bit_count = len(bits)

        for left_index in range(bit_count):
            for right_index in range(left_index + 1, bit_count):
                mask = 0
                for row in rows:
                    pair_state = ((row >> left_index) & 1) | (((row >> right_index) & 1) << 1)
                    mask |= 1 << pair_state

                if mask in EQUAL_MASKS:
                    relation = 0
                elif mask in OPPOSITE_MASKS:
                    relation = 1
                else:
                    continue

                left_bit = bits[left_index]
                right_bit = bits[right_index]
                key = (left_bit, right_bit)

                if key in relation_map:
                    if relation_map[key]["relation"] != relation:
                        raise RuntimeError(
                            f"conflicting direct relations for pair {(left_bit, right_bit)}"
                        )
                    relation_map[key]["support"] += 1
                    relation_map[key]["sources"].add(bit_count)
                else:
                    relation_map[key] = {
                        "left": left_bit,
                        "right": right_bit,
                        "relation": relation,
                        "mask": mask,
                        "support": 1,
                        "sources": {bit_count},
                    }

    relations = list(relation_map.values())
    relations.sort(key=lambda item: (item["left"], item["right"], item["relation"]))
    return relations


def build_rewrite_map(relations: list[dict]) -> tuple[dict[int, tuple[int, int]], dict]:
    union_find = ParityUnionFind()

    for relation in relations:
        if not union_find.union(
            relation["left"],
            relation["right"],
            relation["relation"],
        ):
            raise RuntimeError(
                "conflicting transitive relations for pair "
                f"{(relation['left'], relation['right'])}"
            )

    components: dict[int, list[int]] = defaultdict(list)
    for bit in union_find.parent:
        root, _ = union_find.find(bit)
        components[root].append(bit)

    rewrite_map: dict[int, tuple[int, int]] = {}
    component_rows = []

    for bits in components.values():
        representative = min(bits)
        _, representative_parity = union_find.find(representative)

        members = []
        for bit in sorted(bits):
            _, bit_parity = union_find.find(bit)
            relation_to_representative = bit_parity ^ representative_parity
            rewrite_map[bit] = (representative, relation_to_representative)
            members.append(
                {
                    "bit": bit,
                    "representative": representative,
                    "inverted": bool(relation_to_representative),
                }
            )

        component_rows.append(
            {
                "representative": representative,
                "size": len(bits),
                "members": members,
            }
        )

    component_rows.sort(key=lambda item: (item["size"], item["representative"]))
    return rewrite_map, {
        "bits_involved": len(union_find.parent),
        "component_count": len(component_rows),
        "replaced_bit_count": len(union_find.parent) - len(component_rows),
        "components": component_rows,
    }


def rewrite_tables(
    tables: list[dict],
    rewrite_map: dict[int, tuple[int, int]],
) -> tuple[list[dict], dict]:
    merged: dict[tuple[int, ...], set[int]] = {}
    changed_tables = 0
    reduced_arity_tables = 0
    same_arity_changed_tables = 0
    removed_rows = 0

    for table in tables:
        old_bits = table["bits"]
        old_rows = table["rows"]
        new_bits = sorted({rewrite_map.get(bit, (bit, 0))[0] for bit in old_bits})
        new_index = {bit: index for index, bit in enumerate(new_bits)}
        new_rows: set[int] = set()

        for row in old_rows:
            assignments: dict[int, int] = {}
            for offset, bit in enumerate(old_bits):
                representative, inverted = rewrite_map.get(bit, (bit, 0))
                value = ((row >> offset) & 1) ^ inverted
                previous = assignments.get(representative)
                if previous is None:
                    assignments[representative] = value
                elif previous != value:
                    break
            else:
                new_row = 0
                for bit, value in assignments.items():
                    if value:
                        new_row |= 1 << new_index[bit]
                new_rows.add(new_row)

        if new_bits != old_bits or new_rows != set(old_rows):
            changed_tables += 1
            if len(new_bits) < len(old_bits):
                reduced_arity_tables += 1
            else:
                same_arity_changed_tables += 1

        removed_rows += len(old_rows) - len(new_rows)

        key = tuple(new_bits)
        if key in merged:
            merged[key].intersection_update(new_rows)
        else:
            merged[key] = set(new_rows)

    output_tables = [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(merged.items(), key=lambda item: (len(item[0]), item[0]))
    ]

    return output_tables, {
        "changed_tables": changed_tables,
        "reduced_arity_tables": reduced_arity_tables,
        "same_arity_changed_tables": same_arity_changed_tables,
        "removed_rows": removed_rows,
        "collapsed_duplicate_tables": len(tables) - len(output_tables),
    }


def update_original_mapping(
    mapping: dict[int, tuple[int, int]],
    rewrite_map: dict[int, tuple[int, int]],
) -> dict[int, tuple[int, int]]:
    updated = {}
    for bit, (current, inverted) in mapping.items():
        if current in rewrite_map:
            representative, current_inverted = rewrite_map[current]
            updated[bit] = (representative, inverted ^ current_inverted)
        else:
            updated[bit] = (current, inverted)
    return updated


def summarize_iteration(
    iteration: int,
    tables_before: list[dict],
    relations: list[dict],
    component_stats: dict,
    rewrite_stats: dict | None,
    tables_after: list[dict] | None,
) -> dict:
    summary = {
        "iteration": iteration,
        "input_table_count": len(tables_before),
        "input_arity_distribution": dict(sorted(Counter(len(table["bits"]) for table in tables_before).items())),
        "relation_pair_count": len(relations),
        "bits_involved": component_stats.get("bits_involved", 0),
        "component_count": component_stats.get("component_count", 0),
        "replaced_bit_count": component_stats.get("replaced_bit_count", 0),
    }

    if rewrite_stats is not None and tables_after is not None:
        summary.update(rewrite_stats)
        summary["output_table_count"] = len(tables_after)
        summary["output_arity_distribution"] = dict(
            sorted(Counter(len(table["bits"]) for table in tables_after).items())
        )

    return summary


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(table_artifact(STAGE_SUBSET_PRUNED_FORCED)))
    parser.add_argument(
        "--output",
        default=str(table_artifact(STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED)),
    )
    parser.add_argument("--report", default=str(report_artifact(STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED)))
    parser.add_argument(
        "--relations",
        default=str(pair_relations_artifact(STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED)),
    )
    parser.add_argument(
        "--mapping",
        default=str(rewrite_map_artifact(STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED)),
    )
    parser.add_argument(
        "--components",
        default=str(components_artifact(STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED)),
    )
    args = parser.parse_args()

    tables = load_tables(Path(args.input))
    original_bits = sorted({bit for table in tables for bit in table["bits"]})
    original_mapping = {bit: (bit, 0) for bit in original_bits}
    all_relations = []
    iterations = []
    total_bits_replaced = 0
    iteration = 1

    while True:
        tables_before = tables
        relations = extract_relations(tables)
        rewrite_map, component_stats = build_rewrite_map(relations) if relations else ({}, {
            "bits_involved": 0,
            "component_count": 0,
            "replaced_bit_count": 0,
            "components": [],
        })

        iterations.append(
            summarize_iteration(
                iteration=iteration,
                tables_before=tables,
                relations=relations,
                component_stats=component_stats,
                rewrite_stats=None,
                tables_after=None,
            )
        )

        if not relations:
            break

        all_relations.extend(
            {
                "iteration": iteration,
                "left": relation["left"],
                "right": relation["right"],
                "equal": relation["relation"] == 0,
                "inverted": relation["relation"] == 1,
                "support": relation["support"],
                "source_arities": sorted(relation["sources"]),
            }
            for relation in relations
        )

        original_mapping = update_original_mapping(original_mapping, rewrite_map)
        tables, rewrite_stats = rewrite_tables(tables, rewrite_map)
        total_bits_replaced += component_stats["replaced_bit_count"]

        iterations[-1] = summarize_iteration(
            iteration=iteration,
            tables_before=tables_before,
            relations=relations,
            component_stats=component_stats,
            rewrite_stats=rewrite_stats,
            tables_after=tables,
        )

        iteration += 1

    final_component_map: dict[int, list[dict]] = defaultdict(list)
    for bit, (representative, inverted) in sorted(original_mapping.items()):
        final_component_map[representative].append(
            {
                "bit": bit,
                "representative": representative,
                "inverted": bool(inverted),
            }
        )

    final_components = [
        {
            "representative": representative,
            "size": len(members),
            "members": members,
        }
        for representative, members in sorted(final_component_map.items())
        if len(members) > 1
    ]

    mapping_rows = [
        {
            "bit": bit,
            "representative": representative,
            "inverted": bool(inverted),
        }
        for bit, (representative, inverted) in sorted(original_mapping.items())
        if bit != representative or inverted
    ]

    report = {
        "method": "iterative extraction of equal/opposite bit pairs from pair projections, parity union-find rewriting, and table canonicalization",
        "input": args.input,
        "output": args.output,
        "iteration_count": len(iterations),
        "iterations": iterations,
        "final_table_count": len(tables),
        "final_arity_distribution": dict(sorted(Counter(len(table["bits"]) for table in tables).items())),
        "rewritten_original_bits": len(mapping_rows),
        "final_components_with_rewrites": len(final_components),
        "total_relation_pairs_found": len(all_relations),
        "total_bits_replaced_across_iterations": total_bits_replaced,
    }

    write_json(Path(args.output), tables)
    write_json(Path(args.report), report)
    write_json(Path(args.relations), all_relations)
    write_json(Path(args.mapping), mapping_rows)
    write_json(Path(args.components), final_components)

    print(f"iterations: {len(iterations)}")
    print(f"final tables: {len(tables)}")
    print(f"rewritten original bits: {len(mapping_rows)}")
    print(f"relations found: {len(all_relations)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
