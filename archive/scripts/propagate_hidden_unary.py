import argparse
import json
from pathlib import Path

from merge_subset_tables import collapse_equal_bitsets


HIDDEN_UNARY_PATTERNS = {
    (0, 2): (0, 0),
    (1, 3): (0, 1),
    (0, 1): (1, 0),
    (2, 3): (1, 1),
}


def load_tables(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def detect_hidden_unary_sources(tables: list[dict]) -> tuple[dict[int, int], list[dict]]:
    forced: dict[int, int] = {}
    sources: list[dict] = []

    for index, table in enumerate(tables):
        if len(table["bits"]) != 2:
            continue

        pattern = tuple(table["rows"])
        if pattern not in HIDDEN_UNARY_PATTERNS:
            continue

        bit_index, value = HIDDEN_UNARY_PATTERNS[pattern]
        bit = table["bits"][bit_index]
        current = forced.get(bit)
        if current is not None and current != value:
            raise RuntimeError(f"conflicting unary constraints for bit {bit}")

        forced[bit] = value
        sources.append(
            {
                "table_index": index,
                "bits": table["bits"],
                "rows": table["rows"],
                "forced_bit": bit,
                "forced_value": value,
            }
        )

    return forced, sources


def propagate_forced_bits(
    tables: list[dict],
    forced: dict[int, int],
    sources: list[dict],
) -> tuple[list[dict], dict]:
    source_keys = {
        (tuple(source["bits"]), tuple(source["rows"]))
        for source in sources
    }
    projected = []
    removed_source_tables = 0
    removed_tautologies = 0
    affected_tables = 0
    changed_tables = 0
    removed_rows = 0

    for table in tables:
        table_key = (tuple(table["bits"]), tuple(table["rows"]))
        if table_key in source_keys:
            removed_source_tables += 1
            continue

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
            raise RuntimeError(f"contradiction after projecting table {table}")

        if len(new_rows) == (1 << len(kept_bits)):
            removed_tautologies += 1
            continue

        projected.append({"bits": kept_bits, "rows": sorted(new_rows)})

    canonical, duplicate_count = collapse_equal_bitsets(projected)
    output_tables = [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(canonical.items(), key=lambda item: (len(item[0]), item[0]))
    ]

    return output_tables, {
        "removed_source_tables": removed_source_tables,
        "removed_tautologies": removed_tautologies,
        "affected_tables": affected_tables,
        "changed_tables": changed_tables,
        "removed_rows": removed_rows,
        "collapsed_duplicate_tables": duplicate_count,
    }


def write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables_fixed_point.json")
    parser.add_argument("--output", default="tables_fixed_point_unary.json")
    parser.add_argument("--report", default="hidden_unary_report.json")
    parser.add_argument("--sources", default="hidden_unary_sources.json")
    parser.add_argument("--forced", default="hidden_unary_forced_bits.json")
    args = parser.parse_args()

    tables = load_tables(Path(args.input))
    forced, sources = detect_hidden_unary_sources(tables)
    output_tables, stats = propagate_forced_bits(tables, forced, sources)

    report = {
        "method": "extract hidden unary constraints from two-bit tables, propagate constants, remove source tables and resulting tautologies",
        "input": args.input,
        "output": args.output,
        "input_table_count": len(tables),
        "output_table_count": len(output_tables),
        "forced_bits": [
            {"bit": bit, "value": value}
            for bit, value in sorted(forced.items())
        ],
        "source_table_count": len(sources),
        **stats,
    }

    write_json(Path(args.output), output_tables)
    write_json(Path(args.report), report)
    write_json(Path(args.sources), sources)
    write_json(
        Path(args.forced),
        [{"bit": bit, "value": value} for bit, value in sorted(forced.items())],
    )

    print(f"forced bits: {len(forced)}")
    print(f"source tables removed: {stats['removed_source_tables']}")
    print(f"tautologies removed: {stats['removed_tautologies']}")
    print(f"output tables: {len(output_tables)}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
