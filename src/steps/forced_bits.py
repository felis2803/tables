from __future__ import annotations

from src.steps.subset_absorption import collapse_equal_bitsets


def tables_from_dict(tables_by_bits: dict[tuple[int, ...], set[int]]) -> list[dict]:
    return [
        {"bits": list(bits), "rows": sorted(rows)}
        for bits, rows in sorted(tables_by_bits.items(), key=lambda item: (len(item[0]), item[0]))
    ]


def collect_forced_bits_bitwise(tables: list[dict]) -> tuple[dict[int, int], int]:
    forced: dict[int, int] = {}
    occurrences = 0

    for table in tables:
        bits = table["bits"]
        rows = table["rows"]
        full_mask = (1 << len(bits)) - 1
        and_mask = full_mask
        or_mask = 0

        for row in rows:
            and_mask &= row
            or_mask |= row

        zero_mask = full_mask & ~or_mask
        for offset, bit in enumerate(bits):
            mask = 1 << offset
            if and_mask & mask:
                value = 1
            elif zero_mask & mask:
                value = 0
            else:
                continue

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
