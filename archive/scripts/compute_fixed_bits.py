import argparse
import json
from collections import deque
from pathlib import Path

from ortools.sat.python import cp_model


def load_tables(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def bit_count(tables: list[dict]) -> int:
    return max(max(table["bits"]) for table in tables) + 1


def local_propagation(tables: list[dict], nbits: int) -> dict[int, int]:
    values = [-1] * nbits
    bit_to_tables = [[] for _ in range(nbits)]

    for index, table in enumerate(tables):
        for bit in table["bits"]:
            bit_to_tables[bit].append(index)

    queue = deque(range(len(tables)))
    queued = bytearray(b"\x01") * len(tables)

    while queue:
        table_index = queue.popleft()
        queued[table_index] = 0

        table = tables[table_index]
        bits = table["bits"]
        remaining = []

        for row in table["rows"]:
            for offset, bit in enumerate(bits):
                value = values[bit]
                if value != -1 and ((row >> offset) & 1) != value:
                    break
            else:
                remaining.append(row)

        if not remaining:
            raise RuntimeError(f"contradiction in table {table_index}")

        for offset, bit in enumerate(bits):
            forced = (remaining[0] >> offset) & 1
            if all(((row >> offset) & 1) == forced for row in remaining[1:]):
                current = values[bit]
                if current == -1:
                    values[bit] = forced
                    for dependent in bit_to_tables[bit]:
                        if not queued[dependent]:
                            queued[dependent] = 1
                            queue.append(dependent)
                elif current != forced:
                    raise RuntimeError(f"conflicting values for bit {bit}")

    return {bit: value for bit, value in enumerate(values) if value != -1}


def cp_sat_tightening(
    tables: list[dict], nbits: int, max_time: float
) -> dict[int, int]:
    model = cp_model.CpModel()
    vars_by_bit = [model.NewBoolVar(f"b{bit}") for bit in range(nbits)]

    for table in tables:
        bits = table["bits"]
        assignments = [
            [(row >> offset) & 1 for offset in range(len(bits))]
            for row in table["rows"]
        ]
        model.AddAllowedAssignments(
            [vars_by_bit[bit] for bit in bits],
            assignments,
        )

    solver = cp_model.CpSolver()
    solver.parameters.stop_after_presolve = True
    solver.parameters.fill_tightened_domains_in_response = True
    solver.parameters.max_time_in_seconds = max_time
    solver.parameters.num_search_workers = 8

    solver.Solve(model)
    fixed = {}

    for variable in solver.ResponseProto().tightened_variables:
        if (
            variable.name.startswith("b")
            and len(variable.domain) == 2
            and variable.domain[0] == variable.domain[1]
        ):
            fixed[int(variable.name[1:])] = variable.domain[0]

    return fixed


def write_output(path: Path, values: dict[int, int], nbits: int) -> None:
    ones = [bit for bit, value in sorted(values.items()) if value == 1]
    zeros = [bit for bit, value in sorted(values.items()) if value == 0]
    payload = {
        "method": "local propagation + CP-SAT presolve tightening",
        "complete_backbone": False,
        "one_count": len(ones),
        "zero_count": len(zeros),
        "fixed_count": len(values),
        "unknown_count": nbits - len(values),
        "ones": ones,
        "zeros": zeros,
    }

    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tables.json")
    parser.add_argument("--output", default="fixed_bits.json")
    parser.add_argument("--max-time", type=float, default=120.0)
    args = parser.parse_args()

    tables = load_tables(Path(args.input))
    nbits = bit_count(tables)

    fixed = local_propagation(tables, nbits)
    tightened = cp_sat_tightening(tables, nbits, args.max_time)

    for bit, value in tightened.items():
        current = fixed.get(bit)
        if current is not None and current != value:
            raise RuntimeError(f"conflicting values for bit {bit}")
        fixed[bit] = value

    write_output(Path(args.output), fixed, nbits)

    ones = sum(1 for value in fixed.values() if value == 1)
    zeros = sum(1 for value in fixed.values() if value == 0)
    print(f"fixed bits: {len(fixed)}")
    print(f"ones: {ones}")
    print(f"zeros: {zeros}")
    print(f"unknown: {nbits - len(fixed)}")
    print(f"output: {args.output}")


if __name__ == "__main__":
    main()
