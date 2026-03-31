from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.artifacts import (
    STAGE_COMMON_NODE_FIXED_POINT,
    rank_report_artifact,
    ranks_artifact,
    table_artifact,
)
from src.utils.rank_stats import compute_rank, summarize_table_ranks


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(table_artifact(STAGE_COMMON_NODE_FIXED_POINT)))
    parser.add_argument("--output", default=str(ranks_artifact(STAGE_COMMON_NODE_FIXED_POINT)))
    parser.add_argument(
        "--report",
        default=str(rank_report_artifact(STAGE_COMMON_NODE_FIXED_POINT)),
    )
    parser.add_argument("--top-signatures", type=int, default=25)
    args = parser.parse_args()

    input_path = Path(args.input)
    with input_path.open("r", encoding="utf-8") as fh:
        tables = json.load(fh)

    entries = []
    for table_index, table in enumerate(tables):
        bit_count = len(table["bits"])
        row_count = len(table["rows"])
        if bit_count <= 0:
            raise RuntimeError(f"table {table_index} has non-positive bit count")
        if row_count <= 0:
            raise RuntimeError(f"table {table_index} has non-positive row count")

        rank = compute_rank(row_count, bit_count)
        entry = {
            "table_index": table_index,
            "bit_count": bit_count,
            "row_count": row_count,
            "rank": rank,
        }
        entries.append(entry)
    rank_summary = summarize_table_ranks(tables, topn=args.top_signatures)

    report = {
        "input": str(input_path),
        "output": args.output,
        **rank_summary,
    }

    write_json(Path(args.output), entries)
    write_json(Path(args.report), report)

    print(f"tables: {len(entries)}")
    print(f"min rank: {rank_summary['min_rank']:.15f}")
    print(f"max rank: {rank_summary['max_rank']:.15f}")
    print(f"mean rank: {rank_summary['mean_rank']:.15f}")
    print(f"median rank: {rank_summary['median_rank']:.15f}")
    print(f"output: {args.output}")
    print(f"report: {args.report}")


if __name__ == "__main__":
    main()
