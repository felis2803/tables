from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from typing import Iterable


def compute_rank(row_count: int, bit_count: int) -> float:
    if bit_count <= 0:
        raise ValueError(f"bit_count must be positive, got {bit_count}")
    if row_count <= 0:
        raise ValueError(f"row_count must be positive, got {row_count}")
    return row_count ** (1.0 / bit_count)


def summarize_rank_pairs(
    pairs: Iterable[tuple[int, int]],
    topn: int = 10,
) -> dict:
    grouped: dict[int, list[float]] = defaultdict(list)
    signature_counts: Counter = Counter()
    ranks: list[float] = []

    for bit_count, row_count in pairs:
        rank = compute_rank(row_count, bit_count)
        grouped[bit_count].append(rank)
        signature_counts[(bit_count, row_count)] += 1
        ranks.append(rank)

    if not ranks:
        return {
            "metric": "rank = row_count ** (1 / bit_count)",
            "table_count": 0,
            "min_rank": 0.0,
            "max_rank": 0.0,
            "mean_rank": 0.0,
            "median_rank": 0.0,
            "unique_signatures": 0,
            "by_arity": [],
            "top_signatures": [],
        }

    by_arity = [
        {
            "bit_count": bit_count,
            "table_count": len(arity_ranks),
            "min_rank": min(arity_ranks),
            "max_rank": max(arity_ranks),
            "mean_rank": statistics.fmean(arity_ranks),
            "median_rank": statistics.median(arity_ranks),
        }
        for bit_count, arity_ranks in sorted(grouped.items())
    ]

    top_signatures = [
        {
            "bit_count": bit_count,
            "row_count": row_count,
            "rank": compute_rank(row_count, bit_count),
            "table_count": table_count,
        }
        for (bit_count, row_count), table_count in signature_counts.most_common(topn)
    ]

    return {
        "metric": "rank = row_count ** (1 / bit_count)",
        "table_count": len(ranks),
        "min_rank": min(ranks),
        "max_rank": max(ranks),
        "mean_rank": statistics.fmean(ranks),
        "median_rank": statistics.median(ranks),
        "unique_signatures": len(signature_counts),
        "by_arity": by_arity,
        "top_signatures": top_signatures,
    }


def summarize_table_ranks(tables: list[dict], topn: int = 10) -> dict:
    return summarize_rank_pairs(
        ((len(table["bits"]), len(table["rows"])) for table in tables),
        topn=topn,
    )


def summarize_tables_by_bits_ranks(
    tables_by_bits: dict[tuple[int, ...], set[int]],
    topn: int = 10,
) -> dict:
    return summarize_rank_pairs(
        ((len(bits), len(rows)) for bits, rows in tables_by_bits.items()),
        topn=topn,
    )
