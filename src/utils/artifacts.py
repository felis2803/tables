from __future__ import annotations

from pathlib import Path

from src.utils.paths import DERIVED_DIR, REPORTS_DIR


STAGE_SUBSET_MERGED = "subset_merged"
STAGE_SUBSET_PRUNED = "subset_pruned"
STAGE_SUBSET_PRUNED_FORCED = "subset_pruned.forced"
STAGE_SUBSET_PRUNED_FORCED_PAIR_REDUCED = "subset_pruned.forced.pair_reduced"
STAGE_COMMON_FIXED_POINT = "common_fixed_point"
STAGE_COMMON_FIXED_POINT_NODE_FILTERED = "common_fixed_point.node_filtered"
STAGE_COMMON_NODE_FIXED_POINT = "common_node_fixed_point"


def table_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"tables.{stage}.json"


def report_artifact(stage: str) -> Path:
    return REPORTS_DIR / f"report.{stage}.json"


def forced_bits_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"bits.{stage}.forced.json"


def rewrite_map_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"bits.{stage}.rewrite_map.json"


def components_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"bits.{stage}.components.json"


def nodes_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"nodes.{stage}.json"


def pair_relations_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"pairs.{stage}.relations.json"


def subset_pairs_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"pairs.{stage}.subset_superset.json"


def dropped_tables_artifact(stage: str) -> Path:
    return DERIVED_DIR / f"tables.{stage}.dropped_included.json"
