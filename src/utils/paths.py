from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = PROJECT_ROOT / "docs"
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DERIVED_DIR = DATA_DIR / "derived"
REPORTS_DIR = DATA_DIR / "reports"
RUNS_DIR = PROJECT_ROOT / "runs"
ARCHIVE_DIR = PROJECT_ROOT / "archive"

