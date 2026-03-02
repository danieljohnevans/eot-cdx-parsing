"""Shared configuration for EOT data pipeline."""

from pathlib import Path

# S3 source
S3_BUCKET = "eotarchive"
S3_PREFIX = "eot-index/table/eot-main"

# Available crawl years in the EOT archive
AVAILABLE_YEARS = [2004, 2008, 2012, 2016, 2020]

# Federal .gov domains to analyse
TARGET_DOMAINS = [
    "usda.gov",
    "commerce.gov",
    "defense.gov",
    "ed.gov",
    "energy.gov",
    "hhs.gov",
    "dhs.gov",
    "hud.gov",
    "doi.gov",
    "justice.gov",
    "dol.gov",
    "state.gov",
    "transportation.gov",
    "treasury.gov",
    "va.gov",
]

# How many URL path segments to extract (e.g. /a/b/c/d/e → 5)
PATH_SEGMENT_DEPTH = 5

# Default local paths
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "eot.duckdb"


def parquet_dir(data_dir: Path, year: int) -> Path:
    """Return the local directory for a given crawl year's parquet files."""
    return data_dir / "parquet" / f"EOT-{year}"


def parquet_glob(data_dir: Path, year: int) -> str:
    """Return a glob string DuckDB can use to read all parquets for a year."""
    return str(parquet_dir(data_dir, year) / "*.parquet")
