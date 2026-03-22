"""Shared configuration for EOT data pipeline."""

from pathlib import Path

# S3 source
S3_BUCKET = "eotarchive"
S3_PREFIX = "crawl-data"
PARQUET_S3_PREFIX = "eot-index/table/eot-main"

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
PARQUET_DB_PATH = DATA_DIR / "eot_parquet.duckdb"


def parquet_dir(data_dir: Path, year: int) -> Path:
    """Return the local directory for a given crawl year's parquet files."""
    return data_dir / "parquet" / f"EOT-{year}"


def cdxj_dir(data_dir: Path, year: int) -> Path:
    """Return the local directory for a given crawl year's CDXJ files."""
    return data_dir / "cdxj" / f"EOT-{year}"


def cdxj_glob(data_dir: Path, year: int) -> str:
    """Return a glob string for all CDXJ files for a year."""
    return str(cdxj_dir(data_dir, year) / "*.cdxj.gz")
