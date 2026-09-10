"""Shared configuration for EOT data pipeline."""

from pathlib import Path

# S3 source
S3_BUCKET = "eotarchive"
S3_PREFIX = "crawl-data"
PARQUET_S3_PREFIX = "eot-index/table/eot-main"

AVAILABLE_YEARS = [2004, 2008, 2012, 2016, 2020, 2024]

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


# Map full domain -> folder basename used under data/domains/ (e.g. 'doi.gov' -> 'doi')
_DOMAIN_BASENAME = {d: d.split(".gov")[0] for d in TARGET_DOMAINS}


def discover_domain_dbs(kind: str = "cdxj", search_roots=None) -> dict:
    """Locate each target domain's per-domain DuckDB, across local & server layouts.

    Handles both the server layout (``data/domains/NN_name/{kind}.duckdb``) and the
    local layout (``data/NN_name/{kind}.duckdb``), whether the notebook is run from
    the repo root or a subdirectory. Returns an *ordered* dict {domain: Path} for the
    domains actually found, in TARGET_DOMAINS order. Missing domains are simply absent.

    kind: 'cdxj' or 'parquet'.
    """
    import glob
    import re

    roots = search_roots or [".", "..", DATA_DIR, Path("..") / DATA_DIR]
    basename_to_domain = {b: d for d, b in _DOMAIN_BASENAME.items()}
    patterns = []
    for r in roots:
        r = Path(r)
        patterns += [
            str(r / "data" / "domains" / "*" / f"{kind}.duckdb"),
            str(r / "data" / "*" / f"{kind}.duckdb"),
            str(r / "domains" / "*" / f"{kind}.duckdb"),
            str(r / "*" / f"{kind}.duckdb"),
        ]
    found = {}
    for pat in patterns:
        for p in glob.glob(pat):
            folder = Path(p).parent.name          # e.g. '04_doi'
            base = re.sub(r"^\d+_", "", folder)    # e.g. 'doi'
            dom = basename_to_domain.get(base)
            if dom and dom not in found:
                found[dom] = Path(p)
    # return in canonical TARGET_DOMAINS order
    return {d: found[d] for d in TARGET_DOMAINS if d in found}
