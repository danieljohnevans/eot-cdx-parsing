#!/usr/bin/env python3
"""Load downloaded EOT CDXJ files into a filtered DuckDB database.

Parses .cdxj.gz files, filters to target .gov domains, extracts host/path info.

Usage:
    python load_db.py                       # loads 2012 (default)
    python load_db.py --years 2012 2016
    python load_db.py --years all
"""

import argparse
import glob
import gzip
import json
import sys
import time

import duckdb
from tqdm import tqdm

from config import (
    AVAILABLE_YEARS,
    DATA_DIR,
    DB_PATH,
    PATH_SEGMENT_DEPTH,
    TARGET_DOMAINS,
    cdxj_dir,
)

BATCH_SIZE = 100_000

# Columns we extract from each CDXJ JSON blob
CDXJ_COLUMNS = [
    "url", "mime", "status", "digest", "length", "offset",
    "filename", "mime-detected", "puid", "charset", "languages",
    "surtkey", "timestamp",
]


def build_domain_filter(column: str = "host") -> str:
    """Build a SQL WHERE clause matching any target domain (exact or subdomain)."""
    conditions = []
    for d in TARGET_DOMAINS:
        conditions.append(f"{column} = '{d}'")
        conditions.append(f"ends_with({column}, '.{d}')")
    return " OR ".join(conditions)


def build_path_segment_columns() -> str:
    """Build SQL expressions to extract path segments 1..N from url_path."""
    parts = []
    for i in range(1, PATH_SEGMENT_DEPTH + 1):
        parts.append(
            f"list_extract(string_split(trim(url_path, '/'), '/'), {i}) AS path_seg_{i}"
        )
    return ",\n        ".join(parts)


def parse_cdxj_file(path: str) -> list[dict]:
    """Parse a single .cdxj.gz file into a list of row dicts."""
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                key, ts, json_blob = line.strip().split(" ", 2)
                rec = json.loads(json_blob)
                rec["surtkey"] = key
                rec["timestamp"] = ts
                rows.append({k: rec.get(k) for k in CDXJ_COLUMNS})
            except Exception:
                continue
    return rows


def load_year(con: duckdb.DuckDBPyConnection, year: int, data_dir, table_name: str):
    """Load one crawl year's CDXJ data into the database, filtered."""
    pdir = cdxj_dir(data_dir, year)
    if not pdir.exists():
        print(f"  No CDXJ directory found at {pdir} — run download_eot.py first")
        return

    files = sorted(glob.glob(str(pdir / "*.cdxj.gz")))
    if not files:
        print(f"  No CDXJ files found in {pdir} — run download_eot.py first")
        return

    print(f"  Loading EOT-{year} ({len(files)} files)...")
    t0 = time.time()

    domain_filter = build_domain_filter()
    path_segments = build_path_segment_columns()

    # Check if target table already exists
    existing = con.sql(
        f"SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()[0]

    total_inserted = 0

    # Process files in batches to keep memory bounded
    batch = []
    for filepath in tqdm(files, desc=f"  EOT-{year}", unit="file"):
        batch.extend(parse_cdxj_file(filepath))

        if len(batch) >= BATCH_SIZE:
            total_inserted += _flush_batch(con, batch, table_name, existing, domain_filter, path_segments, year)
            existing = True  # table exists after first flush
            batch.clear()

    # Flush remaining
    if batch:
        total_inserted += _flush_batch(con, batch, table_name, existing, domain_filter, path_segments, year)

    elapsed = time.time() - t0
    print(f"  Loaded {total_inserted:,} matching rows in {elapsed:.1f}s")


def _flush_batch(con, rows, table_name, table_exists, domain_filter, path_segments, year) -> int:
    """Insert a batch of parsed CDXJ rows into the target table, filtered."""
    # Register the Python list as a DuckDB temporary table
    con.execute("DROP VIEW IF EXISTS _cdxj_staging")
    import pyarrow as pa
    arrow_table = pa.Table.from_pylist(rows)
    con.register("_cdxj_staging", arrow_table)

    verb = "INSERT INTO" if table_exists else "CREATE TABLE"
    target = table_name if table_exists else f"{table_name} AS"

    sql = f"""
    {verb} {target}
    SELECT
        url,
        lower(regexp_extract(url, '^[a-z]+://([^/?#:]+)', 1)) AS host,
        regexp_extract(url, '^[a-z]+://[^/]+(/[^?#]*)', 1) AS url_path,
        regexp_extract(url, '\\?(.*)$', 1) AS url_query,

        {path_segments},

        CAST(mime AS VARCHAR) AS mime,
        CAST(status AS INTEGER) AS status,
        CAST(digest AS VARCHAR) AS digest,
        CAST("length" AS BIGINT) AS "length",
        CAST("offset" AS BIGINT) AS "offset",
        CAST(filename AS VARCHAR) AS warc_filename,
        CAST("mime-detected" AS VARCHAR) AS mime_detected,
        CAST(puid AS VARCHAR) AS puid,
        CAST(charset AS VARCHAR) AS charset,
        CAST(languages AS VARCHAR) AS languages,

        CAST(surtkey AS VARCHAR) AS surtkey,
        CAST(timestamp AS VARCHAR) AS fetch_timestamp,
        '{year}' AS crawl_year

    FROM _cdxj_staging
    WHERE {domain_filter}
    """

    before = 0
    if table_exists:
        before = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    con.execute(sql)

    after = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    con.execute("DROP VIEW IF EXISTS _cdxj_staging")
    return after - before


def main():
    parser = argparse.ArgumentParser(description="Load EOT CDXJ files into filtered DuckDB")
    parser.add_argument(
        "--years",
        nargs="+",
        default=["2012"],
        help=f"Crawl years to load. Use 'all' for {AVAILABLE_YEARS}. Default: 2012",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DATA_DIR),
        help=f"Base directory with downloaded CDXJ files. Default: {DATA_DIR}",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DB_PATH),
        help=f"Path for DuckDB database file. Default: {DB_PATH}",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="eot_captures",
        help="Target table name. Default: eot_captures",
    )
    args = parser.parse_args()

    # Resolve years
    if args.years == ["all"]:
        years = AVAILABLE_YEARS
    else:
        years = []
        for y in args.years:
            yr = int(y)
            if yr not in AVAILABLE_YEARS:
                print(f"Error: {yr} not in available years {AVAILABLE_YEARS}", file=sys.stderr)
                sys.exit(1)
            years.append(yr)

    data_dir = __import__("pathlib").Path(args.data_dir)
    db_path = __import__("pathlib").Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    for year in years:
        load_year(con, year, data_dir, args.table)

    # Print summary
    total = con.sql(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
    print(f"\nTotal rows in {args.table}: {total:,}")
    print(f"Database: {db_path}")
    print("\nRows per domain:")
    con.sql(f"""
        SELECT host, COUNT(*) AS captures
        FROM {args.table}
        GROUP BY 1
        ORDER BY 2 DESC
    """).show()

    con.close()


if __name__ == "__main__":
    main()
