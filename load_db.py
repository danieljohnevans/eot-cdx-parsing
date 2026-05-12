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

# Surtkey-derived column depths (mirror parquet's url_host_*_last_part 0..5)
SURTHOST_DEPTH = 6   # surthost_seg_0..5
SURTPATH_DEPTH = 5   # surtpath_1..5


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


# Surtkey is `host_part)path_part?query`, e.g. `gov,doi,data)/api/views/foo?fmt=json`
# SURT canonicalizes the host AND path to lowercase, so surtpath_* ≠ path_seg_*
# whenever the original URL had uppercase letters.
SURTHOST_EXPR = "regexp_extract(surtkey, '^([^)]+)\\)', 1)"
# Path-portion of surtkey, query string excluded (parallels url_path behavior).
SURTPATH_EXPR = "regexp_extract(surtkey, '\\)([^?]*)', 1)"


def build_surt_columns() -> str:
    """SQL for surtkey-derived columns: surthost, surthost_seg_0..5, surtpath_1..5.

    SURT canonicalization strips a leading 'www' label, so www.foo.gov rows
    yield the same surthost as bare foo.gov.
    """
    parts = [f"{SURTHOST_EXPR} AS surthost"]
    for i in range(SURTHOST_DEPTH):
        parts.append(
            f"list_extract(string_split({SURTHOST_EXPR}, ','), {i + 1}) AS surthost_seg_{i}"
        )
    for i in range(1, SURTPATH_DEPTH + 1):
        parts.append(
            f"list_extract(string_split(trim({SURTPATH_EXPR}, '/'), '/'), {i}) AS surtpath_{i}"
        )
    return ",\n        ".join(parts)


def parse_cdxj_file(path: str, error_log=None) -> list[dict]:
    """Parse a single .cdxj.gz file into a list of row dicts.

    If error_log is a list, failed lines are appended as
    (filepath, line_number, error_type, error_message, raw_line_preview).
    """
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            try:
                key, ts, json_blob = line.strip().split(" ", 2)
                rec = json.loads(json_blob)
                rec["surtkey"] = key
                rec["timestamp"] = ts
                rows.append({k: rec.get(k) for k in CDXJ_COLUMNS})
            except Exception as e:
                if error_log is not None:
                    error_log.append((
                        path,
                        lineno,
                        type(e).__name__,
                        str(e),
                        line.strip()[:200],
                    ))
                continue
    return rows


def scan_year_errors(year: int, data_dir, error_log: list):
    """Parse one crawl year's CDXJ files and collect errors without writing to a DB."""
    pdir = cdxj_dir(data_dir, year)
    if not pdir.exists():
        print(f"  No CDXJ directory found at {pdir} — run download_eot.py first")
        return

    files = sorted(glob.glob(str(pdir / "*.cdxj.gz")))
    if not files:
        print(f"  No CDXJ files found in {pdir} — run download_eot.py first")
        return

    print(f"  Scanning EOT-{year} ({len(files)} files)...")
    t0 = time.time()
    errors_before = len(error_log)

    for filepath in tqdm(files, desc=f"  EOT-{year}", unit="file"):
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                try:
                    key, ts, json_blob = line.strip().split(" ", 2)
                    json.loads(json_blob)
                except Exception as e:
                    error_log.append((
                        filepath,
                        lineno,
                        type(e).__name__,
                        str(e),
                        line.strip()[:200],
                    ))

    elapsed = time.time() - t0
    print(f"  Scanned in {elapsed:.1f}s, {len(error_log) - errors_before:,} errors")


def load_year(con: duckdb.DuckDBPyConnection, year: int, data_dir, table_name: str,
              error_log: list | None = None):
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
    surt_columns = build_surt_columns()

    # Check if target table already exists
    existing = con.sql(
        f"SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()[0]

    total_inserted = 0

    # Process files in batches to keep memory bounded
    batch = []
    for filepath in tqdm(files, desc=f"  EOT-{year}", unit="file"):
        batch.extend(parse_cdxj_file(filepath, error_log=error_log))

        if len(batch) >= BATCH_SIZE:
            total_inserted += _flush_batch(con, batch, table_name, existing, domain_filter, path_segments, surt_columns, year)
            existing = True  # table exists after first flush
            batch.clear()

    # Flush remaining
    if batch:
        total_inserted += _flush_batch(con, batch, table_name, existing, domain_filter, path_segments, surt_columns, year)

    elapsed = time.time() - t0
    print(f"  Loaded {total_inserted:,} matching rows in {elapsed:.1f}s")


def _flush_batch(con, rows, table_name, table_exists, domain_filter, path_segments, surt_columns, year) -> int:
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
        -- host regex: '://' is now optional, so dns:hostname (and other non-authority
        -- schemes) get their hostname captured. Web URLs unaffected.
        lower(regexp_extract(url, '^[a-z]+:(?://)?([^/?#:]+)', 1)) AS host,
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
        '{year}' AS crawl_year,

        {surt_columns}

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
    parser.add_argument(
        "--errors-only",
        action="store_true",
        help="Scan CDXJ files for parse errors without writing to a DB. Only outputs cdxj_parse_errors.csv.",
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

    error_log: list[tuple] = []

    if args.errors_only:
        print("Errors-only mode: scanning CDXJ files without writing to a DB.\n")
        for year in years:
            scan_year_errors(year, data_dir, error_log)
        con = None
    else:
        db_path = __import__("pathlib").Path(args.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))

        for year in years:
            load_year(con, year, data_dir, args.table, error_log=error_log)

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

    # Write parse errors to CSV
    if error_log:
        import csv
        error_path = data_dir / "cdxj_parse_errors.csv"
        with open(error_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["file", "line", "error_type", "error_message", "raw_line_preview"])
            w.writerows(error_log)
        print(f"\n{len(error_log):,} parse errors logged to {error_path}")

        # Quick breakdown by error type
        from collections import Counter
        by_type = Counter(e[2] for e in error_log)
        print("Errors by type:")
        for etype, count in by_type.most_common():
            print(f"  {etype}: {count:,}")
    else:
        print("\nNo parse errors.")

    if con is not None:
        con.close()


if __name__ == "__main__":
    main()
