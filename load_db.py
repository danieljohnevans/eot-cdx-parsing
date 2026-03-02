#!/usr/bin/env python3
"""Load downloaded EOT parquet files into a filtered DuckDB database.

Filters to target .gov domains, extracts subdomains and URL path segments.

Usage:
    python load_db.py                       # loads 2012 (default)
    python load_db.py --years 2012 2016
    python load_db.py --years all
"""

import argparse
import sys
import time

import duckdb

from config import (
    AVAILABLE_YEARS,
    DATA_DIR,
    DB_PATH,
    PATH_SEGMENT_DEPTH,
    TARGET_DOMAINS,
    parquet_dir,
    parquet_glob,
)


def build_domain_filter(column: str = "url_host_registered_domain") -> str:
    """Build a SQL WHERE clause matching any target domain."""
    conditions = [f"{column} = '{d}'" for d in TARGET_DOMAINS]
    return " OR ".join(conditions)


def build_path_segment_columns() -> str:
    """Build SQL expressions to extract path segments 1..N from url_path."""
    parts = []
    for i in range(1, PATH_SEGMENT_DEPTH + 1):
        parts.append(
            f"list_extract(string_split(trim(url_path, '/'), '/'), {i}) AS path_seg_{i}"
        )
    return ",\n        ".join(parts)


def load_year(con: duckdb.DuckDBPyConnection, year: int, data_dir, table_name: str):
    """Load one crawl year's parquet data into the database, filtered."""
    pdir = parquet_dir(data_dir, year)
    if not pdir.exists() or not any(pdir.glob("*.parquet")):
        print(f"  No parquet files found at {pdir} — run download_eot.py first")
        return

    glob_path = parquet_glob(data_dir, year)
    domain_filter = build_domain_filter()
    path_segments = build_path_segment_columns()

    # Check if table already exists
    existing = con.sql(
        f"SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()[0]

    verb = "INSERT INTO" if existing else "CREATE TABLE"
    target = table_name if existing else f"{table_name} AS"

    sql = f"""
    {verb} {target}
    SELECT
        url,
        url_host_name,
        url_host_registered_domain,
        url_protocol,
        url_path,
        url_query,

        -- derived: subdomain (everything before the registered domain)
        CASE
            WHEN url_host_name = url_host_registered_domain THEN NULL
            WHEN ends_with(url_host_name, '.' || url_host_registered_domain)
                THEN left(url_host_name, length(url_host_name) - length(url_host_registered_domain) - 1)
            ELSE NULL
        END AS subdomain,

        -- derived: path segments
        {path_segments},

        fetch_time,
        fetch_status,
        content_mime_type,
        content_mime_detected,

        -- crawl metadata
        '{year}' AS crawl_year

    FROM read_parquet('{glob_path}')
    WHERE {domain_filter}
    """

    print(f"  Loading EOT-{year}...")
    t0 = time.time()
    con.execute(sql)
    elapsed = time.time() - t0

    count = con.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE crawl_year = '{year}'"
    ).fetchone()[0]
    print(f"  Loaded {count:,} rows in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Load EOT parquets into filtered DuckDB")
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
        help=f"Base directory with downloaded parquets. Default: {DATA_DIR}",
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
        SELECT url_host_registered_domain AS domain, COUNT(*) AS captures
        FROM {args.table}
        GROUP BY 1
        ORDER BY 2 DESC
    """).show()

    con.close()


if __name__ == "__main__":
    main()
