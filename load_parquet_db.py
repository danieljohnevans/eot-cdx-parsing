#!/usr/bin/env python3
"""Load downloaded EOT parquet index files into a DuckDB database.

Reads the pre-built parquet files (all domains, not filtered) and ingests
them into a DuckDB table for local analysis.

Usage:
    python load_parquet_db.py                       # loads all years
    python load_parquet_db.py --years 2012 2016
    python load_parquet_db.py --years 2012
"""

import argparse
import sys
import time

import duckdb

from config import AVAILABLE_YEARS, DATA_DIR, PARQUET_DB_PATH, parquet_dir


def load_year(con: duckdb.DuckDBPyConnection, year: int, data_dir, table_name: str):
    """Load one crawl year's parquet files into the database."""
    pdir = parquet_dir(data_dir, year)
    if not pdir.exists():
        print(f"  No parquet directory found at {pdir} — run download_parquet.py first")
        return

    glob_path = str(pdir / "*.parquet")

    # Check file count
    file_count = con.sql(
        f"SELECT COUNT(*) FROM glob('{glob_path}')"
    ).fetchone()[0]
    if file_count == 0:
        print(f"  No parquet files found in {pdir} — run download_parquet.py first")
        return

    print(f"  Loading EOT-{year} ({file_count} parquet files)...")
    t0 = time.time()

    # Check if table already exists
    existing = con.sql(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()[0]

    if existing:
        before = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT *, '{year}' AS crawl_year
            FROM read_parquet('{glob_path}')
        """)
        after = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        inserted = after - before
    else:
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT *, '{year}' AS crawl_year
            FROM read_parquet('{glob_path}')
        """)
        inserted = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    elapsed = time.time() - t0
    print(f"  Loaded {inserted:,} rows in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Load EOT parquet files into DuckDB")
    parser.add_argument(
        "--years",
        nargs="+",
        default=["all"],
        help=f"Crawl years to load. Use 'all' for {AVAILABLE_YEARS}. Default: all",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DATA_DIR),
        help=f"Base directory with downloaded parquet files. Default: {DATA_DIR}",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(PARQUET_DB_PATH),
        help=f"Path for DuckDB database file. Default: {PARQUET_DB_PATH}",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="eot_parquet",
        help="Target table name. Default: eot_parquet",
    )
    args = parser.parse_args()

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
    print("\nRows per crawl year:")
    con.sql(f"""
        SELECT crawl_year, COUNT(*) AS rows
        FROM {args.table}
        GROUP BY 1
        ORDER BY 1
    """).show()

    con.close()


if __name__ == "__main__":
    main()
