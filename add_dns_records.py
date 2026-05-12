#!/usr/bin/env python3
"""Append previously-skipped DNS records to an existing CDXJ DuckDB.

DNS records (`mime='text/dns'`, URLs like `dns:hostname`) were dropped during
the original `load_db.py` run because the host-extraction regex required `://`.
This script re-parses the `.cdxj.gz` files and appends only the DNS rows for
the target domains, using the updated regex that also handles `dns:hostname`.

Idempotent: if any `text/dns` rows already exist in the target table, the
script no-ops (override with `--force`).

Usage:
    python add_dns_records.py --years all
    python add_dns_records.py --years 2020
    python add_dns_records.py --years all --db-path data/eot.duckdb
"""

import argparse
import glob
import gzip
import json
import time
from pathlib import Path

import duckdb
from tqdm import tqdm

from config import AVAILABLE_YEARS, DATA_DIR, DB_PATH, cdxj_dir
from load_db import (
    BATCH_SIZE,
    CDXJ_COLUMNS,
    build_domain_filter,
    build_path_segment_columns,
    build_surt_columns,
)


def parse_dns_lines(path: str, error_log: list | None = None) -> list[dict]:
    """Parse a single .cdxj.gz file, keeping only `text/dns` records.

    Two-stage filter for speed:
    1. Cheap substring check on the raw line: skip anything without `"url":"dns:`.
    2. JSON parse + confirm `mime == 'text/dns'`.
    """
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if '"url":"dns:' not in line:
                continue
            try:
                key, ts, json_blob = line.strip().split(" ", 2)
                rec = json.loads(json_blob)
                if rec.get("mime") != "text/dns":
                    continue
                rec["surtkey"] = key
                rec["timestamp"] = ts
                rows.append({k: rec.get(k) for k in CDXJ_COLUMNS})
            except Exception as e:
                if error_log is not None:
                    error_log.append((
                        path, lineno, type(e).__name__, str(e), line.strip()[:200],
                    ))
                continue
    return rows


def _flush_dns_batch(con, rows, table_name, domain_filter, path_segments, surt_columns, year) -> int:
    """Insert a DNS batch into the target table. Mirrors `_flush_batch` in load_db.py."""
    con.execute("DROP VIEW IF EXISTS _cdxj_dns_staging")
    import pyarrow as pa
    arrow_table = pa.Table.from_pylist(rows)
    con.register("_cdxj_dns_staging", arrow_table)

    sql = f"""
    INSERT INTO {table_name}
    SELECT
        url,
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

    FROM _cdxj_dns_staging
    WHERE {domain_filter}
    """

    before = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    con.execute(sql)
    after = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    con.execute("DROP VIEW IF EXISTS _cdxj_dns_staging")
    return after - before


def process_year(con, year: int, data_dir: Path, table_name: str) -> int:
    pdir = cdxj_dir(data_dir, year)
    if not pdir.exists():
        print(f"  No CDXJ directory at {pdir} — skipping")
        return 0

    files = sorted(glob.glob(str(pdir / "*.cdxj.gz")))
    if not files:
        print(f"  No CDXJ files in {pdir} — skipping")
        return 0

    print(f"  Processing EOT-{year} ({len(files)} files)...")
    t0 = time.time()

    domain_filter = build_domain_filter()
    path_segments = build_path_segment_columns()
    surt_columns = build_surt_columns()

    total_inserted = 0
    batch: list[dict] = []
    error_log: list[tuple] = []

    for filepath in tqdm(files, desc=f"  EOT-{year}", unit="file"):
        batch.extend(parse_dns_lines(filepath, error_log=error_log))
        if len(batch) >= BATCH_SIZE:
            total_inserted += _flush_dns_batch(
                con, batch, table_name, domain_filter, path_segments, surt_columns, year,
            )
            batch.clear()

    if batch:
        total_inserted += _flush_dns_batch(
            con, batch, table_name, domain_filter, path_segments, surt_columns, year,
        )

    elapsed = time.time() - t0
    print(f"  Inserted {total_inserted:,} DNS rows in {elapsed:.1f}s "
          f"({len(error_log)} parse errors)")
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Append DNS records to existing CDXJ DuckDB")
    parser.add_argument(
        "--years", nargs="+", default=["all"],
        help=f"Crawl years to scan. Use 'all' for {AVAILABLE_YEARS}. Default: all",
    )
    parser.add_argument("--data-dir", type=str, default=str(DATA_DIR))
    parser.add_argument("--db-path", type=str, default=str(DB_PATH))
    parser.add_argument("--table", type=str, default="eot_captures")
    parser.add_argument(
        "--force", action="store_true",
        help="Run even if DNS rows already exist (otherwise no-ops to avoid duplicates).",
    )
    args = parser.parse_args()

    if args.years == ["all"]:
        years = AVAILABLE_YEARS
    else:
        years = [int(y) for y in args.years]

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    con = duckdb.connect(str(db_path))

    # Idempotency check
    existing = con.sql(
        f"SELECT COUNT(*) FROM {args.table} WHERE mime = 'text/dns'"
    ).fetchone()[0]
    if existing > 0 and not args.force:
        print(f"  {db_path}: already has {existing:,} text/dns rows. Pass --force to add anyway.")
        con.close()
        return

    grand_total = 0
    for year in years:
        grand_total += process_year(con, year, data_dir, args.table)

    final = con.sql(
        f"SELECT COUNT(*) FROM {args.table} WHERE mime = 'text/dns'"
    ).fetchone()[0]
    print(f"\nDone. Inserted {grand_total:,} DNS rows. Table now has {final:,} text/dns rows total.")
    con.close()


if __name__ == "__main__":
    main()
