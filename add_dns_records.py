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

from config import AVAILABLE_YEARS, DATA_DIR, DB_PATH, TARGET_DOMAINS, cdxj_dir
from load_db import (
    BATCH_SIZE,
    CDXJ_COLUMNS,
    build_domain_filter,
    build_path_segment_columns,
    build_surt_columns,
    surtkey_prefix,
)
from split_domains import domain_folder_name


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


def parse_dns_into_db(db_path: Path, years: list[int], data_dir: Path,
                      table: str, force: bool) -> bool:
    """Phase 1: re-parse .cdxj.gz files, insert text/dns rows into one DB.

    Returns True if work was done, False if skipped (already populated).
    """
    if not db_path.exists():
        print(f"  SKIP — DB not found: {db_path}")
        return False

    con = duckdb.connect(str(db_path))
    existing = con.sql(
        f"SELECT COUNT(*) FROM {table} WHERE mime = 'text/dns'"
    ).fetchone()[0]
    if existing > 0 and not force:
        print(f"  {db_path}: already has {existing:,} text/dns rows. Pass --force to add anyway.")
        con.close()
        return False

    grand_total = 0
    for year in years:
        grand_total += process_year(con, year, data_dir, table)

    final = con.sql(
        f"SELECT COUNT(*) FROM {table} WHERE mime = 'text/dns'"
    ).fetchone()[0]
    print(f"  {db_path}: inserted {grand_total:,} DNS rows. Total text/dns: {final:,}")
    con.close()
    return True


def propagate_dns_to_domain_db(
    domain_db: Path,
    source_db: Path,
    domain: str,
    table: str,
    force: bool,
) -> bool:
    """Phase 2: copy DNS rows for one domain from `source_db` into `domain_db`.

    No file parsing — pure SQL ATTACH + INSERT. Fast (~seconds per DB).
    """
    if not domain_db.exists():
        print(f"  SKIP — per-domain DB not found: {domain_db}")
        return False

    con = duckdb.connect(str(domain_db))

    try:
        dst_cols = [row[0] for row in con.sql(f"DESCRIBE {table}").fetchall()]
        con.execute(f"ATTACH '{source_db}' AS src (READ_ONLY)")
        src_cols = [row[0] for row in con.sql(f"DESCRIBE src.{table}").fetchall()]
    except duckdb.Error as e:
        print(f"  ERROR connecting/describing — {e}")
        con.close()
        return False

    if dst_cols != src_cols:
        missing = set(src_cols) - set(dst_cols)
        extra = set(dst_cols) - set(src_cols)
        print(f"  SCHEMA MISMATCH for {domain_db}:")
        if missing:
            print(f"    columns in source but not destination: {sorted(missing)}")
        if extra:
            print(f"    columns in destination but not source: {sorted(extra)}")
        print(f"    Run `add_surtkey_columns.py` on both DBs first, then retry.")
        con.execute("DETACH src")
        con.close()
        return False

    existing = con.sql(
        f"SELECT COUNT(*) FROM {table} WHERE mime = 'text/dns'"
    ).fetchone()[0]
    if existing > 0 and not force:
        print(f"  SKIP {domain_db}: already has {existing:,} text/dns rows (use --force)")
        con.execute("DETACH src")
        con.close()
        return False

    prefix = surtkey_prefix(domain)
    domain_pred = (
        f"(surtkey LIKE '{prefix})%' OR surtkey LIKE '{prefix}:%' "
        f"OR surtkey LIKE '{prefix},%')"
    )
    t0 = time.time()
    before = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(f"""
        INSERT INTO {table}
        SELECT * FROM src.{table}
        WHERE mime = 'text/dns' AND {domain_pred}
    """)
    after = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    inserted = after - before
    elapsed = time.time() - t0
    print(f"  {domain_db}: copied {inserted:,} {domain} DNS rows in {elapsed:.1f}s")
    con.execute("DETACH src")
    con.close()
    return True


def main():
    parser = argparse.ArgumentParser(description="Append DNS records to existing CDXJ DuckDB(s)")
    parser.add_argument(
        "--years", nargs="+", default=["all"],
        help=f"Crawl years to scan. Use 'all' for {AVAILABLE_YEARS}. Default: all",
    )
    parser.add_argument("--data-dir", type=str, default=str(DATA_DIR))
    parser.add_argument("--db-path", type=str, default=str(DB_PATH),
                        help="Full DB to parse DNS into (Phase 1). Default: data/eot.duckdb")
    parser.add_argument("--table", type=str, default="eot_captures")
    parser.add_argument(
        "--force", action="store_true",
        help="Run even if DNS rows already exist (otherwise no-ops to avoid duplicates).",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="After populating --db-path, also propagate to every data/<NN_name>/cdxj.duckdb.",
    )
    parser.add_argument(
        "--skip-parse", action="store_true",
        help="Skip Phase 1 (file parsing into full DB). Use when --db-path is already populated; "
             "implies --all and only does Phase 2 (per-domain copies).",
    )
    args = parser.parse_args()

    if args.years == ["all"]:
        years = AVAILABLE_YEARS
    else:
        years = [int(y) for y in args.years]

    data_dir = Path(args.data_dir)
    full_db = Path(args.db_path)

    # Phase 1: parse files → insert into full DB (unless --skip-parse)
    if args.skip_parse:
        print(f"=== Phase 1: SKIPPED ({full_db} assumed already populated) ===\n")
        if not full_db.exists():
            raise SystemExit(f"DB not found: {full_db}")
    else:
        print(f"=== Phase 1: parse DNS into {full_db} ===")
        parse_dns_into_db(full_db, years, data_dir, args.table, args.force)
        print()

    # Phase 2: propagate to per-domain DBs (if --all or --skip-parse)
    if not (args.all or args.skip_parse):
        return

    print(f"=== Phase 2: propagate DNS rows to per-domain DBs ===")
    for domain in TARGET_DOMAINS:
        folder = domain_folder_name(domain, TARGET_DOMAINS)
        domain_db = data_dir / "domains" / folder / "cdxj.duckdb"
        propagate_dns_to_domain_db(domain_db, full_db, domain, args.table, args.force)


if __name__ == "__main__":
    main()
