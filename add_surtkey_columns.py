#!/usr/bin/env python3
"""Migrate an existing CDXJ DuckDB to add surtkey-derived columns in place.

Adds these columns to the eot_captures table:
    surthost                       (e.g. 'gov,doi,data')
    surthost_seg_0..5              (TLD, regdomain label, sub1..4)
    surtpath_1..5                  (path segments parsed from surtkey)

Strategy: CREATE TABLE eot_captures_new AS SELECT *, <new cols> FROM eot_captures,
then DROP + RENAME. Avoids per-row UPDATE; uses ~2x disk during the operation.

Idempotent: if the target columns already exist, the script no-ops.

Usage:
    python add_surtkey_columns.py --db-path data/eot.duckdb
    python add_surtkey_columns.py --db-path data/04_doi/cdxj.duckdb
    python add_surtkey_columns.py --all   # walks data/<NN_*>/cdxj.duckdb + data/eot.duckdb
"""

import argparse
import glob
import time
from pathlib import Path

import duckdb

from load_db import build_surt_columns, SURTHOST_DEPTH, SURTPATH_DEPTH


NEW_COLUMNS = (
    ["surthost"]
    + [f"surthost_seg_{i}" for i in range(SURTHOST_DEPTH)]
    + [f"surtpath_{i}" for i in range(1, SURTPATH_DEPTH + 1)]
)


def existing_columns(con: duckdb.DuckDBPyConnection, table: str = "eot_captures") -> set:
    return {row[0] for row in con.sql(f"DESCRIBE {table}").fetchall()}


def migrate(db_path: str, table: str = "eot_captures"):
    if not Path(db_path).exists():
        print(f"  SKIP — file not found: {db_path}")
        return

    print(f"\n=== {db_path} ===")
    con = duckdb.connect(db_path)

    cols = existing_columns(con, table)
    if all(c in cols for c in NEW_COLUMNS):
        print(f"  Already migrated — all {len(NEW_COLUMNS)} surtkey columns present")
        con.close()
        return

    if "surtkey" not in cols:
        print(f"  ERROR — table {table} has no surtkey column; cannot derive new columns")
        con.close()
        return

    row_count_before = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  Rows: {row_count_before:,}")

    surt_cols = build_surt_columns()
    new_table = f"{table}_new"

    print(f"  Building {new_table} with new columns...")
    t0 = time.time()
    con.execute(f"DROP TABLE IF EXISTS {new_table}")
    con.execute(f"""
        CREATE TABLE {new_table} AS
        SELECT *,
            {surt_cols}
        FROM {table}
    """)
    elapsed = time.time() - t0
    print(f"  Built in {elapsed:.1f}s")

    row_count_after = con.sql(f"SELECT COUNT(*) FROM {new_table}").fetchone()[0]
    if row_count_after != row_count_before:
        print(f"  ROW COUNT MISMATCH: {row_count_before:,} -> {row_count_after:,}, aborting")
        con.execute(f"DROP TABLE {new_table}")
        con.close()
        return

    print(f"  Swapping tables...")
    con.execute(f"DROP TABLE {table}")
    con.execute(f"ALTER TABLE {new_table} RENAME TO {table}")

    print(f"  Reclaiming space (CHECKPOINT)...")
    con.execute("CHECKPOINT")

    final_cols = existing_columns(con, table)
    added = [c for c in NEW_COLUMNS if c in final_cols]
    print(f"  Added {len(added)} columns: {', '.join(added)}")

    con.sql(
        f"SELECT surthost, surthost_seg_0, surthost_seg_1, surthost_seg_2, "
        f"surtpath_1, surtpath_2 FROM {table} LIMIT 3"
    ).show()

    con.close()


def main():
    p = argparse.ArgumentParser(description="Add surtkey-derived columns to CDXJ DuckDB(s)")
    p.add_argument("--db-path", help="Single DuckDB file to migrate")
    p.add_argument(
        "--all",
        action="store_true",
        help="Walk and migrate data/eot.duckdb plus data/<NN_name>/cdxj.duckdb",
    )
    p.add_argument("--table", default="eot_captures")
    args = p.parse_args()

    targets = []
    if args.db_path:
        targets.append(args.db_path)
    if args.all:
        if Path("data/eot.duckdb").exists():
            targets.append("data/eot.duckdb")
        # per-domain files: data/01_commerce/cdxj.duckdb etc.
        targets.extend(sorted(glob.glob("data/[0-9][0-9]_*/cdxj.duckdb")))
        # legacy: top-level NN_name folders too
        targets.extend(sorted(glob.glob("[0-9][0-9]_*/cdxj.duckdb")))

    if not targets:
        p.error("Provide --db-path FILE or --all")

    for t in targets:
        migrate(t, args.table)

    print("\nDone.")


if __name__ == "__main__":
    main()
