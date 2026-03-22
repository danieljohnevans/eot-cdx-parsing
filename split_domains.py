#!/usr/bin/env python3
"""Split the CDXJ and parquet databases into per-domain DuckDB files.

Creates data/domains/{domain}/cdxj.duckdb and data/domains/{domain}/parquet.duckdb
for each of the 15 TARGET_DOMAINS.

Usage:
    python split_domains.py
    python split_domains.py --domains usda.gov ed.gov
"""

import argparse
import time
from pathlib import Path

import duckdb

from config import DB_PATH, PARQUET_DB_PATH, TARGET_DOMAINS

DATA_DIR = Path("data")


def split_cdxj(cdxj_path: str, domain: str, out_path: Path):
    """Extract one domain from the CDXJ database."""
    src = duckdb.connect(cdxj_path, read_only=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    dst = duckdb.connect(str(out_path))
    dst.execute("DROP TABLE IF EXISTS eot_captures")

    # Attach source as read-only
    dst.execute(f"ATTACH '{cdxj_path}' AS src (READ_ONLY)")
    dst.execute(f"""
        CREATE TABLE eot_captures AS
        SELECT * FROM src.eot_captures
        WHERE host = '{domain}' OR ends_with(host, '.{domain}')
    """)
    count = dst.sql("SELECT COUNT(*) FROM eot_captures").fetchone()[0]
    dst.execute("DETACH src")
    dst.close()
    src.close()
    return count


def split_parquet(parquet_path: str, domain: str, out_path: Path):
    """Extract one domain from the parquet database."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    dst = duckdb.connect(str(out_path))
    dst.execute("DROP TABLE IF EXISTS eot_parquet")

    dst.execute(f"ATTACH '{parquet_path}' AS src (READ_ONLY)")
    dst.execute(f"""
        CREATE TABLE eot_parquet AS
        SELECT * FROM src.eot_parquet
        WHERE url_host_registered_domain = '{domain}'
    """)
    count = dst.sql("SELECT COUNT(*) FROM eot_parquet").fetchone()[0]
    dst.execute("DETACH src")
    dst.close()
    return count


def main():
    parser = argparse.ArgumentParser(description="Split DBs into per-domain DuckDB files")
    parser.add_argument(
        "--domains",
        nargs="+",
        default=None,
        help=f"Domains to split. Default: all {len(TARGET_DOMAINS)} TARGET_DOMAINS",
    )
    parser.add_argument("--cdxj-db", default=str(DB_PATH))
    parser.add_argument("--parquet-db", default=str(PARQUET_DB_PATH))
    parser.add_argument("--out-dir", default=str(DATA_DIR / "domains"))
    args = parser.parse_args()

    domains = args.domains or TARGET_DOMAINS
    out_dir = Path(args.out_dir)

    print(f"Splitting {len(domains)} domains into {out_dir}/\n")
    print(f"{'Domain':<25} {'CDXJ rows':>12} {'Parquet rows':>14} {'Diff':>10}")
    print("-" * 65)

    t0 = time.time()
    for domain in domains:
        cdxj_out = out_dir / domain / "cdxj.duckdb"
        pq_out = out_dir / domain / "parquet.duckdb"

        cdxj_count = split_cdxj(args.cdxj_db, domain, cdxj_out)
        pq_count = split_parquet(args.parquet_db, domain, pq_out)
        diff = pq_count - cdxj_count

        print(f"{domain:<25} {cdxj_count:>12,} {pq_count:>14,} {diff:>+10,}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output: {out_dir}/")


if __name__ == "__main__":
    main()
