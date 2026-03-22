#!/usr/bin/env python3
"""Compare the CDXJ-loaded DB vs the parquet-loaded DB to diagnose row count discrepancies.

Connects to both databases (read-only) and prints a diagnostic report.

Usage:
    python compare_dbs.py
    python compare_dbs.py --cdxj-db data/eot.duckdb --parquet-db data/eot_parquet.duckdb
"""

import argparse

import duckdb

from config import DB_PATH, PARQUET_DB_PATH, TARGET_DOMAINS

DOMAIN_LIST = ", ".join(f"'{d}'" for d in TARGET_DOMAINS)


def section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def run(con: duckdb.DuckDBPyConnection, sql: str):
    """Execute and show a query."""
    con.sql(sql).show()


def main():
    parser = argparse.ArgumentParser(description="Compare CDXJ vs parquet DuckDB databases")
    parser.add_argument("--cdxj-db", default=str(DB_PATH), help=f"CDXJ database path. Default: {DB_PATH}")
    parser.add_argument("--parquet-db", default=str(PARQUET_DB_PATH), help=f"Parquet database path. Default: {PARQUET_DB_PATH}")
    args = parser.parse_args()

    cdxj = duckdb.connect(args.cdxj_db, read_only=True)
    pq = duckdb.connect(args.parquet_db, read_only=True)

    # -- 1. Total counts --
    section("1. Total Row Counts")
    cdxj_total = cdxj.sql("SELECT COUNT(*) FROM eot_captures").fetchone()[0]
    pq_total = pq.sql("SELECT COUNT(*) FROM eot_parquet").fetchone()[0]
    print(f"  CDXJ DB:    {cdxj_total:>15,}")
    print(f"  Parquet DB: {pq_total:>15,}")
    print(f"  Diff:       {pq_total - cdxj_total:>+15,}  (parquet - cdxj)")

    # -- 2. Counts per crawl year --
    section("2. Row Counts per Crawl Year")
    cdxj_years = cdxj.sql("""
        SELECT crawl_year, COUNT(*) AS cdxj_rows
        FROM eot_captures GROUP BY 1 ORDER BY 1
    """).df()
    pq_years = pq.sql("""
        SELECT crawl_year, COUNT(*) AS parquet_rows
        FROM eot_parquet GROUP BY 1 ORDER BY 1
    """).df()
    merged = cdxj_years.merge(pq_years, on="crawl_year", how="outer").fillna(0)
    merged["diff"] = merged["parquet_rows"] - merged["cdxj_rows"]
    print(merged.to_string(index=False))

    # -- 3. Counts per base domain --
    section("3. Row Counts per Base Domain (all years combined)")

    # Build domain CASE for CDXJ (uses host column with ends_with)
    cdxj_domain_case = "\n".join(
        f"        WHEN host = '{d}' OR ends_with(host, '.{d}') THEN '{d}'"
        for d in TARGET_DOMAINS
    )

    cdxj_domains = cdxj.sql(f"""
        SELECT CASE
{cdxj_domain_case}
            ELSE 'other'
        END AS base_domain,
        COUNT(*) AS cdxj_rows
        FROM eot_captures
        GROUP BY 1 ORDER BY cdxj_rows DESC
    """).df()

    pq_domains = pq.sql(f"""
        SELECT url_host_registered_domain AS base_domain,
               COUNT(*) AS parquet_rows
        FROM eot_parquet
        WHERE url_host_registered_domain IN ({DOMAIN_LIST})
        GROUP BY 1 ORDER BY parquet_rows DESC
    """).df()

    domain_merged = cdxj_domains.merge(pq_domains, on="base_domain", how="outer").fillna(0)
    domain_merged["diff"] = domain_merged["parquet_rows"] - domain_merged["cdxj_rows"]
    domain_merged = domain_merged.sort_values("diff", key=abs, ascending=False)
    print(domain_merged.to_string(index=False))

    # -- 4. Counts per domain per year --
    section("4. Row Counts per Domain x Year (where diff != 0)")

    cdxj_dy = cdxj.sql(f"""
        SELECT CASE
{cdxj_domain_case}
            ELSE 'other'
        END AS base_domain,
        crawl_year,
        COUNT(*) AS cdxj_rows
        FROM eot_captures
        GROUP BY 1, 2
    """).df()

    pq_dy = pq.sql(f"""
        SELECT url_host_registered_domain AS base_domain,
               crawl_year,
               COUNT(*) AS parquet_rows
        FROM eot_parquet
        WHERE url_host_registered_domain IN ({DOMAIN_LIST})
        GROUP BY 1, 2
    """).df()

    dy_merged = cdxj_dy.merge(pq_dy, on=["base_domain", "crawl_year"], how="outer").fillna(0)
    dy_merged["diff"] = dy_merged["parquet_rows"] - dy_merged["cdxj_rows"]
    dy_merged = dy_merged[dy_merged["diff"] != 0].sort_values("diff", key=abs, ascending=False)
    print(dy_merged.to_string(index=False))

    # -- 5. Parquet subset breakdown --
    section("5. Parquet 'subset' Column Breakdown")
    print("  (If CDXJ only covers 'warc' subset, 'cdx' rows explain the extra)")
    pq.sql(f"""
        SELECT subset, COUNT(*) AS rows
        FROM eot_parquet
        WHERE url_host_registered_domain IN ({DOMAIN_LIST})
        GROUP BY 1
        ORDER BY rows DESC
    """).show()

    # -- 6. Duplicate analysis --
    section("6. Duplicate Rows (same url + digest)")

    print("  CDXJ duplicates (same url + digest):")
    cdxj.sql("""
        SELECT COUNT(*) AS total,
               COUNT(*) - COUNT(DISTINCT (url || '|' || COALESCE(digest, ''))) AS duplicate_rows
        FROM eot_captures
    """).show()

    print("  Parquet duplicates (same url + content_digest):")
    pq.sql(f"""
        SELECT COUNT(*) AS total,
               COUNT(*) - COUNT(DISTINCT (url || '|' || COALESCE(content_digest, ''))) AS duplicate_rows
        FROM eot_parquet
        WHERE url_host_registered_domain IN ({DOMAIN_LIST})
    """).show()

    # -- 7. Domain matching edge cases --
    section("7. Parquet Rows Not Matching CDXJ Domain Logic")
    print("  Hosts in parquet (via url_host_registered_domain) that might not")
    print("  match CDXJ's ends_with() logic:")
    pq.sql(f"""
        SELECT url_host_name, url_host_registered_domain, COUNT(*) AS rows
        FROM eot_parquet
        WHERE url_host_registered_domain IN ({DOMAIN_LIST})
          AND url_host_name NOT IN ({DOMAIN_LIST})
          AND NOT (
            {' OR '.join(
                f"ends_with(url_host_name, '.{d}')"
                for d in TARGET_DOMAINS
            )}
          )
        GROUP BY 1, 2
        ORDER BY rows DESC
        LIMIT 20
    """).show()

    cdxj.close()
    pq.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
