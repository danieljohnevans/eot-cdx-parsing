"""SURT-based domain/subdomain parsing helpers for the EOT analysis notebooks.

SURT (Sort-friendly URI Reordering Transform) reverses the host components so
the TLD comes first. Example:
    http://data.transportation.gov/x  ->  gov,transportation,data)/x

Reference: http://crawler.archive.org/articles/user_manual/glossary.html#surt

Every row in `eot_captures` carries a `surtkey` column populated during CDXJ
load (see load_db.py). These helpers extract base_domain / subdomain from
that key so analysis queries don't need hard-coded host CASE statements.
"""

from __future__ import annotations

import duckdb
import pandas as pd


def surt_parts_sql(col: str = "surtkey") -> str:
    """SQL expression returning the host components of a SURT key as a list.

    Example: 'gov,transportation,data)/x' -> ['gov', 'transportation', 'data']
    DuckDB lists are 1-indexed.
    """
    return f"string_split(regexp_extract({col}, '^([^)]+)\\)', 1), ',')"


def base_domain_sql(col: str = "surtkey") -> str:
    """SQL expression yielding the registered base domain, e.g. 'transportation.gov'."""
    parts = surt_parts_sql(col)
    return (
        f"CASE WHEN len({parts}) >= 2 "
        f"THEN {parts}[2] || '.' || {parts}[1] "
        f"ELSE NULL END"
    )


def subdomain_sql(col: str = "surtkey") -> str:
    """SQL expression yielding the subdomain prefix (leftmost...innermost).

    'gov,transportation,data,www)/x'  -> 'www.data'
    'gov,transportation)/x'           -> ''   (bare domain)
    """
    parts = surt_parts_sql(col)
    return (
        f"COALESCE("
        f"array_to_string(list_reverse(list_slice({parts}, 3, len({parts}))), '.'),"
        f" '')"
    )


def tld_sql(col: str = "surtkey") -> str:
    """SQL expression yielding the TLD, e.g. 'gov'."""
    parts = surt_parts_sql(col)
    return f"CASE WHEN len({parts}) >= 1 THEN {parts}[1] ELSE NULL END"


def build_domain_summary(
    con: duckdb.DuckDBPyConnection,
    table: str = "eot_captures",
) -> pd.DataFrame:
    """DomainDataSummary: one row per registered base domain with high-level stats.

    Columns: base_domain, total_captures, unique_subdomains, years_covered,
             top_subdomain, top_subdomain_count, bare_captures, pct_bare.
    """
    bd = base_domain_sql()
    sd = subdomain_sql()
    sql = f"""
    WITH parsed AS (
        SELECT {bd} AS base_domain, {sd} AS subdomain, crawl_year
        FROM {table}
        WHERE surtkey IS NOT NULL
    ),
    subdom_counts AS (
        SELECT base_domain, subdomain, COUNT(*) AS n,
               ROW_NUMBER() OVER (PARTITION BY base_domain ORDER BY COUNT(*) DESC) AS rnk
        FROM parsed
        WHERE base_domain IS NOT NULL
        GROUP BY base_domain, subdomain
    ),
    top_sub AS (
        SELECT base_domain,
               subdomain AS top_subdomain,
               n AS top_subdomain_count
        FROM subdom_counts WHERE rnk = 1
    ),
    domain_stats AS (
        SELECT
            base_domain,
            COUNT(*) AS total_captures,
            COUNT(DISTINCT subdomain) AS unique_subdomains,
            COUNT(DISTINCT crawl_year) AS years_covered,
            SUM(CASE WHEN subdomain = '' THEN 1 ELSE 0 END) AS bare_captures
        FROM parsed
        WHERE base_domain IS NOT NULL
        GROUP BY base_domain
    )
    SELECT
        d.base_domain,
        d.total_captures,
        d.unique_subdomains,
        d.years_covered,
        t.top_subdomain,
        t.top_subdomain_count,
        d.bare_captures,
        ROUND(100.0 * d.bare_captures / d.total_captures, 1) AS pct_bare
    FROM domain_stats d
    LEFT JOIN top_sub t USING (base_domain)
    ORDER BY d.total_captures DESC
    """
    return con.sql(sql).df()


def build_subdomain_breakdown(
    con: duckdb.DuckDBPyConnection,
    table: str = "eot_captures",
    limit: int = 20,
) -> pd.DataFrame:
    """Top-N subdomains by capture count, pivoted by crawl year.

    For single-domain DBs this is a subdomain-only view. For the aggregate DB
    (explore_eot.ipynb) the table is multi-domain; pass a pre-filtered
    connection/view if you need per-domain breakdowns.
    """
    sd = subdomain_sql()
    df = con.sql(f"""
        SELECT
            CASE WHEN {sd} = '' THEN '(bare)' ELSE {sd} END AS subdomain,
            crawl_year,
            COUNT(*) AS n
        FROM {table}
        WHERE surtkey IS NOT NULL
        GROUP BY 1, 2
    """).df()
    pivot = (
        df.pivot_table(index="subdomain", columns="crawl_year", values="n", aggfunc="sum")
          .fillna(0).astype(int)
    )
    pivot["total"] = pivot.sum(axis=1)
    return pivot.sort_values("total", ascending=False).head(limit)
