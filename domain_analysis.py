"""SURT-based domain/subdomain parsing helpers for the EOT analysis notebooks.

SURT (Sort-friendly URI Reordering Transform) reverses the host components so
the TLD comes first. Example:
    http://data.transportation.gov/x  ->  gov,transportation,data)/x

Reference: http://crawler.archive.org/articles/user_manual/glossary.html#surt

After `add_surtkey_columns.py` migration, every row in `eot_captures` carries
materialized columns derived from the surtkey:
    - `surthost`        e.g. 'gov,doi,data'
    - `surthost_seg_0`  TLD ('gov')
    - `surthost_seg_1`  registered-domain label ('doi')
    - `surthost_seg_2`  first subdomain label, or NULL for bare/www
    - `surthost_seg_3..5`  deeper subdomain labels, NULL if absent
    - `surtpath_1..5`   surtkey path segments (lowercased per SURT)

These helpers prefer the materialized columns when available; the SQL-expression
variants (`surt_parts_sql`, `subdomain_sql`, etc.) remain available for backward
compatibility with code that may run against an unmigrated DB.
"""

from __future__ import annotations

import duckdb
import pandas as pd


# ---------- Legacy SQL expressions (regex on surtkey, no surt columns needed) ----------

def surt_parts_sql(col: str = "surtkey") -> str:
    """Legacy: SQL expression returning host components of a SURT key as a list.

    Prefer the materialized `surthost_seg_0..5` columns when available.
    """
    return f"string_split(regexp_extract({col}, '^([^)]+)\\)', 1), ',')"


def base_domain_sql(col: str = "surtkey") -> str:
    """Legacy: SQL expression yielding the registered base domain, e.g. 'transportation.gov'.

    Prefer `surthost_seg_1 || '.' || surthost_seg_0` against migrated tables.
    """
    parts = surt_parts_sql(col)
    return (
        f"CASE WHEN len({parts}) >= 2 "
        f"THEN {parts}[2] || '.' || {parts}[1] "
        f"ELSE NULL END"
    )


def subdomain_sql(col: str = "surtkey") -> str:
    """Legacy: SQL expression yielding the joined subdomain prefix.

    For migrated tables, prefer `surthost_seg_2` for top-level subdomain.
    """
    parts = surt_parts_sql(col)
    return (
        f"COALESCE("
        f"array_to_string(list_reverse(list_slice({parts}, 3, len({parts}))), '.'),"
        f" '')"
    )


def tld_sql(col: str = "surtkey") -> str:
    """Legacy: SQL expression yielding the TLD. Prefer `surthost_seg_0`."""
    parts = surt_parts_sql(col)
    return f"CASE WHEN len({parts}) >= 1 THEN {parts}[1] ELSE NULL END"


# ---------- Modern helpers (use materialized surthost_seg_* columns) ----------

def _has_surthost_columns(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Check whether the materialized surthost columns exist on this table."""
    cols = {row[0] for row in con.sql(f"DESCRIBE {table}").fetchall()}
    return {"surthost", "surthost_seg_0", "surthost_seg_1", "surthost_seg_2"}.issubset(cols)


def build_domain_summary(
    con: duckdb.DuckDBPyConnection,
    table: str = "eot_captures",
) -> pd.DataFrame:
    """DomainDataSummary: one row per registered base domain with high-level stats.

    Columns: base_domain, total_captures, unique_subdomains, years_covered,
             top_subdomain, top_subdomain_count, bare_captures, pct_bare.

    Uses materialized `surthost_seg_*` columns when available; falls back to
    regex-on-surtkey for unmigrated tables.
    """
    if _has_surthost_columns(con, table):
        base = "(surthost_seg_1 || '.' || surthost_seg_0)"
        sub = "COALESCE(surthost_seg_2, '')"
    else:
        base = base_domain_sql()
        sub = subdomain_sql()

    sql = f"""
    WITH parsed AS (
        SELECT {base} AS base_domain, {sub} AS subdomain, crawl_year
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
    """Top-N subdomains (by `surthost_seg_2`) pivoted by crawl year.

    Note: `surthost_seg_2` collapses multi-level subdomains under their top
    label (e.g. `iosst1.ios.doi.gov` -> 'ios'). Use `surthost_seg_3` etc. for
    deeper drilling.
    """
    if _has_surthost_columns(con, table):
        sub_expr = "COALESCE(surthost_seg_2, '(bare/www)')"
    else:
        sd = subdomain_sql()
        sub_expr = f"CASE WHEN {sd} = '' THEN '(bare/www)' ELSE {sd} END"

    df = con.sql(f"""
        SELECT
            {sub_expr} AS subdomain,
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
