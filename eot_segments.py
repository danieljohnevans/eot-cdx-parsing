"""Shared path-segment extraction for the RQ notebooks.

Single source of truth for turning a per-domain CDXJ DuckDB into a tidy,
*classified* table of path segments that rq1 / rq3 build on.

Pipeline (see 00_data_and_methods.ipynb for the narrative):
  1. Deduplicate to one row per (crawl_year, surtkey); drop `dns:` records.
  2. Strip a trailing filename (``/<name>.<2-4 alnum>``) to get the directory path.
  3. Emit each positional directory segment in long form.
  4. Classify each distinct segment with readability.classify_segment.

Subdomains are intentionally excluded (path segments only), per the RQ design.
"""
from __future__ import annotations

import duckdb
import pandas as pd

import readability as rb

MAX_SEG = 5  # positional directory segments to extract (/a/b/c/d/e)

# dedup + directory-path CTE, shared by the positional UNION below.
_DEDUP_PATHS_CTE = r"""
WITH dedup AS (
  SELECT crawl_year, surtkey,
         min(regexp_extract(surtkey, '\)([^?]*)', 1)) AS url_path
  FROM eot_captures
  WHERE url NOT LIKE 'dns:%'
  GROUP BY 1, 2
),
paths AS (
  SELECT crawl_year,
    CASE WHEN regexp_matches(url_path, '/[^/]+\.[a-zA-Z0-9]{2,4}$')
         THEN regexp_replace(url_path, '/[^/]+\.[a-zA-Z0-9]{2,4}$', '')
         ELSE rtrim(url_path, '/') END AS dir_path
  FROM dedup
)
"""


def segment_sql(max_seg: int = MAX_SEG) -> str:
    """Long-form positional segment SQL: (crawl_year, pos, seg, n=unique URLs)."""
    parts = []
    for i in range(1, max_seg + 1):
        extract = f"list_extract(string_split(trim(dir_path,'/'),'/'),{i})"
        parts.append(
            f"SELECT crawl_year, {i} AS pos, {extract} AS seg, count(*) AS n\n"
            f"FROM paths WHERE {extract} IS NOT NULL AND {extract} != ''\n"
            f"GROUP BY 1, 2, 3"
        )
    return _DEDUP_PATHS_CTE + "\nUNION ALL\n".join(parts)


def load_segments(dbs: dict, max_seg: int = MAX_SEG) -> pd.DataFrame:
    """Pool classified path segments across the given {domain: db_path} mapping.

    Returns long-form rows: domain, crawl_year, pos, seg, n (unique-URL count), cls.
    `n` is the token-weighted count; for type-level, drop_duplicates on
    (domain, crawl_year, pos, seg).
    """
    sql = segment_sql(max_seg)
    frames = []
    for dom, path in dbs.items():
        con = duckdb.connect(str(path), read_only=True)
        df = con.sql(sql).df()
        con.close()
        df.insert(0, "domain", dom)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    out["crawl_year"] = out["crawl_year"].astype(str)
    out["cls"] = out["seg"].map(rb.classify_segment)
    return out.dropna(subset=["cls"])


def readability_pct(df: pd.DataFrame, group_cols, level: str = "token") -> pd.DataFrame:
    """% of segments in each readability class, grouped by `group_cols`.

    level='token' weights by URL volume (column n); level='type' counts each
    distinct (group + seg) once. Returns a frame with one column per class in
    readability.CLASSES plus 'total'.
    """
    if isinstance(group_cols, str):
        group_cols = [group_cols]
    if level == "type":
        d = df.drop_duplicates(group_cols + ["seg"]).assign(_w=1)
        w = "_w"
    elif level == "token":
        d, w = df, "n"
    else:
        raise ValueError("level must be 'token' or 'type'")

    g = d.groupby(group_cols + ["cls"])[w].sum().unstack("cls").fillna(0)
    for c in rb.CLASSES:
        if c not in g.columns:
            g[c] = 0
    g = g[list(rb.CLASSES)]
    g["total"] = g.sum(axis=1)
    for c in rb.CLASSES:
        g[c] = (100 * g[c] / g["total"]).round(1)
    return g.reset_index()
