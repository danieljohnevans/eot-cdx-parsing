#!/usr/bin/env python3
"""Build per-domain × per-year row counts for parquet and CDXJ.

Output columns: folder, 2004 parquet, ..., 2024 parquet, ALL parquet,
                        2004 CDXJ,    ..., 2024 CDXJ,    ALL CDXJ

Rows are folder-style names (01_commerce, 02_defense, ...), matching the
layout used by split_domains.py.

Usage:
    python build_domain_year_matrix.py
    python build_domain_year_matrix.py --out counts.csv
"""

import argparse

import duckdb
import pandas as pd

from config import AVAILABLE_YEARS, DB_PATH, PARQUET_DB_PATH, TARGET_DOMAINS
from load_db import surtkey_prefix

YEARS = AVAILABLE_YEARS


def folder_name(domain: str) -> str:
    idx = sorted(TARGET_DOMAINS).index(domain) + 1
    return f"{idx:02d}_{domain.removesuffix('.gov')}"


def _surtkey_domain_case(column: str) -> str:
    """Build a CASE expression mapping a surtkey column to base domain label.

    Matches bare (`gov,X)`), bare-with-port (`gov,X:PORT)`), and subdomain
    (`gov,X,...`) SURT host shapes.
    """
    lines = []
    for d in TARGET_DOMAINS:
        p = surtkey_prefix(d)
        lines.append(
            f"        WHEN {column} LIKE '{p})%' OR {column} LIKE '{p}:%' "
            f"OR {column} LIKE '{p},%' THEN '{d}'"
        )
    return "\n".join(lines)


def cdxj_long(path: str) -> pd.DataFrame:
    con = duckdb.connect(path, read_only=True)
    case_sql = _surtkey_domain_case("surtkey")
    df = con.sql(f"""
        SELECT CASE
{case_sql}
            ELSE NULL END AS domain,
        CAST(crawl_year AS INTEGER) AS year,
        COUNT(*) AS n
        FROM eot_captures
        GROUP BY 1, 2
    """).df()
    con.close()
    df = df.dropna(subset=["domain"])
    df["source"] = "CDXJ"
    return df


def parquet_long(path: str) -> pd.DataFrame:
    con = duckdb.connect(path, read_only=True)
    case_sql = _surtkey_domain_case("url_surtkey")
    df = con.sql(f"""
        SELECT CASE
{case_sql}
            ELSE NULL END AS domain,
        CAST(crawl_year AS INTEGER) AS year,
        COUNT(*) AS n
        FROM eot_parquet
        GROUP BY 1, 2
    """).df()
    con.close()
    df = df.dropna(subset=["domain"])
    df["source"] = "parquet"
    return df


def build_matrix(cdxj_df: pd.DataFrame, parquet_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([cdxj_df, parquet_df], ignore_index=True)
    # Fill the full grid so missing combos show as 0, not NaN
    grid = (pd.MultiIndex.from_product(
                [TARGET_DOMAINS, YEARS, ["parquet", "CDXJ"]],
                names=["domain", "year", "source"])
              .to_frame(index=False)
              .merge(combined, on=["domain", "year", "source"], how="left")
              .fillna({"n": 0}))
    grid["n"] = grid["n"].astype(int)

    totals = (grid.groupby(["domain", "source"], as_index=False)["n"].sum()
                  .assign(year="ALL"))
    grid = pd.concat([grid, totals], ignore_index=True)

    grid["col"] = grid["year"].astype(str) + " " + grid["source"]
    wide = grid.pivot(index="domain", columns="col", values="n").fillna(0).astype(int)

    ordered = [f"{y} parquet" for y in YEARS] + ["ALL parquet"] \
            + [f"{y} CDXJ"    for y in YEARS] + ["ALL CDXJ"]
    wide = wide[ordered]

    wide.index = [folder_name(d) for d in wide.index]
    wide.index.name = "folder"
    return wide.sort_index()


def main():
    p = argparse.ArgumentParser(description="Build per-domain × per-year row counts CSV")
    p.add_argument("--cdxj-db", default=str(DB_PATH))
    p.add_argument("--parquet-db", default=str(PARQUET_DB_PATH))
    p.add_argument("--out", default="domain_year_counts.csv")
    args = p.parse_args()

    print(f"Querying CDXJ:    {args.cdxj_db}")
    cdxj_df = cdxj_long(args.cdxj_db)
    print(f"Querying parquet: {args.parquet_db}")
    parquet_df = parquet_long(args.parquet_db)

    matrix = build_matrix(cdxj_df, parquet_df)
    matrix.to_csv(args.out)
    print(f"\nWrote {args.out}  ({len(matrix)} rows × {len(matrix.columns)} cols)")
    print(matrix.to_string())


if __name__ == "__main__":
    main()
