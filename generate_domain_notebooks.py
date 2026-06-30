#!/usr/bin/env python3
"""Generate url_structure_<domain>.ipynb for every target domain that doesn't already have one."""

import json
from pathlib import Path
from config import TARGET_DOMAINS
from split_domains import domain_folder_name

OUT_DIR = Path("domain_analysis")
OUT_DIR.mkdir(exist_ok=True)

SKIP = {"doi.gov", "transportation.gov"}  # already have notebooks


def short(domain: str) -> str:
    return domain.removesuffix(".gov").replace(".", "_")


def make_nb(domain: str) -> dict:
    folder = domain_folder_name(domain, TARGET_DOMAINS)
    con_var = short(domain) + "_con"
    sn = domain  # e.g. "commerce.gov"

    cells = []

    def md(src):
        cells.append({"cell_type": "markdown", "id": f"md_{len(cells)}", "metadata": {},
                      "source": src})

    def code(src):
        cells.append({"cell_type": "code", "execution_count": None,
                      "id": f"code_{len(cells)}", "metadata": {},
                      "outputs": [], "source": src})

    md(f"# URL Structure — {sn}\n\n"
       f"Analyses SURT-derived URL structure for `{sn}` captures across all crawl years.\n\n"
       f"All queries use the materialised `surthost_seg_*` and `surtpath_*` columns populated by "
       f"`add_surtkey_columns.py`. No regex on the raw URL; everything is derived from the SURT key.")

    code(f"""\
import sys
sys.path.insert(0, '..')

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np

from domain_analysis import (
    base_domain_sql,
    subdomain_sql,
    build_domain_summary,
    build_subdomain_breakdown,
)

pd.set_option('display.max_rows', 120)
pd.set_option('display.max_colwidth', 80)
sns.set_theme(style='whitegrid')
plt.rcParams['figure.dpi'] = 100

{con_var} = duckdb.connect('../data/domains/{folder}/cdxj.duckdb', read_only=True)
print('Connected.')
{con_var}.sql("SELECT COUNT(*) AS total_rows FROM eot_captures").show()""")

    code(f"""\
# Total captures per crawl year
{con_var}.sql(\"\"\"
    SELECT crawl_year, COUNT(*) AS captures
    FROM eot_captures
    GROUP BY 1 ORDER BY 1
\"\"\").show()""")

    code(f"""\
# Top hosts by year (raw host column)
{con_var}.sql(\"\"\"
    SELECT host, crawl_year, COUNT(*) AS n
    FROM eot_captures
    GROUP BY 1, 2
    ORDER BY n DESC
\"\"\").show(max_rows=50)""")

    md(f"### SURT host columns\n\n"
       "The DB carries `surthost`, `surthost_seg_0..5`, and `surtpath_1..5` as physical columns "
       "parsed from the SURT key by `add_surtkey_columns.py`.\n\n"
       "SURT canonicalization strips a leading `www`, so `www.{domain}` rows have the same "
       "`surthost` as bare `{domain}` and their `surthost_seg_2` is NULL.")

    code(f"""\
# Verify SURT columns on a sample
{con_var}.sql(\"\"\"
    SELECT host, surthost,
           surthost_seg_0 AS tld,
           surthost_seg_1 AS reg_dom,
           surthost_seg_2 AS sub1,
           surthost_seg_3 AS sub2,
           surtpath_1, surtpath_2
    FROM eot_captures
    LIMIT 8
\"\"\").show()""")

    md("### DomainDataSummary — high-level domain structure (SURT-derived)")

    code(f"""\
domain_summary = build_domain_summary({con_var})
domain_summary""")

    code(f"""\
# Subdomain breakdown (surthost_seg_2) by year
subdomain_df = {con_var}.sql(\"\"\"
    SELECT
        COALESCE(surthost_seg_2, '(bare/www)') AS subdomain,
        crawl_year,
        COUNT(*) AS n
    FROM eot_captures
    WHERE surtkey IS NOT NULL
    GROUP BY 1, 2
\"\"\").df()

subdomain_pivot = (subdomain_df
    .pivot_table(index='subdomain', columns='crawl_year', values='n', aggfunc='sum')
    .fillna(0).astype(int))
subdomain_pivot['total'] = subdomain_pivot.sum(axis=1)
subdomain_pivot = subdomain_pivot.sort_values('total', ascending=False).head(30)

total_subs = {con_var}.sql(\"\"\"
    SELECT COUNT(DISTINCT COALESCE(surthost_seg_2, '(bare/www)'))
    FROM eot_captures WHERE surtkey IS NOT NULL
\"\"\").fetchone()[0]
print(f"Unique top-level subdomain labels (surthost_seg_2): {{total_subs:,}}")
subdomain_pivot""")

    md("### SURT path segment analysis")

    for seg_n in range(1, 6):
        idx_cols = [f"COALESCE(NULLIF(surtpath_{i}, ''), '(none)') AS seg{i}" for i in range(1, seg_n + 1)]
        grp_cols = [f"seg{i}" for i in range(1, seg_n + 1)]
        if seg_n == 1:
            idx_cols[0] = "COALESCE(NULLIF(surtpath_1, ''), '(root)') AS seg1"
        select_str = ",\n        ".join(idx_cols)
        group_str = ", ".join(str(i) for i in range(1, seg_n + 2))

        code(f"""\
# SURT path position {seg_n}
seg{seg_n} = {con_var}.sql(\"\"\"
    SELECT
        {select_str},
        crawl_year,
        COUNT(*) AS n
    FROM eot_captures
    GROUP BY {group_str}
\"\"\").df()

seg{seg_n}_pivot = seg{seg_n}.pivot_table(index={grp_cols if seg_n > 1 else repr('seg1')}, columns='crawl_year', values='n',
                        aggfunc='sum').fillna(0).astype(int)
seg{seg_n}_pivot['total'] = seg{seg_n}_pivot.sum(axis=1)
seg{seg_n}_pivot = seg{seg_n}_pivot.sort_values('total', ascending=False)
seg{seg_n}_pivot.head(30)""")

    md(f"### File extensions — {sn}")

    code(f"""\
ext_df = {con_var}.sql(\"\"\"
    SELECT
        COALESCE(NULLIF(lower(regexp_extract(
            regexp_extract(surtkey, '\\\\)([^?]*)', 1),
            '\\\\.([a-zA-Z0-9]+)$', 1)), ''), '(none)') AS ext,
        crawl_year,
        COUNT(*) AS n
    FROM eot_captures
    GROUP BY 1, 2
\"\"\").df()

ext_pivot = ext_df.pivot(index='ext', columns='crawl_year', values='n').fillna(0).astype(int)
ext_pivot['total'] = ext_pivot.sum(axis=1)
ext_pivot = ext_pivot.sort_values('total', ascending=False)
ext_pivot.head(30)""")

    md(f"### Filenames — {sn}")

    code(f"""\
fname_df = {con_var}.sql(\"\"\"
    SELECT
        COALESCE(NULLIF(regexp_extract(
            regexp_extract(surtkey, '\\\\)([^?]*)', 1),
            '/([^/]+)$', 1), ''), '(root)') AS filename,
        crawl_year,
        COUNT(*) AS n
    FROM eot_captures
    GROUP BY 1, 2
\"\"\").df()

fname_pivot = fname_df.pivot(index='filename', columns='crawl_year', values='n').fillna(0).astype(int)
fname_pivot['total'] = fname_pivot.sum(axis=1)
fname_pivot = fname_pivot.sort_values('total', ascending=False)
fname_pivot.head(30)""")

    md("## Visualizations")

    code(f"""\
# Viz 1: Captures over time
year_counts = {con_var}.sql(\"\"\"
    SELECT crawl_year, COUNT(*) AS captures
    FROM eot_captures GROUP BY 1 ORDER BY 1
\"\"\").df()

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.bar(year_counts['crawl_year'], year_counts['captures'],
              color=sns.color_palette('Blues_d', len(year_counts)))
ax.set_yscale('log')
ax.set_ylabel('Captures (log scale)')
ax.set_xlabel('Crawl Year')
ax.set_title('{sn} — Total Captures per Crawl Year')
for bar, val in zip(bars, year_counts['captures']):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.15,
            f'{{val:,}}', ha='center', va='bottom', fontsize=10, fontweight='bold')
ax.set_ylim(1, year_counts['captures'].max() * 5)
plt.tight_layout()
plt.show()""")

    code(f"""\
# Viz 2: Subdomain stacked bar
host_year = {con_var}.sql(\"\"\"
    SELECT
        COALESCE(surthost_seg_2, '(bare/www)') AS subdomain,
        crawl_year, COUNT(*) AS n
    FROM eot_captures WHERE surtkey IS NOT NULL
    GROUP BY 1, 2
\"\"\").df()

totals = host_year.groupby('subdomain')['n'].sum().sort_values(ascending=False)
top_subs = totals.head(8).index.tolist()
host_year['subdomain'] = host_year['subdomain'].where(host_year['subdomain'].isin(top_subs), 'other')

host_pivot = (host_year.pivot_table(index='crawl_year', columns='subdomain', values='n', aggfunc='sum')
                       .fillna(0).astype(int))
col_order = [s for s in top_subs if s in host_pivot.columns]
if 'other' in host_pivot.columns:
    col_order.append('other')
host_pivot = host_pivot[col_order]

ax = host_pivot.plot(kind='bar', stacked=True, figsize=(9, 6),
                     color=sns.color_palette('Set2', len(col_order)))
ax.set_ylabel('Captures')
ax.set_xlabel('Crawl Year')
ax.set_title('{sn} — Subdomain Breakdown per Crawl Year (surthost_seg_2)')
ax.legend(title='Subdomain', bbox_to_anchor=(1.02, 1), loc='upper left')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{{x:,.0f}}'))
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()""")

    code(f"""\
# Viz 3: Top 15 surtpath_1 values heatmap
top15 = seg1_pivot[seg1_pivot.index != '(root)'].head(15)
year_cols = [c for c in top15.columns if c != 'total']

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(np.log1p(top15[year_cols]), annot=top15[year_cols].values,
            fmt=',d', cmap='YlOrRd', linewidths=0.5, ax=ax,
            cbar_kws={{'label': 'log(1 + count)'}})
ax.set_title('{sn} — Top 15 SURT path_1 values by Crawl Year')
ax.set_ylabel('SURT path_1')
ax.set_xlabel('Crawl Year')
plt.tight_layout()
plt.show()""")

    code(f"""\
# Viz 4: path_1 churn between the two most recent crawl years
year_cols_sorted = sorted([c for c in seg1_pivot.columns if c != 'total'])
if len(year_cols_sorted) >= 2:
    yr_a, yr_b = year_cols_sorted[-2], year_cols_sorted[-1]
    in_a = set(seg1_pivot[seg1_pivot[yr_a] > 0].index)
    in_b = set(seg1_pivot[seg1_pivot[yr_b] > 0].index)
    only_a = len(in_a - in_b)
    only_b = len(in_b - in_a)
    both   = len(in_a & in_b)

    churn = pd.DataFrame({{
        'category': [f'Only in {{yr_a}}', 'In both', f'Only in {{yr_b}}'],
        'count': [only_a, both, only_b]
    }})
    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(churn['category'], churn['count'], color=['#e74c3c', '#2ecc71', '#3498db'])
    for bar, val in zip(bars, churn['count']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                str(val), ha='center', va='bottom', fontsize=13, fontweight='bold')
    ax.set_ylabel('Unique seg1 values')
    ax.set_title(f'{sn} — SURT path_1 Churn ({{yr_a}} vs {{yr_b}})')
    ax.set_ylim(0, max(churn['count']) * 1.15)
    plt.tight_layout()
    plt.show()
    print(f"{{yr_a}}: {{len(in_a)}} unique seg1 | {{yr_b}}: {{len(in_b)}} | "
          f"{{only_a}} dropped, {{only_b}} new, {{both}} persisted")
else:
    print("Only one crawl year available — churn chart skipped.")""")

    code(f"""\
# Viz 5: Extension donut chart
top_ext = ext_pivot.head(10).copy()
other_total = ext_pivot.iloc[10:]['total'].sum()
import pandas as _pd
donut_data = _pd.concat([top_ext[['total']], _pd.DataFrame({{'total': [other_total]}}, index=['(other)'])])

fig, ax = plt.subplots(figsize=(8, 8))
colors = sns.color_palette('Set3', len(donut_data))
ax.pie(donut_data['total'], labels=donut_data.index, autopct='%1.1f%%',
       colors=colors, pctdistance=0.82, startangle=90)
ax.add_patch(plt.Circle((0, 0), 0.55, fc='white'))
ax.set_title('{sn} — File Extension Distribution (all years)')
plt.tight_layout()
plt.show()""")

    md("## Content Analysis")

    code(f"""\
# Average SURT path depth per crawl year
depth_stats = {con_var}.sql(\"\"\"
    SELECT
        crawl_year,
        COUNT(*) AS n,
        AVG(len(string_split(trim(regexp_extract(surtkey, '\\\\)([^?]*)', 1), '/'), '/'))) AS avg_depth,
        MEDIAN(len(string_split(trim(regexp_extract(surtkey, '\\\\)([^?]*)', 1), '/'), '/'))) AS median_depth,
        MAX(len(string_split(trim(regexp_extract(surtkey, '\\\\)([^?]*)', 1), '/'), '/'))) AS max_depth
    FROM eot_captures
    WHERE surtkey IS NOT NULL AND surtpath_1 IS NOT NULL AND surtpath_1 != ''
    GROUP BY 1 ORDER BY 1
\"\"\").df()
print("URL path depth statistics per crawl year:")
print(depth_stats.to_string(index=False))""")

    code(f"""\
# Clean URLs (no extension) vs static files per year
clean_vs_static = {con_var}.sql(\"\"\"
    SELECT
        crawl_year,
        SUM(CASE WHEN regexp_extract(regexp_extract(surtkey, '\\\\)([^?]*)', 1),
                '\\\\.([a-zA-Z0-9]+)$', 1) = '' THEN 1 ELSE 0 END) AS clean_urls,
        SUM(CASE WHEN regexp_extract(regexp_extract(surtkey, '\\\\)([^?]*)', 1),
                '\\\\.([a-zA-Z0-9]+)$', 1) != '' THEN 1 ELSE 0 END) AS static_files,
        COUNT(*) AS total
    FROM eot_captures
    GROUP BY 1 ORDER BY 1
\"\"\").df()
clean_vs_static['clean_pct'] = (clean_vs_static['clean_urls'] / clean_vs_static['total'] * 100).round(1)
print("Clean URLs vs static files:")
print(clean_vs_static.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
x = np.arange(len(clean_vs_static))
ax.bar(x, clean_vs_static['clean_urls'], 0.6, label='Clean URLs (no ext)', color='#2ecc71')
ax.bar(x, clean_vs_static['static_files'], 0.6, bottom=clean_vs_static['clean_urls'],
       label='Static files (with ext)', color='#e67e22')
ax.set_xticks(x)
ax.set_xticklabels(clean_vs_static['crawl_year'])
ax.set_ylabel('Captures')
ax.set_xlabel('Crawl Year')
ax.set_title('{sn} — Clean URLs vs Static Files per Year')
ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{{v:,.0f}}'))
plt.tight_layout()
plt.show()""")

    code(f"""\
# Top PDF filenames
pdf_files = {con_var}.sql(\"\"\"
    SELECT
        regexp_extract(regexp_extract(surtkey, '\\\\)([^?]*)', 1), '/([^/]+\\\\.pdf)$', 1) AS pdf_name,
        crawl_year,
        COUNT(*) AS n
    FROM eot_captures
    WHERE lower(regexp_extract(regexp_extract(surtkey, '\\\\)([^?]*)', 1),
                '\\\\.([a-zA-Z0-9]+)$', 1)) = 'pdf'
    GROUP BY 1, 2
\"\"\").df()

pdf_pivot = pdf_files.pivot_table(index='pdf_name', columns='crawl_year', values='n',
                                   aggfunc='sum').fillna(0).astype(int)
pdf_pivot['total'] = pdf_pivot.sum(axis=1)
pdf_pivot = pdf_pivot.sort_values('total', ascending=False)
print(f"Total unique PDF filenames: {{len(pdf_pivot):,}}")
pdf_pivot.head(30)""")

    code(f"""\
# Drupal node IDs (if present)
node_stats = {con_var}.sql(\"\"\"
    SELECT
        crawl_year,
        COUNT(*) AS node_urls,
        MIN(TRY_CAST(surtpath_2 AS INTEGER)) AS min_node_id,
        MAX(TRY_CAST(surtpath_2 AS INTEGER)) AS max_node_id,
        COUNT(DISTINCT surtpath_2) AS unique_nodes
    FROM eot_captures
    WHERE surtpath_1 = 'node'
    GROUP BY 1 ORDER BY 1
\"\"\").df()
if len(node_stats) > 0:
    print("Drupal /node/ statistics per crawl year:")
    print(node_stats.to_string(index=False))
else:
    print("No /node/ paths found — domain may not use Drupal.")""")

    code(f"{con_var}.close()")

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.13.0"}
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


if __name__ == "__main__":
    created = []
    skipped = []
    for domain in TARGET_DOMAINS:
        if domain in SKIP:
            skipped.append(domain)
            continue
        nb_path = OUT_DIR / f"url_structure_{short(domain)}.ipynb"
        nb = make_nb(domain)
        nb_path.write_text(json.dumps(nb, indent=1))
        created.append(str(nb_path))

    print(f"Created {len(created)} notebooks:")
    for p in created:
        print(f"  {p}")
    print(f"\nSkipped (already exist): {skipped}")
