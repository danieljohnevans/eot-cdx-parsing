# eot-cdx-parsing

Scripts to download and parse EOT (End of Term) web crawl CDX data for 15 federal .gov domains across crawl years 2004, 2008, 2012, 2016, 2020, and 2024.

## Server Access via VS Code

The data and databases live on `htrc-sandbox2`. The easiest way to work with the notebooks remotely is VS Code Remote-SSH.

**1. Install the extension**
In VS Code, install **Remote - SSH** (by Microsoft) from the Extensions marketplace.

**2. Add the host**
Press `Cmd+Shift+P` → **Remote-SSH: Open SSH Configuration File** and add:

```
Host htrc-sandbox2
  HostName <hostname-or-ip>
  User <your-username>
```

If you already have SSH access via terminal, you can instead press `Cmd+Shift+P` → **Remote-SSH: Connect to Host** → **Add New SSH Host** and paste your existing `ssh` command.

**3. Connect**
`Cmd+Shift+P` → **Remote-SSH: Connect to Host** → select `htrc-sandbox2`. VS Code installs a small server component on the remote the first time (~1 min).

**4. Open the project**
Once connected: **File → Open Folder** → `/data/KNURL/eot-cdx-parsing`

**5. Run notebooks**
Install the **Jupyter** VS Code extension, then open any `.ipynb` file. When prompted to select a kernel, choose:
```
/data/KNURL/eot-cdx-parsing/venv/bin/python
```

## Setup

```bash
# Install dependencies into the venv
venv/bin/python -m pip install -r requirements.txt
```

## Project Structure

```
data/
  eot.duckdb              # full CDXJ database (all 15 domains)
  eot_parquet.duckdb      # parquet-based index
  domains/
    01_commerce/
      cdxj.duckdb         # per-domain CDXJ database
      parquet.duckdb
    02_defense/
    ...                   # 15 domains total
  cdxj/EOT-{year}/        # raw downloaded .cdxj.gz files
  parquet/EOT-{year}/     # raw downloaded .parquet files

analysis/                 # RQ-focused analysis notebooks (current)
  00_data_and_methods.ipynb          # corpus, dedup rule, readability classifier + validation
  rq1_readability_over_time.ipynb    # human vs machine URL segments over time
  rq2_complexity_and_construction.ipynb  # depth/length + CMS/framework tells
  rq3_human_vocabulary.ipynb         # what the human words are, and how they shift

record_analysis/
  dns_analysis.ipynb      # DNS record outlier analysis (ed.gov)
  surt_dedup_analysis.ipynb  # SURT URL deduplication and cross-year overlap

archive/                  # superseded notebooks kept for reference (not maintained)
  EOT_parquet.ipynb                  # early Colab pipeline experiments
  generate_domain_notebooks.py       # old per-domain notebook generator
  domain_analysis/url_structure_*.ipynb  # 15 per-domain notebooks (replaced by analysis/)
```

## Research question

**Can we trace a shift from human-readable to machine-readable URLs on the U.S.
federal web, 2004–2024?** URL readability is government-relevant (plain-language /
usability guidance favors human-meaningful, citable URLs), so a measured decline
is a finding in tension with policy — not just generic web-tech evolution. The
`analysis/` notebooks address this; start with `00_data_and_methods.ipynb`.

## Target Domains

15 federal cabinet-level .gov domains:
`usda.gov`, `commerce.gov`, `defense.gov`, `ed.gov`, `energy.gov`, `hhs.gov`,
`dhs.gov`, `hud.gov`, `doi.gov`, `justice.gov`, `dol.gov`, `state.gov`,
`transportation.gov`, `treasury.gov`, `va.gov`

## Key Scripts

| Script | Purpose |
|--------|---------|
| `download_eot.py` | Download `.cdxj.gz` files from S3 |
| `download_parquet.py` | Download `.parquet` files from S3 |
| `load_db.py` | Parse CDXJ files → `data/eot.duckdb` |
| `load_parquet_db.py` | Load parquet files → `data/eot_parquet.duckdb` |
| `split_domains.py` | Split full DB into per-domain DBs under `data/domains/` |
| `add_dns_records.py` | Backfill `text/dns` records (introduced in 2024 crawl) |
| `add_surtkey_columns.py` | Add SURT-derived columns to existing DBs |
| `build_domain_year_matrix.py` | Build summary CSV of captures per domain per year |

## Analysis modules (imported by the `analysis/` notebooks)

| Module | Purpose |
|--------|---------|
| `readability.py` | `classify_segment()` — labels a URL path segment `human` / `acronym` / `machine` using `wordfreq`. Single source of truth for the readability method; tunables are module constants. |
| `eot_segments.py` | Dedup + directory-path + positional-segment extraction pooled across domains (`load_segments`, `readability_pct`). |
| `config.py` | `discover_domain_dbs()` finds each per-domain DuckDB across local/server layouts, plus paths and `TARGET_DOMAINS`. |

## Notes

- DNS records (`mime=text/dns`) only exist in the 2024 crawl — earlier crawls were not configured to capture DNS lookups.
- No parquet files are available for 2024 at this time.
- `transportation.gov` is missing 2012 data.
- Per-domain DBs use `data/domains/NN_basename/` naming (e.g. `06_ed/` for `ed.gov`).
