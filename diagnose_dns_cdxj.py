#!/usr/bin/env python3
"""Diagnose whether 2004-2020 CDXJ files contain DNS records the backfill missed.

Run on the SERVER (needs the raw data/cdxj/EOT-*/*.cdxj.gz files).

For each crawl year it samples a handful of CDXJ files and reports:
  - how many lines mention 'dns:' at all (format-agnostic),
  - how many are real text/dns records (via json.loads on the blob),
  - how many would be caught by add_dns_records.py's substring pre-filter
    (`"url":"dns:`), i.e. the compact-JSON assumption,
  - a raw sample line so you can see the exact JSON spacing.

If 'real dns' > 0 but 'caught by prefilter' == 0, the backfill's substring
optimization is the bug: it assumed compact JSON and skipped spaced JSON.

Usage:
    python diagnose_dns_cdxj.py
    python diagnose_dns_cdxj.py --files-per-year 20
"""

import argparse
import glob
import gzip
import json
from pathlib import Path

from config import AVAILABLE_YEARS, DATA_DIR, cdxj_dir

PREFILTER = '"url":"dns:'  # the exact needle add_dns_records.py uses


def scan_file(path: str):
    mentions = real_dns = caught = 0
    sample = None
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            if "dns:" not in line:
                continue
            mentions += 1
            if PREFILTER in line:
                caught += 1
            try:
                _, _, blob = line.strip().split(" ", 2)
                rec = json.loads(blob)
            except Exception:
                continue
            if rec.get("mime") == "text/dns":
                real_dns += 1
                if sample is None:
                    sample = line.strip()[:220]
    return mentions, real_dns, caught, sample


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files-per-year", type=int, default=10)
    ap.add_argument("--data-dir", default=str(DATA_DIR))
    args = ap.parse_args()
    data_dir = Path(args.data_dir)

    print(f"{'year':>6} {'files':>6} {'dns-mentions':>13} {'real-dns':>10} "
          f"{'prefilter-hits':>15}   sample")
    for year in AVAILABLE_YEARS:
        pdir = cdxj_dir(data_dir, year)
        files = sorted(glob.glob(str(pdir / "*.cdxj.gz")))[: args.files_per_year]
        if not files:
            print(f"{year:>6}   (no CDXJ files at {pdir})")
            continue
        tm = tr = tc = 0
        sample = None
        for fp in files:
            m, r, c, s = scan_file(fp)
            tm += m; tr += r; tc += c
            if sample is None and s:
                sample = s
        verdict = ""
        if tr > 0 and tc == 0:
            verdict = "  <-- MISSED by prefilter (spaced JSON)"
        print(f"{year:>6} {len(files):>6} {tm:>13,} {tr:>10,} {tc:>15,}{verdict}")
        if sample:
            print(f"       sample: {sample}")


if __name__ == "__main__":
    main()
