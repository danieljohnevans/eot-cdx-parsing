#!/usr/bin/env python3
"""Download EOT CDXJ index files from S3.

Usage:
    python download_eot.py                  # downloads 2012 (default)
    python download_eot.py --years 2012 2016
    python download_eot.py --years all
"""

import argparse
import sys

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm

from config import AVAILABLE_YEARS, DATA_DIR, S3_BUCKET, S3_PREFIX, cdxj_dir


def list_cdxj_keys(s3, year: int) -> list[dict]:
    """List all .cdxj.gz files for a given crawl year."""
    prefix = f"{S3_PREFIX}/EOT-{year}/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".cdxj.gz"):
                objects.append(obj)
    return objects


def download_year(s3, year: int, data_dir):
    """Download all CDXJ files for one crawl year, skipping existing."""
    out_dir = cdxj_dir(data_dir, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    objects = list_cdxj_keys(s3, year)
    if not objects:
        print(f"  No CDXJ files found for EOT-{year}")
        return

    # Figure out how much we actually need to download (skip already-done files)
    to_download = []
    skipped = 0
    for obj in objects:
        key = obj["Key"]
        filename = key.rsplit("/", 1)[-1]
        dest = out_dir / filename
        if dest.exists() and dest.stat().st_size == obj["Size"]:
            skipped += 1
        else:
            to_download.append((obj, dest))

    total_bytes = sum(o["Size"] for o, _ in to_download)
    print(f"  Found {len(objects)} files, {len(to_download)} to download ({total_bytes / 1e9:.1f} GB)")
    if skipped:
        print(f"  Skipping {skipped} already-downloaded file(s)")

    if not to_download:
        return

    # Single byte-level progress bar across all files
    with tqdm(total=total_bytes, desc=f"  EOT-{year}", unit="B", unit_scale=True) as pbar:
        for obj, dest in to_download:
            s3.download_file(
                S3_BUCKET,
                obj["Key"],
                str(dest),
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )


def main():
    parser = argparse.ArgumentParser(description="Download EOT CDXJ index files from S3")
    parser.add_argument(
        "--years",
        nargs="+",
        default=["2012"],
        help=f"Crawl years to download. Use 'all' for {AVAILABLE_YEARS}. Default: 2012",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DATA_DIR),
        help=f"Base directory for downloaded data. Default: {DATA_DIR}",
    )
    args = parser.parse_args()

    # Resolve years
    if args.years == ["all"]:
        years = AVAILABLE_YEARS
    else:
        years = []
        for y in args.years:
            yr = int(y)
            if yr not in AVAILABLE_YEARS:
                print(f"Error: {yr} not in available years {AVAILABLE_YEARS}", file=sys.stderr)
                sys.exit(1)
            years.append(yr)

    data_dir = __import__("pathlib").Path(args.data_dir)

    # Anonymous S3 client (no credentials needed)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for year in years:
        print(f"Downloading EOT-{year} CDXJ files...")
        download_year(s3, year, data_dir)

    print("Done.")


if __name__ == "__main__":
    main()
