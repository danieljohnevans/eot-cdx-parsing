#!/usr/bin/env python3
"""Download EOT parquet index files from S3.

These are the pre-built parquet indexes at s3://eotarchive/eot-index/table/eot-main/
and contain ALL crawled domains (not just the TARGET_DOMAINS).

Usage:
    python download_parquet.py                  # downloads all years
    python download_parquet.py --years 2012 2016
    python download_parquet.py --years 2012
"""

import argparse
import sys

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm

from config import AVAILABLE_YEARS, DATA_DIR, PARQUET_S3_PREFIX, S3_BUCKET, parquet_dir


def list_parquet_keys(s3, year: int) -> list[dict]:
    """List all .gz.parquet files for a given crawl year."""
    prefix = f"{PARQUET_S3_PREFIX}/crawl=EOT-{year}/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                objects.append(obj)
    return objects


def download_year(s3, year: int, data_dir):
    """Download all parquet files for one crawl year, skipping existing."""
    out_dir = parquet_dir(data_dir, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    objects = list_parquet_keys(s3, year)
    if not objects:
        print(f"  No parquet files found for EOT-{year}")
        return

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

    with tqdm(total=total_bytes, desc=f"  EOT-{year}", unit="B", unit_scale=True) as pbar:
        for obj, dest in to_download:
            s3.download_file(
                S3_BUCKET,
                obj["Key"],
                str(dest),
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )


def main():
    parser = argparse.ArgumentParser(description="Download EOT parquet index files from S3")
    parser.add_argument(
        "--years",
        nargs="+",
        default=["all"],
        help=f"Crawl years to download. Use 'all' for {AVAILABLE_YEARS}. Default: all",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DATA_DIR),
        help=f"Base directory for downloaded data. Default: {DATA_DIR}",
    )
    args = parser.parse_args()

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

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for year in years:
        print(f"Downloading EOT-{year} parquet files...")
        download_year(s3, year, data_dir)

    print("Done.")


if __name__ == "__main__":
    main()
