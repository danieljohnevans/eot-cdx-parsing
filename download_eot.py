#!/usr/bin/env python3
"""Download EOT CDXJ index files from S3.

Supports two S3 layouts automatically:
  * 2004–2020: per-segment `.cdxj.gz` files directly listable under
    `crawl-data/EOT-YYYY/`, enumerated via list_objects_v2.
  * 2024+ (Common Crawl-style): the year's prefix only contains `*.paths.gz`
    manifest files (cdx.paths.gz holds ~1.2M S3 keys, one per .cdxj.gz file).
    We download the manifest, decompress, and use those keys.

Both paths produce identical local layout: `data/cdxj/EOT-YYYY/<basename>.cdxj.gz`.

Usage:
    python download_eot.py                  # downloads 2012 (default)
    python download_eot.py --years 2012 2016
    python download_eot.py --years all
    python download_eot.py --years 2024 --workers 64
"""

import argparse
import gzip
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

from config import AVAILABLE_YEARS, DATA_DIR, S3_BUCKET, S3_PREFIX, cdxj_dir


def _list_via_listobjects(s3, year: int) -> list[dict]:
    """Legacy 2004–2020 path: list_objects_v2 under the year prefix."""
    prefix = f"{S3_PREFIX}/EOT-{year}/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".cdxj.gz"):
                objects.append(obj)
    return objects


def _list_via_manifest(s3, manifest_key: str) -> list[dict]:
    """2024+ path: decompress cdx.paths.gz and return S3 keys.

    Manifests don't include per-file sizes — we return Size=None and skip the
    size-equality check in `download_year`.
    """
    body = s3.get_object(Bucket=S3_BUCKET, Key=manifest_key)["Body"].read()
    paths = gzip.decompress(body).decode("utf-8").splitlines()
    return [{"Key": p.strip(), "Size": None} for p in paths if p.strip()]


def list_cdxj_keys(s3, year: int) -> list[dict]:
    """List all .cdxj.gz S3 keys for a crawl year, auto-detecting layout."""
    manifest_key = f"{S3_PREFIX}/EOT-{year}/cdx.paths.gz"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=manifest_key)
        print(f"  Using manifest: s3://{S3_BUCKET}/{manifest_key}")
        return _list_via_manifest(s3, manifest_key)
    except ClientError as e:
        if e.response["Error"]["Code"] not in {"404", "NoSuchKey"}:
            raise
    # Fall back to direct list
    return _list_via_listobjects(s3, year)


def _download_one(s3, key: str, dest, pbar=None) -> int:
    """Download one S3 object. Returns bytes downloaded (0 on failure)."""
    try:
        s3.download_file(
            S3_BUCKET, key, str(dest),
            Callback=(lambda n: pbar.update(n)) if pbar else None,
        )
        return dest.stat().st_size
    except Exception as e:
        if dest.exists():
            dest.unlink()  # avoid leaving partial files
        if pbar is None:
            print(f"  ERROR downloading {key}: {e}")
        return 0


def download_year(s3, year: int, data_dir, workers: int = 32):
    """Download all CDXJ files for one crawl year, skipping existing."""
    out_dir = cdxj_dir(data_dir, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    objects = list_cdxj_keys(s3, year)
    if not objects:
        print(f"  No CDXJ files found for EOT-{year}")
        return

    # Existence/size-based skip. For manifest mode Size is None → only check existence.
    to_download = []
    skipped = 0
    for obj in objects:
        key = obj["Key"]
        filename = key.rsplit("/", 1)[-1]
        dest = out_dir / filename
        if dest.exists() and (obj["Size"] is None or dest.stat().st_size == obj["Size"]):
            skipped += 1
        else:
            to_download.append((obj, dest))

    has_sizes = any(o["Size"] is not None for o, _ in to_download)
    total_bytes = sum(o["Size"] for o, _ in to_download if o["Size"] is not None)
    print(f"  Found {len(objects):,} files, {len(to_download):,} to download"
          + (f" ({total_bytes / 1e9:.1f} GB)" if has_sizes and total_bytes else ""))
    if skipped:
        print(f"  Skipping {skipped:,} already-downloaded file(s)")

    if not to_download:
        return

    if has_sizes and len(to_download) <= 10_000:
        # Small enough for serial byte-level progress
        with tqdm(total=total_bytes, desc=f"  EOT-{year}", unit="B", unit_scale=True) as pbar:
            for obj, dest in to_download:
                _download_one(s3, obj["Key"], dest, pbar=pbar)
    else:
        # Manifest mode or huge: parallel downloads with per-file progress
        with tqdm(total=len(to_download), desc=f"  EOT-{year}", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [
                    ex.submit(_download_one, s3, obj["Key"], dest)
                    for obj, dest in to_download
                ]
                for fut in as_completed(futures):
                    pbar.update(1)


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
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Parallel download workers (used in manifest mode). Default: 32",
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
        download_year(s3, year, data_dir, workers=args.workers)

    print("Done.")


if __name__ == "__main__":
    main()
