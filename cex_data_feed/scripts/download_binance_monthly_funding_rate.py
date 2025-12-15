#!/usr/bin/env python3
"""
Downloader for Binance USDT-M Futures monthly funding rate ZIPs.

Files are under:
  https://data.binance.vision/data/futures/um/monthly/fundingRate/{SYMBOL}/{SYMBOL}-fundingRate-YYYY-MM.zip

Example:
  python scripts/download_binance_monthly_funding_rate.py \
    --symbol BTCUSDT \
    --start 2020-01 --end 2024-12 \
    --out ./data/funding_rate
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
import ssl
import certifi
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BASE_MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/fundingRate"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _http_request(url: str, *, method: str = "GET", timeout: float = 60.0):
    req = Request(url, method=method, headers={"User-Agent": USER_AGENT})
    context = ssl.create_default_context(cafile=certifi.where())
    return urlopen(req, timeout=timeout, context=context)


def _month_iter(start_yyyymm: str, end_yyyymm: Optional[str] = None) -> List[str]:
    """Generate inclusive list of months in YYYY-MM between start and end."""
    start_dt = datetime.strptime(start_yyyymm, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
    if end_yyyymm is None:
        now = datetime.now(timezone.utc)
        end_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end_dt = datetime.strptime(end_yyyymm, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
    if end_dt < start_dt:
        return []

    months: List[str] = []
    y, m = start_dt.year, start_dt.month
    while (y < end_dt.year) or (y == end_dt.year and m <= end_dt.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def build_monthly_url(symbol: str, yyyymm: str) -> str:
    return f"{BASE_MONTHLY_URL}/{symbol}/{symbol}-fundingRate-{yyyymm}.zip"


def url_exists(url: str, timeout: float = 30.0) -> Tuple[bool, Optional[int]]:
    try:
        with _http_request(url, method="HEAD", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            size = int(length) if length is not None else None
            return True, size
    except HTTPError as e:
        if getattr(e, "code", None) == 404:
            return False, None
    except URLError:
        pass
    try:
        with _http_request(url, method="GET", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            size = int(length) if length is not None else None
            return True, size
    except Exception:
        return False, None


def _get_remote_content_length(url: str, timeout: float = 60.0) -> Optional[int]:
    try:
        with _http_request(url, method="HEAD", timeout=timeout) as resp:
            length = resp.headers.get("Content-Length")
            if length is not None:
                return int(length)
    except HTTPError as e:
        print(f"[INFO] HEAD failed for {url}: {e}. Will GET instead.")
    except URLError as e:
        print(f"[WARN] HEAD connection error for {url}: {e}")
    except Exception as e:
        print(f"[WARN] HEAD unexpected error for {url}: {e}")
    return None


def _stream_download(url: str, dest_path: str, timeout: float = 120.0) -> None:
    tmp_path = dest_path + ".part"
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    bytes_written = 0
    start_time = time.time()
    try:
        with _http_request(url, method="GET", timeout=timeout) as resp:
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 512)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_written += len(chunk)
                    if bytes_written % (1024 * 1024 * 50) == 0:
                        elapsed = time.time() - start_time
                        mb = bytes_written / (1024 * 1024)
                        print(f"  downloaded ~{mb:.1f} MiB in {elapsed:.1f}s")
        os.replace(tmp_path, dest_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def download_if_needed(url: str, output_dir: str, timeout: float = 120.0) -> Tuple[str, str]:
    file_name = os.path.basename(urlparse(url).path)
    if not file_name:
        return ("failed", "")
    dest_path = os.path.join(output_dir, file_name)

    remote_size = _get_remote_content_length(url, timeout=timeout)
    if os.path.exists(dest_path):
        try:
            local_size = os.path.getsize(dest_path)
        except OSError:
            local_size = None
        if remote_size is not None and local_size == remote_size:
            return ("skipped", dest_path)
        else:
            print(f"[INFO] Re-downloading due to size mismatch or unknown size: {file_name}")
    try:
        _stream_download(url, dest_path, timeout=timeout)
        if remote_size is not None:
            try:
                if os.path.getsize(dest_path) != remote_size:
                    print(f"[WARN] Size mismatch after download for {file_name}")
            except OSError:
                pass
        return ("downloaded", dest_path)
    except (HTTPError, URLError) as e:
        print(f"[ERROR] Download failed for {url}: {e}")
        return ("failed", dest_path)
    except Exception as e:
        print(f"[ERROR] Unexpected error for {url}: {e}")
        traceback.print_exc()
        return ("failed", dest_path)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("Download Binance USDT-M monthly funding rate ZIPs."))
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--start", default="2020-01", help="Start month YYYY-MM (inclusive)")
    parser.add_argument("--end", default=None, help="End month YYYY-MM (inclusive); default: current month")
    parser.add_argument("--out", required=True, help="Output directory for downloaded ZIPs")
    parser.add_argument("--dry-run", action="store_true", help="Only list files to be downloaded; do not download.")
    parser.add_argument("--timeout", type=float, default=120.0, help="Network timeout per request in seconds (default: 120).")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    months = _month_iter(args.start, args.end)
    if not months:
        print("[ERROR] Empty month range. Check --start/--end.")
        return 2

    candidates = [build_monthly_url(args.symbol, m) for m in months]
    existing: List[Tuple[str, Optional[int]]] = []
    print(f"[INFO] Probing {len(candidates)} monthly files...")
    for i, url in enumerate(candidates, start=1):
        if i % 12 == 0: # Log roughly every year
            print(f"  probing {i}/{len(candidates)}...", end="\r")
        ok, size = url_exists(url, timeout=args.timeout)
        if ok:
            existing.append((url, size))
    print(f"  probing {len(candidates)}/{len(candidates)}... Done.")

    if not existing:
        print("[ERROR] No files found for the specified range.")
        return 2

    os.makedirs(args.out, exist_ok=True)

    if args.dry_run:
        for url, size in existing:
            if size is not None:
                print(f"{url}  size={size}")
            else:
                print(url)
        print(f"[INFO] {len(existing)} files available in range {months[0]}..{months[-1]}")
        return 0

    num_downloaded = 0
    num_skipped = 0
    num_failed = 0
    for idx, (url, _) in enumerate(existing, start=1):
        file_name = os.path.basename(urlparse(url).path)
        print(f"[{idx}/{len(existing)}] {file_name}")
        status, _ = download_if_needed(url, args.out, timeout=args.timeout)
        if status == "downloaded":
            num_downloaded += 1
        elif status == "skipped":
            num_skipped += 1
        else:
            num_failed += 1

    print(f"[INFO] Done. downloaded={num_downloaded}, skipped={num_skipped}, failed={num_failed}")
    return 0 if num_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
