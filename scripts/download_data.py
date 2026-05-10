from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Iterable

import requests
from requests import HTTPError, RequestException

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Default range is intentionally capped at 2018-07 because the official TLC
# CloudFront endpoint may return HTTP 403 for some historical monthly files,
# for example yellow_tripdata_2018-08.parquet. The already downloaded 2014-01
# to 2018-07 range is enough for the class project and avoids blocking the
# whole pipeline on a single unavailable source object.
START_YEAR = int(os.getenv("NYC_TAXI_START_YEAR", "2014"))
START_MONTH = int(os.getenv("NYC_TAXI_START_MONTH", "1"))
END_YEAR = int(os.getenv("NYC_TAXI_END_YEAR", "2018"))
END_MONTH = int(os.getenv("NYC_TAXI_END_MONTH", "7"))

YELLOW_DIR = Path("data/raw/yellow")
LOOKUP_DIR = Path("data/raw/lookups")
STATE_DIR = Path("state")
MANIFEST_PATH = STATE_DIR / "download_manifest.json"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def generate_months(start_year: int, start_month: int, end_year: int, end_month: int) -> list[str]:
    months: list[str] = []
    year = start_year
    month = start_month

    while (year < end_year) or (year == end_year and month <= end_month):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month == 13:
            month = 1
            year += 1

    return months


def existing_parquet_files() -> list[Path]:
    return sorted(YELLOW_DIR.glob("yellow_tripdata_*.parquet"))


def write_manifest(successful: Iterable[str], skipped_existing: Iterable[str], failed: list[dict]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "configured_range": {
            "start": f"{START_YEAR}-{START_MONTH:02d}",
            "end": f"{END_YEAR}-{END_MONTH:02d}",
        },
        "successful_downloads": sorted(set(successful)),
        "skipped_existing_files": sorted(set(skipped_existing)),
        "failed_downloads": failed,
        "available_raw_yellow_files": [str(path) for path in existing_parquet_files()],
        "generated_at_epoch": int(time.time()),
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def download_file(url: str, target: Path, *, required: bool = False, max_attempts: int = 3) -> str:
    """Download one file.

    Returns one of: downloaded, skipped_existing, skipped_failed.
    A failed optional file does not stop the pipeline. A failed required file does.
    """
    if target.exists() and target.stat().st_size > 0:
        print(f"[skip] {target}")
        return "skipped_existing"

    target.parent.mkdir(parents=True, exist_ok=True)
    part_file = target.with_suffix(target.suffix + ".part")

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[download] {url} (attempt {attempt}/{max_attempts})")
            with requests.get(url, stream=True, timeout=120, headers=REQUEST_HEADERS) as response:
                response.raise_for_status()
                with open(part_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            if part_file.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty.")

            part_file.replace(target)
            return "downloaded"

        except (HTTPError, RequestException, RuntimeError) as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if part_file.exists():
                part_file.unlink()

            # 403 and 404 are treated as unavailable source objects. Retrying them
            # usually does not help, so skip faster after the first attempt.
            if status_code in {403, 404}:
                message = f"HTTP {status_code}: {url}"
                if required:
                    raise RuntimeError(f"Required source file unavailable: {message}") from exc
                print(f"[warn] skipping unavailable optional file: {message}")
                return "skipped_failed"

            if attempt == max_attempts:
                if required:
                    raise RuntimeError(f"Required source file failed after retries: {url}") from exc
                print(f"[warn] skipping file after retries: {url} ({exc})")
                return "skipped_failed"

            sleep_seconds = 2 ** attempt
            print(f"[warn] download failed, retrying in {sleep_seconds}s: {exc}")
            time.sleep(sleep_seconds)

    return "skipped_failed"


def main() -> None:
    YELLOW_DIR.mkdir(parents=True, exist_ok=True)
    LOOKUP_DIR.mkdir(parents=True, exist_ok=True)

    months = generate_months(START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    successful: list[str] = []
    skipped_existing: list[str] = []
    failed: list[dict] = []

    # Lookup is required because Gold joins borough names from this reference table.
    lookup_status = download_file(ZONES_URL, LOOKUP_DIR / "taxi_zone_lookup.csv", required=True)
    if lookup_status == "downloaded":
        successful.append(str(LOOKUP_DIR / "taxi_zone_lookup.csv"))
    elif lookup_status == "skipped_existing":
        skipped_existing.append(str(LOOKUP_DIR / "taxi_zone_lookup.csv"))

    for month in months:
        url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
        target = YELLOW_DIR / f"yellow_tripdata_{month}.parquet"
        status = download_file(url, target, required=False)

        if status == "downloaded":
            successful.append(str(target))
        elif status == "skipped_existing":
            skipped_existing.append(str(target))
        else:
            failed.append({"month": month, "url": url, "target": str(target)})

    available_files = existing_parquet_files()
    write_manifest(successful, skipped_existing, failed)

    print("\n=== DOWNLOAD SUMMARY ===")
    print(f"Available Yellow Taxi parquet files: {len(available_files)}")
    print(f"Newly downloaded: {len(successful)}")
    print(f"Skipped existing: {len(skipped_existing)}")
    print(f"Skipped unavailable/failed: {len(failed)}")
    print(f"Manifest: {MANIFEST_PATH}")

    if not available_files:
        raise RuntimeError(
            "No Yellow Taxi parquet files are available in data/raw/yellow. "
            "Check your internet connection or set a smaller valid range, for example: "
            "NYC_TAXI_START_YEAR=2018 NYC_TAXI_START_MONTH=1 "
            "NYC_TAXI_END_YEAR=2018 NYC_TAXI_END_MONTH=7 python scripts/download_data.py"
        )

    print("Done.")


if __name__ == "__main__":
    main()
