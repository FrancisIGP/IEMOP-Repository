"""
download_iemop.py

IEMOP Reserve Market Clearing Price (RTD) downloader.

- fetch_iemop_data() returns a pandas DataFrame (for pipeline use)
- running this file directly saves iemop_combined.csv (for local testing)

This script is designed for a free, automated pipeline:
GitHub Actions (cron) -> Python -> Google Sheets -> Tableau Public
"""

import base64
import io
import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

BASE_URL = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def _detect_base_path() -> str:
    """
    Detect the server file path by scraping the IEMOP page for an md_file param,
    decoding it, and extracting the directory path. Falls back to a known path.
    """
    try:
        page_source = requests.get(BASE_URL, headers=HEADERS, timeout=15).text
        match = re.search(r"md_file=([A-Za-z0-9+/=]+)", page_source)
        if not match:
            raise RuntimeError("No md_file link found")

        decoded_path = base64.b64decode(match.group(1)).decode()
        base_path = decoded_path.rsplit("/", 1)[0] + "/"
        return base_path
    except Exception:
        # Fallback path used in your earlier working script
        return "/var/www/html/wp-content/uploads/downloads/data/MPRESERVE/"


def _fetch_one_day_csv(date_obj: datetime, base_path: str) -> Optional[pd.DataFrame]:
    """
    Download one day's CSV (MP_RESERVE_YYYYMMDD.csv). Returns DataFrame if found, else None.
    """
    date_str = date_obj.strftime("%Y%m%d")
    filename = f"{base_path}MP_RESERVE_{date_str}.csv"
    encoded = base64.b64encode(filename.encode()).decode()

    try:
        res = requests.get(f"{BASE_URL}?md_file={encoded}", headers=HEADERS, timeout=15)
        if res.status_code == 200 and len(res.content) > 300:
            return pd.read_csv(io.BytesIO(res.content))
    except Exception:
        return None
    return None


def fetch_iemop_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_days: int = 30,
    missing_limit: int = 10,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Fetch IEMOP RTD reserve market CSVs and return a combined DataFrame.

    If start_date/end_date are provided (YYYY-MM-DD), the function attempts that exact range.
    If not provided, it scans backward from today up to max_days and stops early if it hits
    missing_limit consecutive missing days.

    Parameters
    - start_date: "YYYY-MM-DD" (optional)
    - end_date:   "YYYY-MM-DD" (optional)
    - max_days:   how far back to scan when start/end not provided
    - missing_limit: stop after N consecutive missing days (for open-ended scan)
    - verbose: print progress logs

    Returns
    - pandas DataFrame (possibly empty)
    """
    base_path = _detect_base_path()

    if end_date:
        current_date = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        current_date = datetime.today()

    if start_date:
        earliest_date = datetime.strptime(start_date, "%Y-%m-%d")
        use_fixed_range = True
    else:
        earliest_date = current_date - timedelta(days=max_days - 1)
        use_fixed_range = False

    all_dfs = []
    missing_count = 0

    d = current_date
    while d >= earliest_date:
        df_day = _fetch_one_day_csv(d, base_path)
        if df_day is not None and not df_day.empty:
            all_dfs.append(df_day)
            missing_count = 0
            if verbose:
                print(f"Found: MP_RESERVE_{d.strftime('%Y%m%d')}.csv")
        else:
            missing_count += 1
            if verbose:
                print(f"Missing: MP_RESERVE_{d.strftime('%Y%m%d')}.csv")

        # For open-ended scan only, stop early if we keep missing files
        if (not use_fixed_range) and (missing_count >= missing_limit):
            if verbose:
                print(f"Stopping early after {missing_limit} consecutive missing days.")
            break

        d -= timedelta(days=1)

    if not all_dfs:
        return pd.DataFrame()

    # reverse so Oldest -> Latest
    combined = pd.concat(reversed(all_dfs), ignore_index=True)
    return combined


if __name__ == "__main__":
    # Local test: scan recent days and write a combined CSV
    df = fetch_iemop_data(max_days=30, missing_limit=10, verbose=True)
    if df.empty:
        print("No data found.")
    else:
        df.to_csv("iemop_combined.csv", index=False)
        print(f"\nSaved iemop_combined.csv with {len(df):,} rows.")
