"""
download_iemop.py

Hardened IEMOP Reserve Market Clearing Price (RTD) downloader.

What this version improves:
- Explicit logging for base-path detection and fallback behavior
- Stronger validation that the downloaded file is actually a usable CSV
- Safer exception handling with optional verbose logs
- Compatible with pipeline_to_sheets.py calls:
    fetch_iemop_data(start_date=..., end_date=..., max_days=..., missing_limit=..., verbose=...)

Running this file directly saves iemop_combined.csv for local testing.
"""

import base64
import io
import re
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import requests

BASE_URL = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; IEMOP-Downloader/1.0)"
}

# The page currently does not expose md_file dynamically in a reliable way,
# so we still keep known candidate directories and test them directly.
BASE_PATH_CANDIDATES = [
    "/var/www/html/wp-content/uploads/downloads/data/MPRESERVE/",
]

EXPECTED_COLUMNS = {
    "time_interval",
    "region_name",
    "commodity_type",
    "resource_type",
    "marginal_price",
    "resource_name",
}


def _log(msg: str, verbose: bool) -> None:
    if verbose:
        print(msg)


def _safe_get(url: str, timeout: int = 20) -> requests.Response:
    res = requests.get(url, headers=HEADERS, timeout=timeout)
    res.raise_for_status()
    return res


def _try_extract_md_file_path(page_source: str) -> Optional[str]:
    """
    Try to extract a base path from an md_file query parameter in page HTML.
    Returns the directory path if found, else None.
    """
    match = re.search(r"md_file=([A-Za-z0-9+/=]+)", page_source)
    if not match:
        return None

    try:
        decoded_path = base64.b64decode(match.group(1)).decode()
        return decoded_path.rsplit("/", 1)[0] + "/"
    except Exception:
        return None


def _looks_like_csv(content: bytes) -> bool:
    if not content or len(content) < 50:
        return False

    head = content[:500].decode("utf-8", errors="ignore").lower()

    # Reject obvious HTML/error pages
    if "<html" in head or "<!doctype" in head or "<body" in head:
        return False

    # Must at least look comma-separated
    return "," in head


def _parse_csv_bytes(content: bytes) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        return None

    if df is None or df.empty:
        return None

    normalized_cols = {str(c).strip().lower() for c in df.columns}
    if not EXPECTED_COLUMNS.issubset(normalized_cols):
        return None

    return df


def _download_csv_from_base_path(date_obj: datetime, base_path: str, verbose: bool = False) -> Optional[pd.DataFrame]:
    """
    Download one day's CSV using a specific base path.
    Returns DataFrame if valid, else None.
    """
    date_str = date_obj.strftime("%Y%m%d")
    server_filename = f"{base_path}MP_RESERVE_{date_str}.csv"
    encoded = base64.b64encode(server_filename.encode()).decode()
    url = f"{BASE_URL}?md_file={encoded}"

    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
    except Exception as e:
        _log(f"[ERROR] Request failed for {date_str}: {e}", verbose)
        return None

    if res.status_code != 200:
        _log(f"[MISS] {date_str}: HTTP {res.status_code}", verbose)
        return None

    if not _looks_like_csv(res.content):
        _log(f"[MISS] {date_str}: response is not valid CSV-like content", verbose)
        return None

    df = _parse_csv_bytes(res.content)
    if df is None:
        _log(f"[MISS] {date_str}: CSV parsed but expected columns were not found", verbose)
        return None

    _log(f"[OK] {date_str}: downloaded via {base_path}", verbose)
    return df


def _detect_base_path(verbose: bool = False) -> str:
    """
    Try to detect the real base path from the page.
    If that fails, test known candidate paths against a recent date and use the first one that works.
    """
    _log("[INFO] Detecting IEMOP base path...", verbose)

    try:
        page_source = _safe_get(BASE_URL).text
        detected = _try_extract_md_file_path(page_source)
        if detected:
            _log(f"[INFO] Found md_file-derived base path: {detected}", verbose)

            # Validate detected path using recent dates
            today = datetime.today()
            for offset in range(0, 7):
                test_date = today - timedelta(days=offset)
                test_df = _download_csv_from_base_path(test_date, detected, verbose=False)
                if test_df is not None:
                    _log(f"[INFO] md_file-derived path validated using {test_date.strftime('%Y-%m-%d')}", verbose)
                    return detected

            _log("[WARN] md_file-derived path was found but could not be validated", verbose)
        else:
            _log("[WARN] No md_file parameter found on page HTML", verbose)

    except Exception as e:
        _log(f"[WARN] Failed to inspect IEMOP page for md_file: {e}", verbose)

    _log("[INFO] Testing fallback base-path candidates...", verbose)

    today = datetime.today()
    for candidate in BASE_PATH_CANDIDATES:
        for offset in range(0, 10):
            test_date = today - timedelta(days=offset)
            test_df = _download_csv_from_base_path(test_date, candidate, verbose=False)
            if test_df is not None:
                _log(
                    f"[INFO] Using fallback base path: {candidate} "
                    f"(validated with {test_date.strftime('%Y-%m-%d')})",
                    verbose,
                )
                return candidate

    raise RuntimeError(
        "Could not validate any IEMOP base path. "
        "The source path may have changed, or the site may be unavailable."
    )


def _fetch_one_day_csv(date_obj: datetime, base_path: str, verbose: bool = False) -> Optional[pd.DataFrame]:
    """
    Wrapper used by the main fetch loop.
    """
    return _download_csv_from_base_path(date_obj, base_path, verbose=verbose)


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
    - end_date: "YYYY-MM-DD" (optional)
    - max_days: how far back to scan when start/end not provided
    - missing_limit: stop after N consecutive missing days for open-ended scan
    - verbose: print progress logs

    Returns
    - pandas DataFrame (possibly empty)
    """
    base_path = _detect_base_path(verbose=verbose)

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

    all_dfs: List[pd.DataFrame] = []
    missing_count = 0
    d = current_date

    _log(
        f"[INFO] Fetching IEMOP data from {earliest_date.strftime('%Y-%m-%d')} "
        f"to {current_date.strftime('%Y-%m-%d')}",
        verbose,
    )

    while d >= earliest_date:
        df_day = _fetch_one_day_csv(d, base_path, verbose=verbose)

        if df_day is not None and not df_day.empty:
            all_dfs.append(df_day)
            missing_count = 0
            _log(f"[FOUND] MP_RESERVE_{d.strftime('%Y%m%d')}.csv", verbose)
        else:
            missing_count += 1
            _log(f"[MISSING] MP_RESERVE_{d.strftime('%Y%m%d')}.csv", verbose)

            if (not use_fixed_range) and (missing_count >= missing_limit):
                _log(
                    f"[INFO] Stopping early after {missing_limit} consecutive missing days.",
                    verbose,
                )
                break

        d -= timedelta(days=1)

    if not all_dfs:
        _log("[INFO] No IEMOP files found for the requested period.", verbose)
        return pd.DataFrame()

    # Reverse so output runs oldest -> latest
    combined = pd.concat(reversed(all_dfs), ignore_index=True)

    # Final light cleanup
    combined.columns = [str(c).strip().lower() for c in combined.columns]

    # Standardize time parsing if present
    if "time_interval" in combined.columns:
        combined["time_interval"] = pd.to_datetime(combined["time_interval"], errors="coerce")

    _log(f"[INFO] Combined rows: {len(combined):,}", verbose)
    return combined


if __name__ == "__main__":
    try:
        df = fetch_iemop_data(max_days=30, missing_limit=10, verbose=True)

        if df.empty:
            print("No data found.")
        else:
            df.to_csv("iemop_combined.csv", index=False)
            print(f"\nSaved iemop_combined.csv with {len(df):,} rows.")
    except Exception as e:
        print(f"Downloader failed: {e}")
        raise
