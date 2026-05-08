"""
pipeline_to_sheets.py

Automated pipeline:
1) Fetch IEMOP reserve market data using download_iemop.fetch_iemop_data()
2) Clean/standardize
3) Append only NEW rows to Google Sheets (based on max_time_interval)
4) Update metadata timestamps (UTC + PHT) and max_time_interval

Environment variables (set in GitHub Actions):
- GSHEET_ID: Google Sheet ID
- GCP_SA_JSON: Service account JSON (as a single JSON string)
"""

import os
import json
from datetime import datetime, timezone, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from download_iemop import fetch_iemop_data

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DATA_HEADERS = [
    "time_interval",
    "region_name",
    "commodity_type",
    "resource_type",
    "resource_name",
    "marginal_price",
    "is_battery",
    "source",
    "source_url",
    "ingested_at_utc",
]


def connect_sheet():
    sheet_id = os.environ["GSHEET_ID"]
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def get_metadata_map(ws_meta):
    """
    Reads metadata tab where Column A = field, Column B = value
    Returns dict like {"last_updated_utc": "...", "max_time_interval": "..."}
    """
    vals = ws_meta.get_all_values()
    meta = {}
    for r in vals[1:] if vals and vals[0] and vals[0][0].strip().lower() == "field" else vals:
        if len(r) >= 2:
            k = r[0].strip()
            v = r[1].strip()
            if k:
                meta[k] = v
    return meta


def upsert_metadata(ws_meta, label, value):
    colA = [x.strip() for x in ws_meta.col_values(1)]
    if label in colA:
        row_idx = colA.index(label) + 1
        ws_meta.update(range_name=f"B{row_idx}", values=[[value]])
    else:
        next_row = len(colA) + 1
        ws_meta.update(range_name=f"A{next_row}", values=[[label]])
        ws_meta.update(range_name=f"B{next_row}", values=[[value]])


def update_last_updated_and_max_time(sh, max_time_interval_str=None):
    ws_meta = sh.worksheet("metadata")

    utc_ts = datetime.now(timezone.utc)
    pht_tz = timezone(timedelta(hours=8))
    pht_ts = utc_ts.astimezone(pht_tz)

    utc_str = utc_ts.strftime("%Y-%m-%d %H:%M:%S")
    pht_str = pht_ts.strftime("%Y-%m-%d %H:%M:%S")

    upsert_metadata(ws_meta, "last_updated_utc", utc_str)
    upsert_metadata(ws_meta, "last_updated_pht", pht_str)

    if max_time_interval_str:
        upsert_metadata(ws_meta, "max_time_interval", max_time_interval_str)

    print(f"Updated last_updated_utc = {utc_str}")
    print(f"Updated last_updated_pht = {pht_str}")
    if max_time_interval_str:
        print(f"Updated max_time_interval = {max_time_interval_str}")


def ensure_data_headers(ws_data):
    current = ws_data.row_values(1)
    if not current or all(x.strip() == "" for x in current):
        ws_data.append_row(DATA_HEADERS, value_input_option="USER_ENTERED")
        return DATA_HEADERS
    return current


def parse_time_interval(series: pd.Series) -> pd.Series:
    """
    Avoid pandas warning by trying strict known format first,
    then fallback to general parsing for any remaining.
    """
    s = series.astype(str).str.strip()

    # Try strict format (common)
    dt1 = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    # Fallback parse for entries that failed
    mask = dt1.isna() & s.ne("")
    if mask.any():
        dt2 = pd.to_datetime(s[mask], errors="coerce")
        dt1.loc[mask] = dt2

    return dt1


def clean_iemop(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df.columns = df.columns.str.lower()

    features = ["time_interval", "region_name", "commodity_type", "resource_type", "marginal_price", "resource_name"]
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in IEMOP data: {missing}")

    df = df[features]

    # Parse timestamps safely
    df["time_interval"] = parse_time_interval(df["time_interval"])

    reserve_map = {"Dr": "Dispatchable", "Rd": "Regulating Down", "Ru": "Regulating Up", "Fr": "Contingency"}
    df["commodity_type"] = df["commodity_type"].map(reserve_map).fillna(df["commodity_type"])
    df["commodity_type"] = df["commodity_type"].astype(str).str.title()
    df["resource_type"] = df["resource_type"].astype(str).str.title()

    region_map = {"CLUZ": "Luzon", "CVIS": "Visayas", "CMIN": "Mindanao"}
    df["region_name"] = df["region_name"].map(region_map).fillna(df["region_name"])

    df["is_battery"] = df["resource_name"].astype(str).str.contains("_BAT", case=False, na=False)

    df = df.dropna(subset=["time_interval", "resource_name", "commodity_type", "marginal_price"])
    df = df.drop_duplicates(subset=["time_interval", "resource_name", "commodity_type"])
    df = df.sort_values(by=["time_interval", "region_name"]).reset_index(drop=True)

    df["source"] = "IEMOP"
    df["source_url"] = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df[DATA_HEADERS]
    return df


def fetch_incremental(last_time_str: str | None):
    """
    Fetch only recent data if we already have a max_time_interval.
    Falls back safely if fetch_iemop_data signature doesn't support start/end.
    """
    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if pd.isna(last_dt):
            last_time_str = None
        else:
            # 2-day overlap buffer
            start_date = (last_dt - pd.Timedelta(days=2)).strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            try:
                return fetch_iemop_data(start_date=start_date, end_date=end_date, verbose=False)
            except TypeError:
                # If your fetch_iemop_data doesn't accept start_date/end_date
                return fetch_iemop_data(max_days=7, missing_limit=10, verbose=False)

    # First run or bad metadata
    return fetch_iemop_data(max_days=7, missing_limit=10, verbose=False)


def append_rows_chunked(ws, rows, chunk_size=500):
    """
    Append in chunks to avoid request size limits.
    """
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i:i + chunk_size]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        total += len(batch)
    return total


def main():
    sh = connect_sheet()
    ws_data = sh.worksheet("data")
    ws_meta = sh.worksheet("metadata")

    headers = ensure_data_headers(ws_data)

    meta = get_metadata_map(ws_meta)
    last_time_str = meta.get("max_time_interval", "").strip() or None

    df_raw = fetch_incremental(last_time_str)
    df_clean = clean_iemop(df_raw)

    # Filter strictly newer than max_time_interval to avoid re-appending history
    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if not pd.isna(last_dt):
            df_clean = df_clean[df_clean["time_interval"] > last_dt].copy()

    if df_clean.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=last_time_str)
        print("No new rows to append.")
        return

    # Safety cap to prevent huge accidental appends
    # Keeps the newest rows only if something goes wrong again.
    MAX_APPEND = 15000
    if len(df_clean) > MAX_APPEND:
        df_clean = df_clean.tail(MAX_APPEND).copy()
        print(f"Warning: limiting append to the newest {MAX_APPEND} rows to prevent sheet overflow.")

    # Convert to sheet row format
    df_clean["time_interval"] = df_clean["time_interval"].dt.strftime("%Y-%m-%d %H:%M:%S")
    rows_to_append = [[r.get(col, "") for col in headers] for _, r in df_clean.iterrows()]

    appended = append_rows_chunked(ws_data, rows_to_append, chunk_size=500)

    # Update metadata max_time_interval to the newest time_interval appended
    new_max_time = df_clean["time_interval"].max()
    update_last_updated_and_max_time(sh, max_time_interval_str=new_max_time)

    print(f"Fetched rows: {len(df_raw):,} | Clean new rows: {len(df_clean):,} | Appended: {appended:,}")


if __name__ == "__main__":
    main()
