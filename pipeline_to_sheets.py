"""
pipeline_to_sheets.py

Automated pipeline:
1) Fetch IEMOP reserve market data (web scrape download) using download_iemop.fetch_iemop_data()
2) Clean/standardize (same logic used in your notebook)
3) Append only NEW rows to Google Sheets
4) Update metadata 'last_updated_utc'

Environment variables (set in GitHub Actions):
- GSHEET_ID: Google Sheet ID
- GCP_SA_JSON: Service account JSON (as a single JSON string)
"""

import os
import json
from datetime import datetime, timezone
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from download_iemop import fetch_iemop_data


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def connect_sheet():
    sheet_id = os.environ["GSHEET_ID"]
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh


def clean_iemop(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Match the cleaning/standardization you used in data2_processing.ipynb.
    Expects IEMOP columns that become lowercase:
    time_interval, region_name, commodity_type, resource_type, marginal_price, resource_name
    """
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df.columns = df.columns.str.lower()

    # Keep only the columns used in your lab notebook
    features = ["time_interval", "region_name", "commodity_type", "resource_type", "marginal_price", "resource_name"]
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in IEMOP data: {missing}")

    df = df[features]

    # Parse timestamps
    df["time_interval"] = pd.to_datetime(df["time_interval"], errors="coerce")

    # Map reserve codes to friendly names
    reserve_map = {
        "Dr": "Dispatchable",
        "Rd": "Regulating Down",
        "Ru": "Regulating Up",
        "Fr": "Contingency",
    }
    df["commodity_type"] = df["commodity_type"].map(reserve_map).fillna(df["commodity_type"])
    df["commodity_type"] = df["commodity_type"].astype(str).str.title()
    df["resource_type"] = df["resource_type"].astype(str).str.title()

    # Map region codes
    region_map = {"CLUZ": "Luzon", "CVIS": "Visayas", "CMIN": "Mindanao"}
    df["region_name"] = df["region_name"].map(region_map).fillna(df["region_name"])

    # Battery flag
    df["is_battery"] = df["resource_name"].astype(str).str.contains("_BAT", case=False, na=False)

    # Deduplicate + drop bad rows
    df = df.sort_values(by=["time_interval", "region_name"]).reset_index(drop=True)
    df = df.dropna(subset=["time_interval", "resource_name", "commodity_type", "marginal_price"])
    df = df.drop_duplicates(subset=["time_interval", "resource_name", "commodity_type"])

    # Add metadata fields for dashboard + traceability
    df["source"] = "IEMOP"
    df["source_url"] = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Order columns (match the template you can upload to Google Sheets)
    out_cols = [
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
    df = df[out_cols]

    return df


def read_existing_keys(ws):
    """
    Build a set of keys already in the sheet so we append only new rows.
    Primary key: (time_interval, resource_name, commodity_type)
    """
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return set(), None, None

    header = values[0]
    idx = {name: i for i, name in enumerate(header)}

    needed = ["time_interval", "resource_name", "commodity_type"]
    if not all(n in idx for n in needed):
        raise ValueError(f"Sheet headers must include: {needed}. Found: {header}")

    keyset = set()
    max_time = None

    for row in values[1:]:
        try:
            t = row[idx["time_interval"]].strip()
            rname = row[idx["resource_name"]].strip()
            ctype = row[idx["commodity_type"]].strip()
            if not (t and rname and ctype):
                continue
            keyset.add((t, rname, ctype))
            if max_time is None or t > max_time:
                max_time = t
        except Exception:
            continue

    return keyset, header, max_time


def append_new_rows(ws, header, df_new: pd.DataFrame, existing_keys: set) -> int:
    """
    Append only rows not already in existing_keys.
    """
    if df_new.empty:
        return 0

    df_new = df_new.copy()
    df_new["time_interval"] = df_new["time_interval"].dt.strftime("%Y-%m-%d %H:%M:%S")

    rows_to_append = []
    for _, r in df_new.iterrows():
        key = (r["time_interval"], str(r["resource_name"]), str(r["commodity_type"]))
        if key in existing_keys:
            continue
        rows_to_append.append([r.get(col, "") for col in header])

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    return len(rows_to_append)


def update_last_updated(sh):
    ws_meta = sh.worksheet("metadata")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        colA = ws_meta.col_values(1)
        if "last_updated_utc" in colA:
            row_idx = colA.index("last_updated_utc") + 1
            ws_meta.update(f"B{row_idx}", ts)
        else:
            ws_meta.update("B1", ts)
    except Exception:
        pass


def main():
    sh = connect_sheet()
    ws = sh.worksheet("data")

    existing_keys, header, max_time = read_existing_keys(ws)

    if header is None:
        header = [
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
        ws.append_row(header, value_input_option="USER_ENTERED")
        existing_keys = set()

    df_raw = fetch_iemop_data(max_days=30, missing_limit=10, verbose=False)
    df_clean = clean_iemop(df_raw)

    added = append_new_rows(ws, header, df_clean, existing_keys)
    update_last_updated(sh)

    print(f"Fetched rows: {len(df_raw):,} | Clean rows: {len(df_clean):,} | Appended new rows: {added:,}")
    if max_time:
        print(f"Sheet latest time_interval before run: {max_time}")


if __name__ == "__main__":
    main()
