"""
pipeline_to_sheets.py

Automated pipeline:
1) Fetch IEMOP reserve market data using download_iemop.fetch_iemop_data()
2) Clean / standardize
3) Enrich resource metadata from a reference Google Sheet
4) Replace resource_name with a human-readable label
5) Preserve raw_resource_name for joins / uniqueness
6) Append only NEW rows to Google Sheets (based on max_time_interval)
7) Update metadata timestamps (UTC + PHT) and max_time_interval

Required environment variables:
- GSHEET_ID: destination Google Sheet ID
- GCP_SA_JSON: service account JSON as a single JSON string

Optional environment variables:
- REFERENCE_SHEET_ID: reference Google Sheet ID
- REFERENCE_GID: worksheet gid inside the reference Google Sheet
"""

import os
import json
import re
from datetime import datetime, timezone, timedelta

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from download_iemop import fetch_iemop_data

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Defaults taken from the reference sheet link you shared
DEFAULT_REFERENCE_SHEET_ID = "1QtR6jz0-s8tYHdG2s-sKxzBzj0eMS3SAPmQW_bG-5Zo"
DEFAULT_REFERENCE_GID = "1046991677"

DATA_HEADERS = [
    "time_interval",
    "region_name",
    "commodity_type",
    "resource_type",
    "raw_resource_name",
    "resource_name",      # human-readable label
    "plant_name",
    "fuel_type",
    "asset_kind",         # unit / generator / battery
    "marginal_price",
    "is_battery",
    "source",
    "source_url",
    "ingested_at_utc",
]


def connect_gspread():
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)


def connect_sheet(gc, sheet_id: str):
    return gc.open_by_key(sheet_id)


def worksheet_by_gid(spreadsheet, gid: str):
    gid_int = int(gid)
    for ws in spreadsheet.worksheets():
        ws_gid = getattr(ws, "id", None)
        if ws_gid is None:
            ws_gid = ws._properties.get("sheetId")
        if int(ws_gid) == gid_int:
            return ws
    raise ValueError(f"Worksheet with gid={gid} not found in spreadsheet {spreadsheet.id}")


def get_metadata_map(ws_meta):
    vals = ws_meta.get_all_values()
    meta = {}
    if not vals:
        return meta

    start_idx = 1 if len(vals[0]) >= 2 and vals[0][0].strip().lower() == "field" else 0
    for r in vals[start_idx:]:
        if len(r) >= 2:
            k = str(r[0]).strip()
            v = str(r[1]).strip()
            if k:
                meta[k] = v
    return meta


def upsert_metadata(ws_meta, label, value):
    col_a = [str(x).strip() for x in ws_meta.col_values(1)]
    if label in col_a:
        row_idx = col_a.index(label) + 1
        ws_meta.update(range_name=f"B{row_idx}", values=[[value]])
    else:
        next_row = len(col_a) + 1
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


def sync_data_headers(ws_data):
    current = ws_data.row_values(1)

    if not current or all(str(x).strip() == "" for x in current):
        ws_data.update("A1", [DATA_HEADERS])
        return DATA_HEADERS

    if current != DATA_HEADERS:
        ws_data.update("A1", [DATA_HEADERS])
        print("Updated data sheet headers to the new schema.")

    return DATA_HEADERS


def parse_time_interval(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    dt1 = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")

    mask = dt1.isna() & s.ne("")
    if mask.any():
        dt2 = pd.to_datetime(s[mask], errors="coerce")
        dt1.loc[mask] = dt2

    return dt1


def normalize_key(x: str) -> str:
    if pd.isna(x):
        return ""
    x = str(x).strip().upper()
    x = re.sub(r"[^A-Z0-9]+", "", x)
    return x


def find_column(df: pd.DataFrame, aliases):
    normalized_map = {
        re.sub(r"[^a-z0-9]+", "", str(c).lower()): c
        for c in df.columns
    }

    for alias in aliases:
        key = re.sub(r"[^a-z0-9]+", "", alias.lower())
        if key in normalized_map:
            return normalized_map[key]

    return None


def infer_asset_kind(raw_resource_name, resource_type, fuel_type, existing_kind=None):
    if pd.notna(existing_kind) and str(existing_kind).strip():
        val = str(existing_kind).strip().lower()
        if "battery" in val or "bess" in val:
            return "battery"
        if "generator" in val or val == "gen":
            return "generator"
        if "unit" in val:
            return "unit"
        return str(existing_kind).strip()

    raw = str(raw_resource_name or "").lower()
    rtype = str(resource_type or "").lower()
    fuel = str(fuel_type or "").lower()

    if "_bat" in raw or "battery" in raw or "bess" in raw or "battery" in fuel or "bess" in fuel:
        return "battery"

    if "generator" in rtype or rtype == "gen":
        return "generator"

    if "unit" in rtype:
        return "unit"

    return "generator"


def load_reference_mapping(gc) -> pd.DataFrame:
    ref_sheet_id = os.environ.get("REFERENCE_SHEET_ID", DEFAULT_REFERENCE_SHEET_ID)
    ref_gid = os.environ.get("REFERENCE_GID", DEFAULT_REFERENCE_GID)

    sh_ref = connect_sheet(gc, ref_sheet_id)
    ws_ref = worksheet_by_gid(sh_ref, ref_gid)

    records = ws_ref.get_all_records()
    if not records:
        raise ValueError("Reference worksheet is empty.")

    df_ref = pd.DataFrame(records)
    df_ref.columns = [str(c).strip() for c in df_ref.columns]

    raw_col = find_column(df_ref, [
        "resource_name",
        "raw_resource_name",
        "resource",
        "resource id",
        "resource_id",
        "iemop_resource_name",
        "iemop resource name",
        "unit",
        "generator",
    ])
    plant_col = find_column(df_ref, [
        "plant_name",
        "plant name",
        "plant",
        "resource_label",
        "resource label",
        "label",
        "name",
    ])
    fuel_col = find_column(df_ref, [
        "fuel_type",
        "fuel type",
        "fuel",
        "technology",
        "tech",
    ])
    kind_col = find_column(df_ref, [
        "asset_kind",
        "asset kind",
        "type",
        "resource_kind",
        "resource kind",
        "unit/generator/battery",
        "unit_generator_battery",
        "asset_type",
        "asset type",
    ])

    if raw_col is None:
        raise ValueError(
            "Could not find a raw resource-name column in the reference sheet. "
            "Expected something like resource_name / raw_resource_name / resource."
        )

    out = pd.DataFrame()
    out["raw_resource_name"] = df_ref[raw_col].astype(str).str.strip()

    if plant_col is not None:
        out["plant_name"] = df_ref[plant_col].astype(str).str.strip()
    else:
        out["plant_name"] = out["raw_resource_name"]

    if fuel_col is not None:
        out["fuel_type"] = df_ref[fuel_col].astype(str).str.strip()
    else:
        out["fuel_type"] = ""

    if kind_col is not None:
        out["asset_kind"] = df_ref[kind_col].astype(str).str.strip()
    else:
        out["asset_kind"] = ""

    out["join_key"] = out["raw_resource_name"].map(normalize_key)

    out = out[out["join_key"] != ""].copy()
    out = out.drop_duplicates(subset=["join_key"], keep="first").reset_index(drop=True)

    return out


def enrich_resources(df: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["raw_resource_name"] = df["resource_name"].astype(str).str.strip()
    df["join_key"] = df["raw_resource_name"].map(normalize_key)

    df = df.merge(
        df_ref[["join_key", "plant_name", "fuel_type", "asset_kind"]],
        on="join_key",
        how="left",
        suffixes=("", "_ref"),
    )

    df["plant_name"] = df["plant_name"].fillna(df["raw_resource_name"])
    df["fuel_type"] = df["fuel_type"].replace("", pd.NA).fillna("Unknown")

    df["asset_kind"] = df.apply(
        lambda r: infer_asset_kind(
            raw_resource_name=r.get("raw_resource_name"),
            resource_type=r.get("resource_type"),
            fuel_type=r.get("fuel_type"),
            existing_kind=r.get("asset_kind"),
        ),
        axis=1,
    )

    # This is the human-readable label that replaces the original display name
    df["resource_name"] = df["plant_name"]

    df["is_battery"] = (
        df["asset_kind"].astype(str).str.lower().eq("battery")
        | df["fuel_type"].astype(str).str.contains("battery|bess", case=False, na=False)
        | df["raw_resource_name"].astype(str).str.contains("_BAT", case=False, na=False)
    )

    df = df.drop(columns=["join_key"])
    return df


def clean_iemop(df_raw: pd.DataFrame, gc=None) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df.columns = df.columns.str.lower()

    features = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_type",
        "marginal_price",
        "resource_name",
    ]
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in IEMOP data: {missing}")

    df = df[features]

    df["time_interval"] = parse_time_interval(df["time_interval"])

    reserve_map = {
        "Dr": "Dispatchable",
        "Rd": "Regulating Down",
        "Ru": "Regulating Up",
        "Fr": "Contingency",
    }
    df["commodity_type"] = df["commodity_type"].map(reserve_map).fillna(df["commodity_type"])
    df["commodity_type"] = df["commodity_type"].astype(str).str.title()
    df["resource_type"] = df["resource_type"].astype(str).str.title()

    region_map = {
        "CLUZ": "Luzon",
        "CVIS": "Visayas",
        "CMIN": "Mindanao",
    }
    df["region_name"] = df["region_name"].map(region_map).fillna(df["region_name"])

    if gc is not None:
        df_ref = load_reference_mapping(gc)
        df = enrich_resources(df, df_ref)
    else:
        df["raw_resource_name"] = df["resource_name"].astype(str).str.strip()
        df["plant_name"] = df["raw_resource_name"]
        df["fuel_type"] = "Unknown"
        df["asset_kind"] = df.apply(
            lambda r: infer_asset_kind(
                raw_resource_name=r.get("raw_resource_name"),
                resource_type=r.get("resource_type"),
                fuel_type="Unknown",
            ),
            axis=1,
        )
        df["is_battery"] = (
            df["asset_kind"].astype(str).str.lower().eq("battery")
            | df["raw_resource_name"].astype(str).str.contains("_BAT", case=False, na=False)
        )

    df = df.dropna(subset=["time_interval", "raw_resource_name", "commodity_type", "marginal_price"])
    df = df.drop_duplicates(subset=["time_interval", "raw_resource_name", "commodity_type"])
    df = df.sort_values(by=["time_interval", "region_name"]).reset_index(drop=True)

    df["source"] = "IEMOP"
    df["source_url"] = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df = df[DATA_HEADERS]
    return df


def fetch_incremental(last_time_str):
    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if pd.isna(last_dt):
            last_time_str = None
        else:
            start_date = (last_dt - pd.Timedelta(days=2)).strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            try:
                return fetch_iemop_data(start_date=start_date, end_date=end_date, verbose=False)
            except TypeError:
                return fetch_iemop_data(max_days=7, missing_limit=10, verbose=False)

    return fetch_iemop_data(max_days=7, missing_limit=10, verbose=False)


def append_rows_chunked(ws, rows, chunk_size=500):
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i:i + chunk_size]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        total += len(batch)
    return total


def main():
    gc = connect_gspread()

    destination_sheet_id = os.environ["GSHEET_ID"]
    sh = connect_sheet(gc, destination_sheet_id)

    ws_data = sh.worksheet("data")
    ws_meta = sh.worksheet("metadata")

    headers = sync_data_headers(ws_data)

    meta = get_metadata_map(ws_meta)
    last_time_str = meta.get("max_time_interval", "").strip() or None

    df_raw = fetch_incremental(last_time_str)
    df_clean = clean_iemop(df_raw, gc=gc)

    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if not pd.isna(last_dt):
            df_clean = df_clean[df_clean["time_interval"] > last_dt].copy()

    if df_clean.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=last_time_str)
        print("No new rows to append.")
        return

    MAX_APPEND = 15000
    if len(df_clean) > MAX_APPEND:
        df_clean = df_clean.tail(MAX_APPEND).copy()
        print(f"Warning: limiting append to the newest {MAX_APPEND} rows to prevent sheet overflow.")

    df_clean["time_interval"] = df_clean["time_interval"].dt.strftime("%Y-%m-%d %H:%M:%S")

    rows_to_append = [
        [r.get(col, "") for col in headers]
        for _, r in df_clean.iterrows()
    ]

    appended = append_rows_chunked(ws_data, rows_to_append, chunk_size=500)

    new_max_time = df_clean["time_interval"].max()
    update_last_updated_and_max_time(sh, max_time_interval_str=new_max_time)

    print(f"Fetched rows: {len(df_raw):,}")
    print(f"Clean new rows: {len(df_clean):,}")
    print(f"Appended rows: {appended:,}")


if __name__ == "__main__":
    main()
