"""
pipeline_to_sheets.py

Automated pipeline:
1) Fetch IEMOP reserve market data using download_iemop.fetch_iemop_data()
2) Clean / standardize
3) Enrich resources from a reference Google Sheet
4) Replace resource_name with a human-readable plant label
5) Add fuel_type and asset_kind (unit / generator / battery)
6) Append only NEW rows to Google Sheets (based on max_time_interval)
7) Update metadata timestamps (UTC + PHT) and max_time_interval

Required environment variables:
- GSHEET_ID: destination Google Sheet ID
- GCP_SA_JSON: service account JSON as a single JSON string

Optional environment variables:
- REFERENCE_SHEET_ID
- REFERENCE_GID
"""

import os
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from download_iemop import fetch_iemop_data

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_REFERENCE_SHEET_ID = "1QtR6jz0-s8tYHdG2s-sKxzBzj0eMS3SAPmQW_bG-5Zo"
DEFAULT_REFERENCE_GID = "1046991677"

DATA_HEADERS = [
    "time_interval",
    "region_name",
    "commodity_type",
    "resource_type",
    "raw_resource_name",
    "resource_name",   # human-readable label
    "plant_name",
    "fuel_type",
    "asset_kind",      # unit / generator / battery
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
    return gc.open_by_key(sheet_id), gc


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

    has_header = len(vals[0]) >= 2 and str(vals[0][0]).strip().lower() == "field"
    rows = vals[1:] if has_header else vals

    for r in rows:
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
        ws_meta.update(values=[[value]], range_name=f"B{row_idx}")
    else:
        next_row = len(col_a) + 1
        ws_meta.update(values=[[label]], range_name=f"A{next_row}")
        ws_meta.update(values=[[value]], range_name=f"B{next_row}")


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
    current = [str(x).strip() for x in current]

    if not current or all(x == "" for x in current):
        ws_data.update(values=[DATA_HEADERS], range_name="A1")
        return DATA_HEADERS

    existing_headers = [x for x in current if x != ""]
    final_headers = existing_headers.copy()

    for col in DATA_HEADERS:
        if col not in final_headers:
            final_headers.append(col)

    if final_headers != existing_headers:
        ws_data.update(values=[final_headers], range_name="A1")
        print("Updated data sheet headers by appending missing columns.")

    return final_headers


def parse_time_interval(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")

    mask = dt.isna() & s.ne("")
    if mask.any():
        try:
            dt.loc[mask] = pd.to_datetime(s.loc[mask], errors="coerce", format="mixed")
        except TypeError:
            dt.loc[mask] = pd.to_datetime(s.loc[mask], errors="coerce")

    return dt


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


def normalize_asset_kind(val, raw_name="", fuel=""):
    v = str(val).strip().lower()
    raw = str(raw_name).strip().lower()
    fuel = str(fuel).strip().lower()

    if "battery" in v or "bess" in v or "battery" in fuel or "bess" in fuel or "_bat" in raw:
        return "battery"
    if "unit" in v:
        return "unit"
    if "generator" in v or "gen set" in v or "genset" in v or v == "gen":
        return "generator"
    return ""


def infer_asset_kind(raw_resource_name, resource_type, fuel_type, existing_kind=None):
    if pd.notna(existing_kind) and str(existing_kind).strip():
        normalized = normalize_asset_kind(
            existing_kind,
            raw_name=raw_resource_name,
            fuel=fuel_type,
        )
        if normalized:
            return normalized

    raw = str(raw_resource_name or "").lower()
    rtype = str(resource_type or "").lower()
    fuel = str(fuel_type or "").lower()

    if "_bat" in raw or "battery" in raw or "bess" in raw or "battery" in fuel or "bess" in fuel:
        return "battery"
    if "unit" in raw or "unit" in rtype:
        return "unit"
    if "generator" in raw or "gen set" in raw or "genset" in raw or "generator" in rtype:
        return "generator"

    return "generator"


def load_reference_mapping(gc) -> pd.DataFrame:
    ref_sheet_id = os.environ.get("REFERENCE_SHEET_ID", DEFAULT_REFERENCE_SHEET_ID)
    ref_gid = os.environ.get("REFERENCE_GID", DEFAULT_REFERENCE_GID)

    sh_ref = gc.open_by_key(ref_sheet_id)
    ws_ref = worksheet_by_gid(sh_ref, ref_gid)

    values = ws_ref.get_all_values()
    if not values or len(values) < 2:
        raise ValueError("Reference worksheet is empty.")

    header = [str(x).strip() for x in values[0]]
    rows = values[1:]

    df_ref = pd.DataFrame(rows, columns=header)

    df_ref = df_ref.loc[
        ~(df_ref.apply(lambda r: all(str(v).strip() == "" for v in r), axis=1))
    ].copy()

    raw_col = find_column(df_ref, [
        "Full resource name",
        "full resource name",
        "resource_name",
        "raw_resource_name",
        "resource",
        "iemop_resource_name",
        "iemop resource name",
    ])
    plant_col = find_column(df_ref, [
        "Plant name",
        "plant name",
        "plant_name",
        "plant",
        "resource_label",
        "label",
    ])
    fuel_col = find_column(df_ref, [
        "Fuel",
        "fuel",
        "fuel_type",
        "fuel type",
        "technology",
    ])
    kind_col = find_column(df_ref, [
        "Unit / generator",
        "unit / generator",
        "Unit / generator / battery",
        "unit / generator / battery",
        "unit/generator",
        "unit/generator/battery",
        "asset_kind",
        "asset kind",
        "type",
    ])

    if raw_col is None:
        raise ValueError(
            f"Could not find a raw resource-name column in the reference sheet. "
            f"Found columns: {list(df_ref.columns)}"
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

    out["asset_kind"] = out.apply(
        lambda r: normalize_asset_kind(
            r["asset_kind"],
            raw_name=r["raw_resource_name"],
            fuel=r["fuel_type"],
        ),
        axis=1,
    )

    out = out.drop_duplicates(subset=["join_key"], keep="first").reset_index(drop=True)
    return out[["join_key", "plant_name", "fuel_type", "asset_kind"]]


def safe_load_reference_mapping(gc) -> Optional[pd.DataFrame]:
    try:
        return load_reference_mapping(gc)
    except Exception as e:
        print(f"Warning: reference mapping could not be loaded. Continuing without enrichment. Details: {e}")
        return None


def enrich_resources(df: pd.DataFrame, df_ref: Optional[pd.DataFrame]) -> pd.DataFrame:
    df = df.copy()

    df["raw_resource_name"] = df["resource_name"].astype(str).str.strip()

    if df_ref is not None and not df_ref.empty:
        df["join_key"] = df["raw_resource_name"].map(normalize_key)
        df = df.merge(df_ref, on="join_key", how="left")
        df = df.drop(columns=["join_key"])
    else:
        df["plant_name"] = pd.NA
        df["fuel_type"] = pd.NA
        df["asset_kind"] = pd.NA

    df["plant_name"] = df["plant_name"].replace("", pd.NA).fillna(df["raw_resource_name"])
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

    df["resource_name"] = df["plant_name"]

    df["is_battery"] = (
        df["asset_kind"].astype(str).str.lower().eq("battery")
        | df["fuel_type"].astype(str).str.contains("battery|bess", case=False, na=False)
        | df["raw_resource_name"].astype(str).str.contains("_BAT", case=False, na=False)
    )

    return df


def clean_iemop(df_raw: pd.DataFrame, gc=None) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_type",
        "marginal_price",
        "resource_name",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in IEMOP data: {missing}")

    df = df[required].copy()

    df["time_interval"] = parse_time_interval(df["time_interval"])
    df["marginal_price"] = pd.to_numeric(df["marginal_price"], errors="coerce")

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

    df_ref = safe_load_reference_mapping(gc) if gc is not None else None
    df = enrich_resources(df, df_ref)

    df = df.dropna(subset=["time_interval", "raw_resource_name", "commodity_type", "marginal_price"])
    df = df.drop_duplicates(subset=["time_interval", "raw_resource_name", "commodity_type"])
    df = df.sort_values(by=["time_interval", "region_name"]).reset_index(drop=True)

    df["source"] = "IEMOP"
    df["source_url"] = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return df[DATA_HEADERS]


def fetch_incremental(last_time_str: Optional[str]):
    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if not pd.isna(last_dt):
            start_date = (last_dt - pd.Timedelta(days=2)).strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            try:
                return fetch_iemop_data(start_date=start_date, end_date=end_date, verbose=False)
            except TypeError:
                pass

            try:
                return fetch_iemop_data(start_date=start_date, end_date=end_date)
            except TypeError:
                pass

    try:
        return fetch_iemop_data(max_days=7, missing_limit=10, verbose=False)
    except TypeError:
        return fetch_iemop_data()


def append_rows_chunked(ws, rows, chunk_size=500):
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i:i + chunk_size]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        total += len(batch)
    return total


def main():
    sh, gc = connect_sheet()
    ws_data = sh.worksheet("data")
    ws_meta = sh.worksheet("metadata")

    headers = sync_data_headers(ws_data)

    meta = get_metadata_map(ws_meta)
    last_time_str = meta.get("max_time_interval", "").strip() or None

    df_raw = fetch_incremental(last_time_str)

    if df_raw is None or len(df_raw) == 0:
        update_last_updated_and_max_time(sh, max_time_interval_str=last_time_str)
        print("No data returned from fetch_iemop_data().")
        return

    df_clean = clean_iemop(df_raw, gc=gc)

    if last_time_str:
        last_dt = pd.to_datetime(last_time_str, errors="coerce")
        if not pd.isna(last_dt):
            df_clean = df_clean[df_clean["time_interval"] > last_dt].copy()

    if df_clean.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=last_time_str)
        print("No new rows to append.")
        return

    max_append = 15000
    if len(df_clean) > max_append:
        df_clean = df_clean.tail(max_append).copy()
        print(f"Warning: limiting append to newest {max_append} rows.")

    df_clean["time_interval"] = df_clean["time_interval"].dt.strftime("%Y-%m-%d %H:%M:%S")

    rows_to_append = []
    for _, row in df_clean.iterrows():
        rows_to_append.append([row.get(col, "") for col in headers])

    appended = append_rows_chunked(ws_data, rows_to_append, chunk_size=500)

    new_max_time = df_clean["time_interval"].max()
    update_last_updated_and_max_time(sh, max_time_interval_str=new_max_time)

    print(
        f"Fetched rows: {len(df_raw):,} | "
        f"Clean rows: {len(df_clean):,} | "
        f"Appended rows: {appended:,}"
    )


if __name__ == "__main__":
    main()
