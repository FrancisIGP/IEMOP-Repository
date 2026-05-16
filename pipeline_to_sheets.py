"""
pipeline_to_sheets.py

IEMOP -> Google Sheets pipeline
- keeps the original simple pipeline flow
- adds readable resource labels and fuel type
- appends based on actual existing rows in the data tab
- updates metadata only after append logic
"""

import os
import json
import re
import time
import warnings
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
    "raw_resource_name",
    "resource_name",
    "fuel_type",
    "asset_kind",
    "commodity_type",
    "region_name",
    "marginal_price",
    "source",
    "ingested_at_utc",
    "source_url",
]

MAX_APPEND_PER_RUN = int(os.environ.get("MAX_APPEND_PER_RUN", "8000"))
APPEND_CHUNK_SIZE = int(os.environ.get("APPEND_CHUNK_SIZE", "2000"))


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


def batch_update_metadata(ws_meta, updates: dict):
    col_a = [str(x).strip() for x in ws_meta.col_values(1)]
    requests = []

    for label, value in updates.items():
        if label in col_a:
            row_idx = col_a.index(label) + 1
            requests.append({
                "range": f"B{row_idx}",
                "values": [[value]],
            })
        else:
            next_row = len(col_a) + 1
            requests.append({
                "range": f"A{next_row}",
                "values": [[label]],
            })
            requests.append({
                "range": f"B{next_row}",
                "values": [[value]],
            })
            col_a.append(label)

    if requests:
        ws_meta.batch_update(requests)


def update_last_updated_and_max_time(sh, max_time_interval_str=None):
    ws_meta = sh.worksheet("metadata")

    utc_ts = datetime.now(timezone.utc)
    pht_ts = utc_ts.astimezone(timezone(timedelta(hours=8)))

    updates = {
        "last_updated_utc": utc_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "last_updated_pht": pht_ts.strftime("%Y-%m-%d %H:%M:%S"),
    }

    if max_time_interval_str:
        updates["max_time_interval"] = max_time_interval_str

    batch_update_metadata(ws_meta, updates)

    print("Metadata updated.")
    if max_time_interval_str:
        print(f"Updated max_time_interval = {max_time_interval_str}")


def ensure_data_headers(ws_data):
    expected = DATA_HEADERS[:]
    current = [str(x).strip() for x in ws_data.row_values(1)]

    # Always enforce exact header order
    ws_data.update(values=[expected], range_name="A1")
    print("Set data headers to exact schema.")

    # Remove extra trailing columns from older versions
    if len(current) > len(expected):
        ws_data.delete_columns(len(expected) + 1, len(current))
        print(f"Removed {len(current) - len(expected)} obsolete columns.")

    return expected


def parse_time_interval(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")

    mask = dt.isna() & s.ne("")
    if mask.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
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


def infer_asset_kind(raw_resource_name, fuel_type, existing_kind=None):
    if pd.notna(existing_kind) and str(existing_kind).strip():
        norm = normalize_asset_kind(existing_kind, raw_resource_name, fuel_type)
        if norm:
            return norm

    raw = str(raw_resource_name or "").lower()
    fuel = str(fuel_type or "").lower()

    if "_bat" in raw or "battery" in raw or "bess" in raw or "battery" in fuel or "bess" in fuel:
        return "battery"
    if "unit" in raw:
        return "unit"
    if "generator" in raw or "gen set" in raw or "genset" in raw:
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
        "resource_name",
        "raw_resource_name",
        "resource",
        "iemop_resource_name",
        "iemop resource name",
    ])
    plant_col = find_column(df_ref, [
        "Plant name",
        "plant_name",
        "plant",
        "resource_label",
        "label",
    ])
    fuel_col = find_column(df_ref, [
        "Fuel",
        "fuel_type",
        "fuel type",
        "fuel",
        "technology",
    ])
    kind_col = find_column(df_ref, [
        "Unit / generator",
        "Unit / generator / battery",
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
    out["plant_name"] = df_ref[plant_col].astype(str).str.strip() if plant_col else out["raw_resource_name"]
    out["fuel_type"] = df_ref[fuel_col].astype(str).str.strip() if fuel_col else ""
    out["asset_kind"] = df_ref[kind_col].astype(str).str.strip() if kind_col else ""

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


def clean_iemop(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=[
            "time_interval",
            "region_name",
            "commodity_type",
            "resource_name",
            "marginal_price",
        ])

    df = df_raw.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_name",
        "marginal_price",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in IEMOP data: {missing}")

    df = df[required].copy()
    df["time_interval"] = parse_time_interval(df["time_interval"])

    df["marginal_price"] = (
        df["marginal_price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["marginal_price"] = pd.to_numeric(df["marginal_price"], errors="coerce")

    reserve_map = {
        "Dr": "Dispatchable",
        "Rd": "Regulating Down",
        "Ru": "Regulating Up",
        "Fr": "Contingency",
    }
    df["commodity_type"] = df["commodity_type"].map(reserve_map).fillna(df["commodity_type"])
    df["commodity_type"] = df["commodity_type"].astype(str).str.title()

    region_map = {
        "CLUZ": "Luzon",
        "CVIS": "Visayas",
        "CMIN": "Mindanao",
    }
    df["region_name"] = df["region_name"].map(region_map).fillna(df["region_name"])

    df = df.dropna(subset=["time_interval", "resource_name", "commodity_type", "marginal_price"])
    df = df.drop_duplicates(subset=["time_interval", "resource_name", "commodity_type"])
    df = df.sort_values(by=["time_interval", "region_name"]).reset_index(drop=True)

    return df


def enrich_iemop(df_clean: pd.DataFrame, gc) -> pd.DataFrame:
    if df_clean.empty:
        return pd.DataFrame(columns=DATA_HEADERS)

    df = df_clean.copy()
    df["raw_resource_name"] = df["resource_name"].astype(str).str.strip()

    df_ref = safe_load_reference_mapping(gc)

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
            fuel_type=r.get("fuel_type"),
            existing_kind=r.get("asset_kind"),
        ),
        axis=1,
    )

    df["resource_name"] = df["plant_name"]
    df["source"] = "IEMOP"
    df["ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["source_url"] = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"

    return df[DATA_HEADERS]


def fetch_recent_window():
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%d")

    try:
        return fetch_iemop_data(start_date=start_date, end_date=end_date, verbose=False)
    except TypeError:
        try:
            return fetch_iemop_data(start_date=start_date, end_date=end_date)
        except TypeError:
            return fetch_iemop_data(max_days=14, missing_limit=10, verbose=False)


def build_existing_keys(ws_data):
    values = ws_data.get_all_values()
    if not values or len(values) <= 1:
        return set()

    header_row = [str(x).strip() for x in values[0]]
    idx = {name: i for i, name in enumerate(header_row)}

    time_idx = idx.get("time_interval")
    comm_idx = idx.get("commodity_type")
    raw_idx = idx.get("raw_resource_name")
    name_idx = idx.get("resource_name")

    keys = set()

    if time_idx is None or comm_idx is None:
        return keys

    for row in values[1:]:
        if len(row) <= max(time_idx, comm_idx):
            continue

        time_val = row[time_idx].strip() if time_idx < len(row) else ""
        comm_val = row[comm_idx].strip() if comm_idx < len(row) else ""

        raw_val = ""
        if raw_idx is not None and raw_idx < len(row):
            raw_val = row[raw_idx].strip()

        if not raw_val and name_idx is not None and name_idx < len(row):
            raw_val = row[name_idx].strip()

        if time_val and raw_val and comm_val:
            keys.add((time_val, raw_val, comm_val))

    return keys


def append_rows_chunked(ws, rows, chunk_size=APPEND_CHUNK_SIZE):
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i:i + chunk_size]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        total += len(batch)
        print(f"Appended batch: {len(batch):,} rows")
        time.sleep(2)
    return total


def main():
    sh, gc = connect_sheet()
    ws_data = sh.worksheet("data")
    ws_meta = sh.worksheet("metadata")

    headers = ensure_data_headers(ws_data)
    meta = get_metadata_map(ws_meta)
    old_max_time = meta.get("max_time_interval", "").strip() or None
    print("Existing metadata max_time_interval:", old_max_time)

    df_raw = fetch_recent_window()
    if df_raw is None or len(df_raw) == 0:
        update_last_updated_and_max_time(sh, max_time_interval_str=old_max_time)
        print("No rows returned from fetch_iemop_data.")
        return

    print(f"Fetched raw rows: {len(df_raw):,}")

    df_clean = clean_iemop(df_raw)
    print(f"Rows after base cleaning: {len(df_clean):,}")

    if df_clean.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=old_max_time)
        print("No rows remained after base cleaning.")
        return

    df_final = enrich_iemop(df_clean, gc)
    print(f"Rows after enrichment: {len(df_final):,}")

    if df_final.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=old_max_time)
        print("No rows remained after enrichment.")
        return

    existing_keys = build_existing_keys(ws_data)
    print(f"Existing keys in data tab: {len(existing_keys):,}")

    df_final["time_interval"] = pd.to_datetime(df_final["time_interval"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df_final["append_key"] = list(
        zip(
            df_final["time_interval"],
            df_final["raw_resource_name"].astype(str),
            df_final["commodity_type"].astype(str),
        )
    )

    df_to_append = df_final.loc[~df_final["append_key"].isin(existing_keys)].copy()
    print(f"Rows to append after dedupe: {len(df_to_append):,}")

    latest_seen_time = df_final["time_interval"].max()

    if df_to_append.empty:
        update_last_updated_and_max_time(sh, max_time_interval_str=latest_seen_time)
        print("No new rows to append after checking existing data tab.")
        return

    if len(df_to_append) > MAX_APPEND_PER_RUN:
        df_to_append = df_to_append.tail(MAX_APPEND_PER_RUN).copy()
        print(f"Limiting append to {MAX_APPEND_PER_RUN:,} rows this run to avoid quota issues.")

    rows_to_append = []
    for _, row in df_to_append.iterrows():
        row_dict = row.to_dict()
        rows_to_append.append([row_dict.get(col, "") for col in headers])

    appended = append_rows_chunked(ws_data, rows_to_append, chunk_size=APPEND_CHUNK_SIZE)

    new_max_time = df_to_append["time_interval"].max()
    time.sleep(3)
    update_last_updated_and_max_time(sh, max_time_interval_str=new_max_time)

    print(
        f"Fetched rows: {len(df_raw):,} | "
        f"Clean rows: {len(df_clean):,} | "
        f"Final rows: {len(df_final):,} | "
        f"Appended rows: {appended:,}"
    )


if __name__ == "__main__":
    main()
