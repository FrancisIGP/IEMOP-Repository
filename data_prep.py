"""
data_prep.py

Runs AFTER pipeline_to_sheets.py.

What this does:
1. Opens the main Google Sheet using GSHEET_ID.
2. Opens the reference Google Sheet.
3. Matches data.resource_name with reference Full resource name / resource_name.
4. Adds:
   - Plant name
   - Unit/Generator
   - Location
   - Fuel
   - Operator/Owner
5. Removes:
   - resource_type
   - is_battery
6. Overwrites the existing "data" tab.

Required GitHub Actions secrets:
- GCP_SA_JSON
- GSHEET_ID
"""

import os
import json
import re
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials


# ============================================================
# Config
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAIN_GSHEET_ID = os.environ["GSHEET_ID"]

REFERENCE_GSHEET_ID = os.environ.get(
    "REFERENCE_GSHEET_ID",
    "1QtR6jz0-s8tYHdG2s-sKxzBzj0eMS3SAPmQW_bG-5Zo",
)

DATA_TAB = os.environ.get("DATA_TAB", "data")


# ============================================================
# Google Sheets connection
# ============================================================

def connect_gsheet(sheet_id: str):
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


# ============================================================
# Cleaning helpers
# ============================================================

def normalize_column_name(col: str) -> str:
    col = str(col).strip().lower()
    col = col.replace("/", " ")
    col = col.replace("-", " ")
    col = col.replace("(", " ")
    col = col.replace(")", " ")
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def make_unique_columns(columns):
    seen = {}
    unique = []

    for col in columns:
        base = col
        if base not in seen:
            seen[base] = 0
            unique.append(base)
        else:
            seen[base] += 1
            unique.append(f"{base}_{seen[base]}")

    return unique


def normalize_resource_name(value) -> str:
    if pd.isna(value):
        return ""

    value = str(value).strip().upper()
    value = re.sub(r"\s+", "", value)

    return value


def safe_cell_value(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        if np.isnan(value):
            return ""
        return float(value)

    if isinstance(value, np.bool_):
        return bool(value)

    return value


def dataframe_to_values(df: pd.DataFrame):
    df = df.replace([np.inf, -np.inf], np.nan)

    values = [df.columns.tolist()]

    for row in df.itertuples(index=False, name=None):
        values.append([safe_cell_value(v) for v in row])

    return values


def read_worksheet_as_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)

    df = df.dropna(how="all")
    df = df.loc[
        ~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")
    ]

    return df


def write_df_to_worksheet(ws, df: pd.DataFrame, chunk_size: int = 5000):
    rows_needed = max(len(df) + 1, 2)
    cols_needed = max(len(df.columns), 1)

    print(f"Resizing '{ws.title}' to {rows_needed:,} rows x {cols_needed:,} columns...")
    ws.resize(rows=rows_needed, cols=cols_needed)

    print(f"Clearing '{ws.title}'...")
    ws.clear()

    values = dataframe_to_values(df)

    print("Writing header...")
    ws.update(
        range_name="A1",
        values=[values[0]],
        value_input_option="USER_ENTERED",
    )

    body = values[1:]

    print(f"Writing {len(body):,} rows in chunks...")
    for start in range(0, len(body), chunk_size):
        chunk = body[start:start + chunk_size]
        start_row = start + 2

        ws.update(
            range_name=f"A{start_row}",
            values=chunk,
            value_input_option="USER_ENTERED",
        )

        print(f"Wrote rows {start_row:,} to {start_row + len(chunk) - 1:,}")


# ============================================================
# Reference sheet detection
# ============================================================

REFERENCE_COLUMN_ALIASES = {
    "full_resource_name": "resource_name",
    "resource_name": "resource_name",
    "resource": "resource_name",
    "resources": "resource_name",

    "plant_name": "plant_name",
    "plant": "plant_name",

    "unit_generator": "unit_generator",
    "unit": "unit_generator",
    "generator": "unit_generator",
    "unit_gen": "unit_generator",
    "unit_generators": "unit_generator",

    "location": "location",
    "province": "location",

    "fuel": "fuel",
    "fuel_type": "fuel",
    "energy_type": "fuel",

    "operator_owner": "operator_owner",
    "operator": "operator_owner",
    "owner": "operator_owner",
    "operator_and_owner": "operator_owner",
    "operator_owner_name": "operator_owner",
}


def canonicalize_reference_columns(columns):
    normalized = [normalize_column_name(c) for c in columns]
    canonical = [REFERENCE_COLUMN_ALIASES.get(c, c) for c in normalized]
    canonical = make_unique_columns(canonical)
    return canonical


def reference_header_score(row):
    canonical = canonicalize_reference_columns(row)

    required = {
        "resource_name",
        "plant_name",
        "unit_generator",
        "location",
        "fuel",
        "operator_owner",
    }

    return len(required.intersection(set(canonical)))


def read_reference_sheet_auto(ws) -> pd.DataFrame:
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    best_header_row = 0
    best_score = -1

    for i, row in enumerate(values[:10]):
        score = reference_header_score(row)

        if score > best_score:
            best_score = score
            best_header_row = i

    print(
        f"Detected header row {best_header_row + 1} in reference tab '{ws.title}' "
        f"with score {best_score}."
    )

    header = values[best_header_row]
    rows = values[best_header_row + 1:]

    df = pd.DataFrame(rows, columns=header)

    df = df.dropna(how="all")
    df = df.loc[
        ~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")
    ]

    return df


def get_best_reference_worksheet(reference_sh):
    worksheets = reference_sh.worksheets()

    if not worksheets:
        raise ValueError("Reference spreadsheet has no worksheets.")

    best_ws = None
    best_score = -1

    print(f"Available reference tabs: {[ws.title for ws in worksheets]}")

    for ws in worksheets:
        values = ws.get_all_values()

        if not values:
            continue

        local_best_score = -1

        for row in values[:10]:
            score = reference_header_score(row)
            local_best_score = max(local_best_score, score)

        print(f"Reference tab candidate '{ws.title}' score: {local_best_score}")

        if local_best_score > best_score:
            best_score = local_best_score
            best_ws = ws

    if best_ws is None or best_score < 4:
        raise ValueError(
            "Could not detect the correct reference tab. "
            "Make sure the reference sheet has columns for resource name, plant name, "
            "unit/generator, location, fuel, and operator/owner."
        )

    print(f"Using reference tab: {best_ws.title}")
    return best_ws


# ============================================================
# Load data
# ============================================================

def load_main_data(main_sh) -> pd.DataFrame:
    ws = main_sh.worksheet(DATA_TAB)
    values = ws.get_all_values()

    if not values:
        raise ValueError(f"The '{DATA_TAB}' tab is empty.")

    required = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_name",
        "marginal_price",
    ]

    first_row = values[0]
    normalized_first_row = [normalize_column_name(c) for c in first_row]

    has_valid_header = all(col in normalized_first_row for col in required)

    def normalize_rows_to_width(rows, width):
        fixed_rows = []

        for row in rows:
            row = row[:width]

            if len(row) < width:
                row = row + [""] * (width - len(row))

            fixed_rows.append(row)

        return fixed_rows

    if has_valid_header:
        print("Valid header row detected in data tab.")

        header = first_row
        rows = values[1:]

        df = pd.DataFrame(rows, columns=header)

    else:
        print("WARNING: Header row is missing. Repairing header automatically...")

        col_count = len(first_row)

        # Case 1: already prepared data but header is missing.
        # Current row pattern:
        # time_interval, resource_name, Plant name, marginal_price,
        # unit/generator, fuel, commodity_type, operator/owner,
        # region_name, Location, source, source_url, ingested_at_utc
        if col_count == 13:
            first_row_lower = [str(x).strip().lower() for x in first_row]

            # Detect whether source/source_url/ingested_at_utc are in old order.
            if (
                len(first_row_lower) >= 13
                and first_row_lower[10] == "iemop"
                and first_row_lower[11].startswith("http")
            ):
                header = [
                    "time_interval",
                    "resource_name",
                    "plant_name",
                    "marginal_price",
                    "unit_generator",
                    "fuel",
                    "commodity_type",
                    "operator_owner",
                    "region_name",
                    "location",
                    "source",
                    "source_url",
                    "ingested_at_utc",
                ]
            else:
                header = [
                    "time_interval",
                    "resource_name",
                    "plant_name",
                    "marginal_price",
                    "unit_generator",
                    "fuel",
                    "commodity_type",
                    "operator_owner",
                    "region_name",
                    "location",
                    "ingested_at_utc",
                    "source",
                    "source_url",
                ]

            rows = normalize_rows_to_width(values, len(header))
            df = pd.DataFrame(rows, columns=header)

        # Case 2: old raw pipeline data but header is missing.
        elif col_count == 10:
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

            rows = normalize_rows_to_width(values, len(header))
            df = pd.DataFrame(rows, columns=header)

        else:
            raise ValueError(
                f"Header row is missing and the script could not infer the structure. "
                f"Found {col_count} columns in the first row: {first_row}"
            )

    df = df.dropna(how="all")
    df = df.loc[
        ~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")
    ]

    df.columns = [normalize_column_name(c) for c in df.columns]
    df.columns = make_unique_columns(df.columns)

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required columns in '{DATA_TAB}' tab: {missing}. "
            f"Found columns: {df.columns.tolist()}"
        )

    df["resource_name"] = df["resource_name"].astype(str).str.strip()
    df["resource_key"] = df["resource_name"].apply(normalize_resource_name)

    return df

def load_reference_data(reference_sh) -> pd.DataFrame:
    ws = get_best_reference_worksheet(reference_sh)
    ref = read_reference_sheet_auto(ws)

    if ref.empty:
        raise ValueError("Reference sheet is empty.")

    ref.columns = canonicalize_reference_columns(ref.columns)

    required = [
        "resource_name",
        "plant_name",
        "unit_generator",
        "location",
        "fuel",
        "operator_owner",
    ]

    missing = [c for c in required if c not in ref.columns]

    if missing:
        raise ValueError(
            f"Missing required reference columns: {missing}. "
            f"Found columns after cleaning: {ref.columns.tolist()}"
        )

    ref = ref[required].copy()

    for col in required:
        ref[col] = ref[col].astype(str).str.strip()

    ref["resource_key"] = ref["resource_name"].apply(normalize_resource_name)

    ref = ref[ref["resource_key"] != ""]
    ref = ref.drop_duplicates(subset=["resource_key"], keep="first")

    print(f"Reference rows loaded: {len(ref):,}")

    return ref


# ============================================================
# Prepare data
# ============================================================

def prepare_data(df: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
    # Remove old enrichment columns first to avoid duplicate columns after reruns.
    columns_to_remove_before_merge = [
        "plant_name",
        "unit_generator",
        "location",
        "fuel",
        "operator_owner",
    ]

    df = df.drop(columns=columns_to_remove_before_merge, errors="ignore")

    ref_for_merge = ref.drop(columns=["resource_name"], errors="ignore")

    merged = df.merge(
        ref_for_merge,
        on="resource_key",
        how="left",
    )

    merged["plant_name"] = merged["plant_name"].replace("", pd.NA).fillna("")
    merged["unit_generator"] = merged["unit_generator"].replace("", pd.NA).fillna("")
    merged["location"] = merged["location"].replace("", pd.NA).fillna("")
    merged["fuel"] = merged["fuel"].replace("", pd.NA).fillna("")
    merged["operator_owner"] = merged["operator_owner"].replace("", pd.NA).fillna("")

    # Remove requested old columns.
    merged = merged.drop(
        columns=[
            "resource_type",
            "is_battery",
            "resource_key",
        ],
        errors="ignore",
    )

    # Rename enriched fields to final Google Sheet column names.
    output_rename = {
        "plant_name": "Plant name",
        "unit_generator": "unit/generator",
        "location": "Location",
        "fuel": "fuel",
        "operator_owner": "operator/owner",
    }

    merged = merged.rename(columns=output_rename)

    # Final requested column order.
    preferred_order = [
        "time_interval",
        "resource_name",
        "Plant name",
        "marginal_price",
        "unit/generator",
        "fuel",
        "commodity_type",
        "operator/owner",
        "region_name",
        "Location",
        "ingested_at_utc",
        "source",
        "source_url",
    ]

    existing_preferred = [c for c in preferred_order if c in merged.columns]
    remaining = [c for c in merged.columns if c not in existing_preferred]

    prepared = merged[existing_preferred + remaining].copy()

    # Sort if possible.
    if "time_interval" in prepared.columns:
        prepared["_sort_time"] = pd.to_datetime(prepared["time_interval"], errors="coerce")

        sort_cols = ["_sort_time"]

        for col in ["region_name", "commodity_type", "resource_name"]:
            if col in prepared.columns:
                sort_cols.append(col)

        prepared = prepared.sort_values(sort_cols, na_position="last")
        prepared = prepared.drop(columns=["_sort_time"])

    return prepared

def build_unmatched_report(prepared: pd.DataFrame) -> pd.DataFrame:
    missing_mask = (
        prepared["Plant name"].astype(str).str.strip().eq("") |
        prepared["unit/generator"].astype(str).str.strip().eq("") |
        prepared["Location"].astype(str).str.strip().eq("") |
        prepared["fuel"].astype(str).str.strip().eq("") |
        prepared["operator/owner"].astype(str).str.strip().eq("")
    )

    unmatched = (
        prepared.loc[missing_mask, ["resource_name"]]
        .drop_duplicates()
        .sort_values("resource_name")
        .reset_index(drop=True)
    )

    return unmatched

# ============================================================
# Metadata
# ============================================================

def get_or_create_worksheet(sh, title: str, rows: int = 20, cols: int = 3):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def update_metadata(main_sh, prepared_rows: int, unmatched_count: int):
    ws = get_or_create_worksheet(main_sh, "metadata", rows=20, cols=3)

    utc_ts = datetime.now(timezone.utc)
    pht_ts = utc_ts.astimezone(timezone(timedelta(hours=8)))

    values = ws.get_all_values()
    col_a = [row[0].strip() for row in values if row and len(row) > 0]

    def upsert(label, value):
        nonlocal col_a

        if label in col_a:
            row_idx = col_a.index(label) + 1
            ws.update(
                range_name=f"B{row_idx}",
                values=[[value]],
                value_input_option="USER_ENTERED",
            )
        else:
            next_row = len(col_a) + 1
            ws.update(
                range_name=f"A{next_row}:B{next_row}",
                values=[[label, value]],
                value_input_option="USER_ENTERED",
            )
            col_a.append(label)

    upsert("data_prep_last_updated_utc", utc_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prep_last_updated_pht", pht_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prep_rows", prepared_rows)
    upsert("data_prep_unmatched_resources", unmatched_count)
    upsert("reference_gsheet_id", REFERENCE_GSHEET_ID)


# ============================================================
# Main
# ============================================================

def main():
    print("Connecting to main Google Sheet...")
    main_sh = connect_gsheet(MAIN_GSHEET_ID)

    print("Connecting to reference Google Sheet...")
    reference_sh = connect_gsheet(REFERENCE_GSHEET_ID)

    print("Loading main data...")
    main_df = load_main_data(main_sh)
    print(f"Main rows loaded: {len(main_df):,}")

    print("Loading reference data...")
    reference_df = load_reference_data(reference_sh)

    print("Preparing data...")
    prepared = prepare_data(main_df, reference_df)
    print(f"Prepared rows: {len(prepared):,}")
    print(f"Prepared columns: {prepared.columns.tolist()}")

    unmatched = build_unmatched_report(prepared)
    print(f"Unmatched resources: {len(unmatched):,}")

    if not unmatched.empty:
        print("Unmatched resource names:")
        for name in unmatched["resource_name"].head(50).tolist():
            print(f"- {name}")

    print(f"Overwriting existing '{DATA_TAB}' tab...")
    data_ws = main_sh.worksheet(DATA_TAB)
    write_df_to_worksheet(data_ws, prepared)

    print("Updating metadata...")
    update_metadata(
        main_sh=main_sh,
        prepared_rows=len(prepared),
        unmatched_count=len(unmatched),
    )

    print("Done. Data tab has been enriched and old columns were removed.")


if __name__ == "__main__":
    main()
