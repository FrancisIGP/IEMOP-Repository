"""
data_prep.py

Runs AFTER pipeline_to_sheets.py.

Purpose:
1. Read raw IEMOP data from the main Google Sheet "data" tab.
2. Read meaningful resource labels from the reference Google Sheet.
3. Merge labels into the main data.
4. Remove old columns:
   - resource_type
   - is_battery
5. Write cleaned output to "data_prepared".
6. Create insight tables for:
   - top producers by fuel type
   - top producers for selected fuel type
   - top producers by selected fuel per island heatmap
   - fuel type by island heatmap
   - market concentration by operator/conglomerate
7. Create dashboard charts in Google Sheets.

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

RESOURCE_LABELS_GSHEET_ID = os.environ.get(
    "RESOURCE_LABELS_GSHEET_ID",
    "15IeST_wPRmYbnKeCA6Sv5dutYF7dAl-6uB9zuw6URZI",
)

RAW_DATA_TAB = os.environ.get("RAW_DATA_TAB", "data")
REFERENCE_TAB = os.environ.get("REFERENCE_TAB", "External")
PREPARED_TAB = os.environ.get("PREPARED_TAB", "data_prepared")

# Change this in GitHub Actions if you want Coal, Hydro, Natural Gas, Solar, etc.
TOP_FUEL_FILTER = os.environ.get("TOP_FUEL_FILTER", "Coal")


# ============================================================
# Google Sheets helpers
# ============================================================

def connect_gsheet(sheet_id: str):
    sa_info = json.loads(os.environ["GCP_SA_JSON"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 30):
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def normalize_column_name(col: str) -> str:
    col = str(col).strip().lower()
    col = col.replace("/", " ")
    col = col.replace("-", " ")
    col = col.replace("(", " ")
    col = col.replace(")", " ")
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


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
    if df.empty:
        return [["No data"]]

    clean_df = df.copy()
    clean_df = clean_df.replace([np.inf, -np.inf], np.nan)

    values = [clean_df.columns.tolist()]

    for row in clean_df.itertuples(index=False, name=None):
        values.append([safe_cell_value(v) for v in row])

    return values


def write_df_to_worksheet(ws, df: pd.DataFrame, chunk_size: int = 5000):
    ws.clear()

    if df.empty:
        ws.update(range_name="A1", values=[["No data"]], value_input_option="USER_ENTERED")
        return

    rows_needed = max(len(df) + 1, 2)
    cols_needed = max(len(df.columns), 1)

    ws.resize(rows=rows_needed, cols=cols_needed)

    values = dataframe_to_values(df)

    # Write header
    ws.update(
        range_name="A1",
        values=[values[0]],
        value_input_option="USER_ENTERED",
    )

    # Write body in chunks
    body = values[1:]

    for start in range(0, len(body), chunk_size):
        chunk = body[start:start + chunk_size]
        start_row = start + 2

        ws.update(
            range_name=f"A{start_row}",
            values=chunk,
            value_input_option="USER_ENTERED",
        )


def read_worksheet_as_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)

    # Remove fully empty rows
    df = df.dropna(how="all")
    df = df.loc[
        ~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")
    ]

    return df


# ============================================================
# Flexible reference sheet detection
# ============================================================

LABEL_COLUMN_ALIASES = {
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

    "region": "label_region",
    "island": "label_region",

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

    "zone": "zone",
}


def canonicalize_label_columns(columns):
    normalized = [normalize_column_name(c) for c in columns]
    canonical = [LABEL_COLUMN_ALIASES.get(c, c) for c in normalized]
    return canonical


def reference_header_score(columns):
    canonical = canonicalize_label_columns(columns)

    required = {
        "resource_name",
        "plant_name",
        "unit_generator",
        "location",
        "fuel",
        "operator_owner",
    }

    return len(required.intersection(set(canonical)))


def read_reference_worksheet_as_df(ws) -> pd.DataFrame:
    """
    Reads a reference worksheet and auto-detects the header row.
    Useful when the reference sheet has title rows or notes above the table.
    """

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

    if best_score < 3:
        # Fallback to first row if nothing good is detected.
        best_header_row = 0

    header = values[best_header_row]
    rows = values[best_header_row + 1:]

    df = pd.DataFrame(rows, columns=header)

    df = df.dropna(how="all")
    df = df.loc[
        ~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")
    ]

    return df


def get_reference_worksheet(labels_sh, preferred_title: str):
    worksheets = labels_sh.worksheets()
    available_titles = [ws.title for ws in worksheets]

    print(f"Available tabs in reference spreadsheet '{labels_sh.title}': {available_titles}")

    if not worksheets:
        raise ValueError("The reference Google Sheet has no tabs.")

    # 1. Try exact title match
    for ws in worksheets:
        if ws.title == preferred_title:
            print(f"Using reference tab by exact match: {ws.title}")
            return ws

    # 2. Try stripped/case-insensitive title match
    preferred_clean = preferred_title.strip().lower()

    for ws in worksheets:
        if ws.title.strip().lower() == preferred_clean:
            print(f"Using reference tab by flexible title match: {ws.title}")
            return ws

    # 3. Auto-detect by headers
    best_ws = None
    best_score = -1

    for ws in worksheets:
        values = ws.get_all_values()

        if not values:
            continue

        local_best_score = -1

        for row in values[:10]:
            score = reference_header_score(row)
            local_best_score = max(local_best_score, score)

        print(f"Reference tab candidate '{ws.title}' header score: {local_best_score}")

        if local_best_score > best_score:
            best_score = local_best_score
            best_ws = ws

    if best_ws is not None and best_score >= 3:
        print(
            f"WARNING: Reference tab '{preferred_title}' was not found. "
            f"Auto-detected reference tab: '{best_ws.title}'"
        )
        return best_ws

    # 4. Last fallback
    print(
        f"WARNING: Could not confidently detect reference tab. "
        f"Using first tab: '{worksheets[0].title}'"
    )

    return worksheets[0]


# ============================================================
# Data loading
# ============================================================

def load_raw_data(main_sh) -> pd.DataFrame:
    ws = main_sh.worksheet(RAW_DATA_TAB)
    df = read_worksheet_as_df(ws)

    if df.empty:
        raise ValueError(f"The '{RAW_DATA_TAB}' tab is empty.")

    df.columns = [normalize_column_name(c) for c in df.columns]

    required = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_name",
        "marginal_price",
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required columns in raw data tab '{RAW_DATA_TAB}': {missing}. "
            f"Found columns: {df.columns.tolist()}"
        )

    df["resource_name"] = df["resource_name"].astype(str).str.strip()
    df["resource_key"] = df["resource_name"].apply(normalize_resource_name)

    df["time_interval"] = pd.to_datetime(df["time_interval"], errors="coerce")

    df["marginal_price"] = (
        df["marginal_price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace("", pd.NA)
    )

    df["marginal_price"] = pd.to_numeric(df["marginal_price"], errors="coerce")

    return df


def load_resource_labels(labels_sh) -> pd.DataFrame:
    ws = get_reference_worksheet(labels_sh, REFERENCE_TAB)
    labels = read_reference_worksheet_as_df(ws)

    if labels.empty:
        raise ValueError(f"The reference tab '{ws.title}' is empty.")

    labels.columns = canonicalize_label_columns(labels.columns)

    required = [
        "resource_name",
        "plant_name",
        "unit_generator",
        "label_region",
        "location",
        "fuel",
        "operator_owner",
    ]

    missing = [c for c in required if c not in labels.columns]

    if missing:
        raise ValueError(
            f"Missing required columns in reference tab '{ws.title}': {missing}. "
            f"Found columns after cleaning: {labels.columns.tolist()}"
        )

    keep_cols = [
        "resource_name",
        "plant_name",
        "unit_generator",
        "label_region",
        "location",
        "fuel",
        "operator_owner",
    ]

    if "zone" in labels.columns:
        keep_cols.insert(1, "zone")

    labels = labels[keep_cols].copy()

    for col in labels.columns:
        labels[col] = labels[col].astype(str).str.strip()

    labels["resource_key"] = labels["resource_name"].apply(normalize_resource_name)

    labels = labels[labels["resource_key"] != ""]
    labels = labels.drop_duplicates(subset=["resource_key"], keep="first")

    return labels


# ============================================================
# Conglomerate grouping
# ============================================================

def classify_conglomerate(operator_owner: str) -> str:
    """
    Editable grouping logic.

    This groups obvious related operators into bigger parent groups.
    You can refine this later as you clean your reference sheet.
    """

    text = str(operator_owner).strip()

    if not text or text.lower() in ["nan", "none", "unmapped"]:
        return "Unmapped"

    t = text.upper()

    if any(k in t for k in ["ABOITIZ", "THERMA", "HEDCOR", "SN ABOITIZ", "AP RENEWABLES"]):
        return "Aboitiz Power Group"

    if any(k in t for k in ["SMC", "SAN MIGUEL", "LIMAY", "MASINLOC"]):
        return "SMC Global Power Group"

    if any(k in t for k in ["FIRST GEN", "FGP", "EDC", "ENERGY DEVELOPMENT CORP"]):
        return "First Gen / Lopez Group"

    if any(k in t for k in ["MERALCO", "MGEN", "GLOBAL BUSINESS POWER", "PANAY ENERGY", "CEBU ENERGY"]):
        return "Meralco PowerGen / GBP Group"

    if any(k in t for k in ["ACEN", "AYALA"]):
        return "ACEN / Ayala Group"

    if any(k in t for k in ["DMCI", "SEMIRARA"]):
        return "DMCI / Semirara Group"

    if any(k in t for k in ["GNPOWER", "AC ENERGY"]):
        return "GNPower / AC Energy Group"

    if "PSALM" in t:
        return "PSALM"

    if "NPC" in t or "NATIONAL POWER" in t:
        return "National Power Corporation"

    return text


# ============================================================
# Data preparation
# ============================================================

def prepare_data(raw: pd.DataFrame, labels: pd.DataFrame):
    merged = raw.merge(
        labels.drop(columns=["resource_name"], errors="ignore"),
        on="resource_key",
        how="left",
    )

    if "label_region" in merged.columns:
        merged["region_name"] = merged["region_name"].replace("", pd.NA)
        merged["region_name"] = merged["region_name"].fillna(merged["label_region"])

    merged["plant_name"] = merged["plant_name"].replace("", pd.NA).fillna(merged["resource_name"])
    merged["unit_generator"] = merged["unit_generator"].replace("", pd.NA).fillna("Unmapped")
    merged["location"] = merged["location"].replace("", pd.NA).fillna("Unmapped")
    merged["fuel"] = merged["fuel"].replace("", pd.NA).fillna("Unmapped")
    merged["operator_owner"] = merged["operator_owner"].replace("", pd.NA).fillna("Unmapped")

    merged["conglomerate_group"] = merged["operator_owner"].apply(classify_conglomerate)

    unmatched = merged[
        (merged["fuel"] == "Unmapped") |
        (merged["operator_owner"] == "Unmapped") |
        (merged["unit_generator"] == "Unmapped")
    ][["resource_name"]].drop_duplicates()

    unmatched = unmatched.sort_values("resource_name").reset_index(drop=True)

    merged = merged.drop(
        columns=[
            "resource_type",
            "is_battery",
            "resource_key",
            "label_region",
        ],
        errors="ignore",
    )

    preferred_cols = [
        "time_interval",
        "region_name",
        "commodity_type",
        "resource_name",
        "plant_name",
        "unit_generator",
        "location",
        "fuel",
        "operator_owner",
        "conglomerate_group",
        "marginal_price",
        "source",
        "source_url",
        "ingested_at_utc",
    ]

    existing_preferred = [c for c in preferred_cols if c in merged.columns]
    remaining = [c for c in merged.columns if c not in existing_preferred]

    prepared = merged[existing_preferred + remaining].copy()

    prepared["time_interval"] = pd.to_datetime(prepared["time_interval"], errors="coerce")

    prepared = prepared.sort_values(
        by=["time_interval", "region_name", "commodity_type", "resource_name"],
        na_position="last",
    )

    prepared["time_interval"] = prepared["time_interval"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return prepared, unmatched


# ============================================================
# Insight tables
# ============================================================

def build_insights(prepared: pd.DataFrame):
    df = prepared.copy()

    df["marginal_price"] = pd.to_numeric(df["marginal_price"], errors="coerce")

    required_text_cols = [
        "fuel",
        "operator_owner",
        "conglomerate_group",
        "plant_name",
        "region_name",
        "resource_name",
    ]

    for col in required_text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unmapped").replace("", "Unmapped")

    # --------------------------------------------------------
    # 1. Top producers by all fuel types
    # --------------------------------------------------------

    top_producers_by_fuel = (
        df.groupby(["fuel", "operator_owner"], dropna=False)
        .agg(
            unique_resources=("resource_name", "nunique"),
            unique_plants=("plant_name", "nunique"),
            record_count=("resource_name", "count"),
            avg_marginal_price=("marginal_price", "mean"),
            max_marginal_price=("marginal_price", "max"),
        )
        .reset_index()
    )

    top_producers_by_fuel["rank_within_fuel"] = (
        top_producers_by_fuel
        .groupby("fuel")["unique_resources"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    top_producers_by_fuel = top_producers_by_fuel.sort_values(
        ["fuel", "rank_within_fuel", "unique_resources", "record_count"],
        ascending=[True, True, False, False],
    )

    top_producers_by_fuel["avg_marginal_price"] = top_producers_by_fuel["avg_marginal_price"].round(2)
    top_producers_by_fuel["max_marginal_price"] = top_producers_by_fuel["max_marginal_price"].round(2)

    # --------------------------------------------------------
    # 2. Top producers for selected fuel type
    # --------------------------------------------------------

    selected_fuel = TOP_FUEL_FILTER

    selected = top_producers_by_fuel[
        top_producers_by_fuel["fuel"].str.lower() == selected_fuel.lower()
    ].copy()

    if selected.empty:
        fallback_fuel = df["fuel"].value_counts().index[0]
        print(
            f"WARNING: TOP_FUEL_FILTER='{TOP_FUEL_FILTER}' was not found. "
            f"Using fallback fuel: '{fallback_fuel}'"
        )

        selected_fuel = fallback_fuel

        selected = top_producers_by_fuel[
            top_producers_by_fuel["fuel"].str.lower() == selected_fuel.lower()
        ].copy()

    top_selected_fuel = selected.sort_values(
        ["unique_resources", "record_count"],
        ascending=[False, False],
    ).head(15)

    top_selected_fuel = top_selected_fuel[
        [
            "operator_owner",
            "unique_resources",
            "unique_plants",
            "record_count",
            "avg_marginal_price",
            "max_marginal_price",
        ]
    ].copy()

    top_selected_fuel.insert(0, "fuel_filter", selected_fuel)

    # --------------------------------------------------------
    # 3. Fuel by island heatmap
    # --------------------------------------------------------

    heatmap_fuel_by_island = pd.pivot_table(
        df,
        index="fuel",
        columns="region_name",
        values="resource_name",
        aggfunc=pd.Series.nunique,
        fill_value=0,
    )

    for island in ["Luzon", "Visayas", "Mindanao"]:
        if island not in heatmap_fuel_by_island.columns:
            heatmap_fuel_by_island[island] = 0

    heatmap_fuel_by_island = heatmap_fuel_by_island[["Luzon", "Visayas", "Mindanao"]]
    heatmap_fuel_by_island["Total"] = heatmap_fuel_by_island.sum(axis=1)
    heatmap_fuel_by_island = heatmap_fuel_by_island.reset_index().sort_values("Total", ascending=False)

    # --------------------------------------------------------
    # 4. Top producers by selected fuel per island heatmap
    # --------------------------------------------------------

    selected_fuel_df = df[df["fuel"].str.lower() == selected_fuel.lower()].copy()

    heatmap_top_producers_by_island = pd.pivot_table(
        selected_fuel_df,
        index="operator_owner",
        columns="region_name",
        values="resource_name",
        aggfunc=pd.Series.nunique,
        fill_value=0,
    )

    for island in ["Luzon", "Visayas", "Mindanao"]:
        if island not in heatmap_top_producers_by_island.columns:
            heatmap_top_producers_by_island[island] = 0

    heatmap_top_producers_by_island = heatmap_top_producers_by_island[
        ["Luzon", "Visayas", "Mindanao"]
    ]

    heatmap_top_producers_by_island["Total"] = heatmap_top_producers_by_island.sum(axis=1)

    heatmap_top_producers_by_island = (
        heatmap_top_producers_by_island
        .reset_index()
        .sort_values("Total", ascending=False)
        .head(30)
    )

    heatmap_top_producers_by_island.insert(0, "fuel_filter", selected_fuel)

    # --------------------------------------------------------
    # 5. Market concentration by conglomerate/operator group
    # --------------------------------------------------------

    concentration = (
        df.groupby("conglomerate_group", dropna=False)
        .agg(
            unique_resources=("resource_name", "nunique"),
            unique_plants=("plant_name", "nunique"),
            fuel_types=("fuel", lambda x: ", ".join(sorted(set(x.astype(str))))),
            islands=("region_name", lambda x: ", ".join(sorted(set(x.astype(str))))),
            record_count=("resource_name", "count"),
            avg_marginal_price=("marginal_price", "mean"),
        )
        .reset_index()
    )

    total_resources = concentration["unique_resources"].sum()

    if total_resources > 0:
        concentration["resource_share_pct"] = (
            concentration["unique_resources"] / total_resources * 100
        ).round(2)
    else:
        concentration["resource_share_pct"] = 0

    concentration["avg_marginal_price"] = concentration["avg_marginal_price"].round(2)

    concentration = concentration.sort_values(
        ["unique_resources", "resource_share_pct", "record_count"],
        ascending=[False, False, False],
    )

    chart_market_concentration = concentration[
        [
            "conglomerate_group",
            "unique_resources",
            "unique_plants",
            "resource_share_pct",
            "record_count",
        ]
    ].head(20)

    # --------------------------------------------------------
    # 6. Simple summary notes
    # --------------------------------------------------------

    summary_rows = []

    if not top_selected_fuel.empty:
        top_row = top_selected_fuel.iloc[0]

        summary_rows.append({
            "insight": "Top producer for selected fuel",
            "value": (
                f"For {selected_fuel}, the top mapped operator/owner is "
                f"{top_row['operator_owner']} with "
                f"{top_row['unique_resources']} unique resources."
            ),
        })

    if not concentration.empty:
        top_cong = concentration.iloc[0]

        summary_rows.append({
            "insight": "Largest mapped conglomerate/operator group",
            "value": (
                f"{top_cong['conglomerate_group']} has the largest mapped resource count "
                f"with {top_cong['unique_resources']} unique resources, equal to "
                f"{top_cong['resource_share_pct']}% of mapped resources."
            ),
        })

    unmapped_count = df[df["fuel"] == "Unmapped"]["resource_name"].nunique()

    summary_rows.append({
        "insight": "Unmapped resources",
        "value": f"There are {unmapped_count} unique resources without mapped fuel information.",
    })

    summary_notes = pd.DataFrame(summary_rows)

    return {
        "insight_summary_notes": summary_notes,
        "insight_top_producers_by_fuel": top_producers_by_fuel,
        "chart_top_selected_fuel": top_selected_fuel,
        "heatmap_fuel_by_island": heatmap_fuel_by_island,
        "heatmap_top_producers_by_island": heatmap_top_producers_by_island,
        "insight_market_concentration": concentration,
        "chart_market_concentration": chart_market_concentration,
    }


# ============================================================
# Google Sheets formatting and charts
# ============================================================

def delete_existing_charts(spreadsheet, dashboard_ws):
    metadata = spreadsheet.fetch_sheet_metadata()
    requests = []

    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})

        if props.get("sheetId") != dashboard_ws.id:
            continue

        for chart in sheet.get("charts", []):
            requests.append({
                "deleteEmbeddedObject": {
                    "objectId": chart["chartId"]
                }
            })

    if requests:
        spreadsheet.batch_update({"requests": requests})


def clear_conditional_format_rules(spreadsheet, ws):
    metadata = spreadsheet.fetch_sheet_metadata()
    rule_count = 0

    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})

        if props.get("sheetId") == ws.id:
            rule_count = len(sheet.get("conditionalFormats", []))
            break

    if rule_count == 0:
        return

    requests = []

    # Delete from index 0 repeatedly because rules shift after each delete.
    for _ in range(rule_count):
        requests.append({
            "deleteConditionalFormatRule": {
                "sheetId": ws.id,
                "index": 0,
            }
        })

    spreadsheet.batch_update({"requests": requests})


def apply_heatmap_formatting(spreadsheet, heatmap_ws, heatmap_df):
    if heatmap_df.empty:
        return

    clear_conditional_format_rules(spreadsheet, heatmap_ws)

    # Numeric heatmap columns are usually B:D or C:E depending on table.
    # Detect Luzon, Visayas, Mindanao column positions.
    cols = list(heatmap_df.columns)

    heatmap_cols = []

    for col_name in ["Luzon", "Visayas", "Mindanao"]:
        if col_name in cols:
            heatmap_cols.append(cols.index(col_name))

    if not heatmap_cols:
        return

    start_col = min(heatmap_cols)
    end_col = max(heatmap_cols) + 1

    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {
                            "sheetId": heatmap_ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": len(heatmap_df) + 1,
                            "startColumnIndex": start_col,
                            "endColumnIndex": end_col,
                        }
                    ],
                    "gradientRule": {
                        "minpoint": {
                            "type": "MIN",
                            "color": {
                                "red": 1.0,
                                "green": 1.0,
                                "blue": 1.0,
                            },
                        },
                        "midpoint": {
                            "type": "PERCENTILE",
                            "value": "50",
                            "color": {
                                "red": 1.0,
                                "green": 0.9,
                                "blue": 0.6,
                            },
                        },
                        "maxpoint": {
                            "type": "MAX",
                            "color": {
                                "red": 0.9,
                                "green": 0.3,
                                "blue": 0.2,
                            },
                        },
                    },
                },
                "index": 0,
            }
        }
    ]

    spreadsheet.batch_update({"requests": requests})


def add_bar_chart(
    spreadsheet,
    dashboard_ws,
    source_ws,
    title: str,
    domain_col_index: int,
    value_col_index: int,
    start_row_index: int,
    end_row_index: int,
    anchor_row_index: int,
    anchor_col_index: int,
    bottom_axis_title: str,
    left_axis_title: str,
):
    if end_row_index <= start_row_index:
        return

    request = {
        "addChart": {
            "chart": {
                "spec": {
                    "title": title,
                    "basicChart": {
                        "chartType": "BAR",
                        "legendPosition": "NO_LEGEND",
                        "axis": [
                            {
                                "position": "BOTTOM_AXIS",
                                "title": bottom_axis_title,
                            },
                            {
                                "position": "LEFT_AXIS",
                                "title": left_axis_title,
                            },
                        ],
                        "domains": [
                            {
                                "domain": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": source_ws.id,
                                                "startRowIndex": start_row_index,
                                                "endRowIndex": end_row_index,
                                                "startColumnIndex": domain_col_index,
                                                "endColumnIndex": domain_col_index + 1,
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "series": [
                            {
                                "series": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": source_ws.id,
                                                "startRowIndex": start_row_index,
                                                "endRowIndex": end_row_index,
                                                "startColumnIndex": value_col_index,
                                                "endColumnIndex": value_col_index + 1,
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "headerCount": 0,
                    },
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {
                            "sheetId": dashboard_ws.id,
                            "rowIndex": anchor_row_index,
                            "columnIndex": anchor_col_index,
                        },
                        "offsetXPixels": 0,
                        "offsetYPixels": 0,
                        "widthPixels": 650,
                        "heightPixels": 380,
                    }
                },
            }
        }
    }

    spreadsheet.batch_update({"requests": [request]})


def create_dashboard(spreadsheet, insight_tables):
    dashboard_ws = get_or_create_worksheet(
        spreadsheet,
        "insights_dashboard",
        rows=100,
        cols=15,
    )

    dashboard_ws.clear()
    delete_existing_charts(spreadsheet, dashboard_ws)

    now_utc = datetime.now(timezone.utc)
    now_pht = now_utc.astimezone(timezone(timedelta(hours=8)))

    dashboard_text = [
        ["IEMOP Data Preparation Dashboard"],
        [f"Last generated UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}"],
        [f"Last generated PHT: {now_pht.strftime('%Y-%m-%d %H:%M:%S')}"],
        [""],
        ["Charts created from data_prepared and insight tabs."],
        ["Important: these insights are based on mapped resources and records, not actual MWh generation volume."],
        [f"Selected fuel filter: {TOP_FUEL_FILTER}"],
    ]

    dashboard_ws.update(
        range_name="A1",
        values=dashboard_text,
        value_input_option="USER_ENTERED",
    )

    selected_fuel_ws = spreadsheet.worksheet("chart_top_selected_fuel")
    concentration_ws = spreadsheet.worksheet("chart_market_concentration")

    selected_rows = len(insight_tables["chart_top_selected_fuel"]) + 1
    concentration_rows = len(insight_tables["chart_market_concentration"]) + 1

    if selected_rows > 1:
        add_bar_chart(
            spreadsheet=spreadsheet,
            dashboard_ws=dashboard_ws,
            source_ws=selected_fuel_ws,
            title=f"Top Producers for Selected Fuel",
            domain_col_index=1,
            value_col_index=2,
            start_row_index=1,
            end_row_index=selected_rows,
            anchor_row_index=9,
            anchor_col_index=0,
            bottom_axis_title="Unique Resources",
            left_axis_title="Operator / Owner",
        )

    if concentration_rows > 1:
        add_bar_chart(
            spreadsheet=spreadsheet,
            dashboard_ws=dashboard_ws,
            source_ws=concentration_ws,
            title="Market Concentration by Conglomerate / Operator Group",
            domain_col_index=0,
            value_col_index=1,
            start_row_index=1,
            end_row_index=concentration_rows,
            anchor_row_index=9,
            anchor_col_index=7,
            bottom_axis_title="Unique Resources",
            left_axis_title="Conglomerate / Operator Group",
        )


# ============================================================
# Metadata
# ============================================================

def update_prep_metadata(spreadsheet, prepared_rows: int, unmatched_rows: int):
    ws_meta = get_or_create_worksheet(spreadsheet, "metadata", rows=30, cols=3)

    utc_ts = datetime.now(timezone.utc)
    pht_ts = utc_ts.astimezone(timezone(timedelta(hours=8)))

    values = ws_meta.get_all_values()
    col_a = [row[0].strip() for row in values if row and len(row) > 0]

    def upsert(label, value):
        nonlocal col_a

        if label in col_a:
            row_idx = col_a.index(label) + 1
            ws_meta.update(
                range_name=f"B{row_idx}",
                values=[[value]],
                value_input_option="USER_ENTERED",
            )
        else:
            next_row = len(col_a) + 1
            ws_meta.update(
                range_name=f"A{next_row}:B{next_row}",
                values=[[label, value]],
                value_input_option="USER_ENTERED",
            )
            col_a.append(label)

    upsert("data_prep_last_updated_utc", utc_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prep_last_updated_pht", pht_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prepared_rows", prepared_rows)
    upsert("unmatched_resource_count", unmatched_rows)
    upsert("reference_gsheet_id", RESOURCE_LABELS_GSHEET_ID)
    upsert("raw_data_tab", RAW_DATA_TAB)
    upsert("prepared_data_tab", PREPARED_TAB)
    upsert("selected_fuel_filter", TOP_FUEL_FILTER)


# ============================================================
# Main
# ============================================================

def main():
    print("Connecting to main Google Sheet...")
    main_sh = connect_gsheet(MAIN_GSHEET_ID)

    print("Connecting to resource label Google Sheet...")
    labels_sh = connect_gsheet(RESOURCE_LABELS_GSHEET_ID)

    print("Loading raw data...")
    raw = load_raw_data(main_sh)
    print(f"Raw rows loaded: {len(raw):,}")

    print("Loading resource labels...")
    labels = load_resource_labels(labels_sh)
    print(f"Resource labels loaded: {len(labels):,}")

    print("Preparing data...")
    prepared, unmatched = prepare_data(raw, labels)

    print(f"Prepared rows: {len(prepared):,}")
    print(f"Unmatched resources: {len(unmatched):,}")

    print(f"Writing prepared data to '{PREPARED_TAB}'...")
    prepared_ws = get_or_create_worksheet(
        main_sh,
        PREPARED_TAB,
        rows=max(len(prepared) + 10, 1000),
        cols=max(len(prepared.columns), 30),
    )

    write_df_to_worksheet(prepared_ws, prepared)

    print("Writing unmatched resources...")
    unmatched_ws = get_or_create_worksheet(
        main_sh,
        "unmatched_resources",
        rows=max(len(unmatched) + 10, 100),
        cols=5,
    )

    write_df_to_worksheet(unmatched_ws, unmatched)

    print("Building insight tables...")
    insight_tables = build_insights(prepared)

    for tab_name, insight_df in insight_tables.items():
        print(f"Writing {tab_name}...")

        ws = get_or_create_worksheet(
            main_sh,
            tab_name,
            rows=max(len(insight_df) + 10, 100),
            cols=max(len(insight_df.columns), 10),
        )

        write_df_to_worksheet(ws, insight_df)

    print("Applying heatmap formatting...")

    heatmap_fuel_ws = main_sh.worksheet("heatmap_fuel_by_island")
    apply_heatmap_formatting(
        main_sh,
        heatmap_fuel_ws,
        insight_tables["heatmap_fuel_by_island"],
    )

    heatmap_producers_ws = main_sh.worksheet("heatmap_top_producers_by_island")
    apply_heatmap_formatting(
        main_sh,
        heatmap_producers_ws,
        insight_tables["heatmap_top_producers_by_island"],
    )

    print("Creating dashboard charts...")
    create_dashboard(main_sh, insight_tables)

    print("Updating metadata...")
    update_prep_metadata(
        spreadsheet=main_sh,
        prepared_rows=len(prepared),
        unmatched_rows=len(unmatched),
    )

    print("Data preparation complete.")


if __name__ == "__main__":
    main()
