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
   - top producers per island heatmap
   - electricity market concentration by operator/conglomerate
7. Create Google Sheets charts/dashboard tabs.

Required GitHub Actions secrets:
- GCP_SA_JSON
- GSHEET_ID
"""

import os
import json
import re
from datetime import datetime, timezone, timedelta

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Config
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAIN_GSHEET_ID = os.environ["GSHEET_ID"]

# Your reference Google Sheet containing meaningful labels.
# You may also move this to a GitHub Actions secret/env named RESOURCE_LABELS_GSHEET_ID.
RESOURCE_LABELS_GSHEET_ID = os.environ.get(
    "RESOURCE_LABELS_GSHEET_ID",
    "15IeST_wPRmYbnKeCA6Sv5dutYF7dAl-6uB9zuw6URZI",
)

RAW_DATA_TAB = os.environ.get("RAW_DATA_TAB", "data")
REFERENCE_TAB = os.environ.get("REFERENCE_TAB", "External")
PREPARED_TAB = os.environ.get("PREPARED_TAB", "data_prepared")

# Optional: change this to "Coal", "Hydro", "Natural gas", "Battery (BESS)", etc.
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
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def read_worksheet_as_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)

    # Remove fully empty rows.
    df = df.dropna(how="all")
    df = df.loc[~(df.astype(str).apply(lambda row: "".join(row).strip(), axis=1) == "")]

    return df


def clean_for_sheets(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        df[col] = df[col].where(pd.notna(df[col]), "")

    return df


def write_df_to_worksheet(ws, df: pd.DataFrame):
    df = clean_for_sheets(df)

    ws.clear()

    if df.empty:
        ws.update([["No data"]], value_input_option="USER_ENTERED")
        return

    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.update(values, value_input_option="USER_ENTERED")


def normalize_column_name(col: str) -> str:
    col = str(col).strip().lower()
    col = col.replace("/", " ")
    col = col.replace("-", " ")
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def normalize_resource_name(value) -> str:
    if pd.isna(value):
        return ""

    value = str(value).strip().upper()
    value = re.sub(r"\s+", "", value)

    return value


# ============================================================
# Data loading
# ============================================================

def load_raw_data(main_sh) -> pd.DataFrame:
    ws = main_sh.worksheet(RAW_DATA_TAB)
    df = read_worksheet_as_df(ws)

    if df.empty:
        raise ValueError(f"The '{RAW_DATA_TAB}' tab is empty.")

    df.columns = [normalize_column_name(c) for c in df.columns]

    required = ["time_interval", "region_name", "commodity_type", "resource_name", "marginal_price"]
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
    ws = labels_sh.worksheet(REFERENCE_TAB)
    labels = read_worksheet_as_df(ws)

    if labels.empty:
        raise ValueError(f"The reference tab '{REFERENCE_TAB}' is empty.")

    labels.columns = [normalize_column_name(c) for c in labels.columns]

    rename_map = {
        "full_resource_name": "resource_name",
        "plant_name": "plant_name",
        "unit_generator": "unit_generator",
        "region": "label_region",
        "location": "location",
        "fuel": "fuel",
        "operator_owner": "operator_owner",
        "zone": "zone",
    }

    labels = labels.rename(columns={c: rename_map.get(c, c) for c in labels.columns})

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
            f"Missing required columns in reference tab '{REFERENCE_TAB}': {missing}. "
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

    The reference sheet usually gives operator/owner names.
    This function groups obvious related operators into parent groups.
    Adjust this as your research improves.
    """

    text = str(operator_owner).strip()

    if not text:
        return "Unknown"

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

    if "PSALM" in t:
        return "PSALM"

    return text


# ============================================================
# Data preparation
# ============================================================

def prepare_data(raw: pd.DataFrame, labels: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = raw.merge(
        labels.drop(columns=["resource_name"], errors="ignore"),
        on="resource_key",
        how="left",
    )

    # Fill region_name from reference label if raw region is missing.
    if "label_region" in merged.columns:
        merged["region_name"] = merged["region_name"].replace("", pd.NA)
        merged["region_name"] = merged["region_name"].fillna(merged["label_region"])

    # Add fallback values for unmatched resources.
    merged["plant_name"] = merged["plant_name"].fillna(merged["resource_name"])
    merged["unit_generator"] = merged["unit_generator"].fillna("Unmapped")
    merged["location"] = merged["location"].fillna("Unmapped")
    merged["fuel"] = merged["fuel"].fillna("Unmapped")
    merged["operator_owner"] = merged["operator_owner"].fillna("Unmapped")
    merged["conglomerate_group"] = merged["operator_owner"].apply(classify_conglomerate)

    # Create unmatched resources table so you can update the reference sheet later.
    unmatched = merged[
        (merged["fuel"] == "Unmapped") |
        (merged["operator_owner"] == "Unmapped") |
        (merged["unit_generator"] == "Unmapped")
    ][["resource_name"]].drop_duplicates()

    unmatched = unmatched.sort_values("resource_name").reset_index(drop=True)

    # Remove old columns requested by user.
    merged = merged.drop(
        columns=[
            "resource_type",
            "is_battery",
            "resource_key",
            "label_region",
        ],
        errors="ignore",
    )

    # Clean final column order.
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

    for col in ["fuel", "operator_owner", "conglomerate_group", "plant_name", "region_name", "resource_name"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unmapped").replace("", "Unmapped")

    # 1. Top producers by fuel type.
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

    # 2. Specific fuel selected for bar chart.
    selected_fuel = TOP_FUEL_FILTER

    selected = top_producers_by_fuel[
        top_producers_by_fuel["fuel"].str.lower() == selected_fuel.lower()
    ].copy()

    if selected.empty:
        # Fallback: use the most common fuel in the prepared data.
        fallback_fuel = df["fuel"].value_counts().index[0]
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

    # 3. Heatmap table: fuel type per island.
    heatmap = pd.pivot_table(
        df,
        index="fuel",
        columns="region_name",
        values="resource_name",
        aggfunc=pd.Series.nunique,
        fill_value=0,
    )

    for island in ["Luzon", "Visayas", "Mindanao"]:
        if island not in heatmap.columns:
            heatmap[island] = 0

    heatmap = heatmap[["Luzon", "Visayas", "Mindanao"]]
    heatmap["Total"] = heatmap.sum(axis=1)
    heatmap = heatmap.reset_index().sort_values("Total", ascending=False)

    # 4. Market concentration by conglomerate/operator group.
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

    # 5. Chart-ready concentration table.
    chart_concentration = concentration[
        ["conglomerate_group", "unique_resources", "unique_plants", "resource_share_pct", "record_count"]
    ].head(20)

    return {
        "insight_top_producers_by_fuel": top_producers_by_fuel,
        "chart_top_selected_fuel": top_selected_fuel,
        "heatmap_fuel_by_island": heatmap,
        "insight_market_concentration": concentration,
        "chart_market_concentration": chart_concentration,
    }


# ============================================================
# Google Sheets dashboard formatting/charts
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


def apply_heatmap_formatting(spreadsheet, heatmap_ws, heatmap_df):
    if heatmap_df.empty:
        return

    # Apply gradient formatting to numeric cells only:
    # columns B:D are Luzon, Visayas, Mindanao.
    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {
                            "sheetId": heatmap_ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": len(heatmap_df) + 1,
                            "startColumnIndex": 1,
                            "endColumnIndex": 4,
                        }
                    ],
                    "gradientRule": {
                        "minpoint": {
                            "type": "MIN",
                            "color": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        },
                        "midpoint": {
                            "type": "PERCENTILE",
                            "value": "50",
                            "color": {"red": 1.0, "green": 0.9, "blue": 0.6},
                        },
                        "maxpoint": {
                            "type": "MAX",
                            "color": {"red": 0.9, "green": 0.3, "blue": 0.2},
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
                        "widthPixels": 700,
                        "heightPixels": 400,
                    }
                },
            }
        }
    }

    spreadsheet.batch_update({"requests": [request]})


def create_dashboard(spreadsheet, insight_tables):
    dashboard_ws = get_or_create_worksheet(spreadsheet, "insights_dashboard", rows=80, cols=12)
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
        ["Note: producer/conglomerate dominance is estimated from unique mapped resources, not actual MWh generation."],
    ]

    dashboard_ws.update(dashboard_text, value_input_option="USER_ENTERED")

    # Create charts from chart-ready sheets.
    selected_fuel_ws = spreadsheet.worksheet("chart_top_selected_fuel")
    concentration_ws = spreadsheet.worksheet("chart_market_concentration")

    selected_rows = len(insight_tables["chart_top_selected_fuel"]) + 1
    concentration_rows = len(insight_tables["chart_market_concentration"]) + 1

    if selected_rows > 1:
        add_bar_chart(
            spreadsheet=spreadsheet,
            dashboard_ws=dashboard_ws,
            source_ws=selected_fuel_ws,
            title=f"Top Producers for Selected Fuel: {TOP_FUEL_FILTER}",
            domain_col_index=1,
            value_col_index=2,
            start_row_index=1,
            end_row_index=selected_rows,
            anchor_row_index=8,
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
            anchor_row_index=8,
            anchor_col_index=6,
            bottom_axis_title="Unique Resources",
            left_axis_title="Conglomerate / Operator Group",
        )


# ============================================================
# Metadata
# ============================================================

def update_prep_metadata(spreadsheet, prepared_rows: int, unmatched_rows: int):
    ws_meta = get_or_create_worksheet(spreadsheet, "metadata", rows=20, cols=3)

    utc_ts = datetime.now(timezone.utc)
    pht_ts = utc_ts.astimezone(timezone(timedelta(hours=8)))

    values = ws_meta.get_all_values()
    col_a = [row[0].strip() for row in values if row]

    def upsert(label, value):
        nonlocal col_a

        if label in col_a:
            row_idx = col_a.index(label) + 1
            ws_meta.update(range_name=f"B{row_idx}", values=[[value]])
        else:
            next_row = len(col_a) + 1
            ws_meta.update(range_name=f"A{next_row}", values=[[label]])
            ws_meta.update(range_name=f"B{next_row}", values=[[value]])
            col_a.append(label)

    upsert("data_prep_last_updated_utc", utc_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prep_last_updated_pht", pht_ts.strftime("%Y-%m-%d %H:%M:%S"))
    upsert("data_prepared_rows", prepared_rows)
    upsert("unmatched_resource_count", unmatched_rows)


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
    prepared_ws = get_or_create_worksheet(main_sh, PREPARED_TAB, rows=max(len(prepared) + 10, 1000), cols=30)
    write_df_to_worksheet(prepared_ws, prepared)

    print("Writing unmatched resources...")
    unmatched_ws = get_or_create_worksheet(main_sh, "unmatched_resources", rows=max(len(unmatched) + 10, 100), cols=5)
    write_df_to_worksheet(unmatched_ws, unmatched)

    print("Building insight tables...")
    insight_tables = build_insights(prepared)

    for tab_name, insight_df in insight_tables.items():
        print(f"Writing {tab_name}...")
        ws = get_or_create_worksheet(main_sh, tab_name, rows=max(len(insight_df) + 10, 100), cols=30)
        write_df_to_worksheet(ws, insight_df)

    print("Applying heatmap formatting...")
    heatmap_ws = main_sh.worksheet("heatmap_fuel_by_island")
    apply_heatmap_formatting(main_sh, heatmap_ws, insight_tables["heatmap_fuel_by_island"])

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
