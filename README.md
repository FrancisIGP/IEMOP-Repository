# IEMOP Reserve Market, Dynamic Dashboard 

This repository contains a zero-cost, automated data pipeline that collects Philippine IEMOP reserve market clearing price data, appends it to a live Google Sheet, and powers a Tableau Public dashboard that updates without manual file uploads.

### Project Goal
This project aims to make Philippine electricity reserve market pricing more transparent and easier to monitor through a live dashboard, helping users track trends and potential price spikes over time.

### Repository Structure
- `download_iemop.py`  
  Fetches IEMOP RTD reserve market CSV data and exposes the `fetch_iemop_data()` function.
- `pipeline_to_sheets.py`  
  Runs the main extraction and loading process. It retrieves new IEMOP data, cleans the raw records, deduplicates existing data, appends new rows to the Google Sheet `data` tab, and updates the `metadata` tab.
- `data_prep.py`  
  Runs after `pipeline_to_sheets.py`. It enriches the main `data` tab by matching each `resource_name` with a reference Google Sheet containing meaningful resource labels. It adds plant name, unit/generator, location, fuel type, and operator/owner. It also removes the old `resource_type` and `is_battery` columns.
- `requirements.txt`  
  Contains the Python dependencies required by the pipeline.
- `.github/workflows/pipeline.yml`  
  Contains the GitHub Actions workflow that automatically runs the pipeline on schedule or through manual trigger.
- `assets/`  
  Contains proof screenshots and supporting images used in the README.

### Google Sheet Tab Structure
- `data` - main live table used by Tableau Public after the pipeline and data preparation step run.
  - `time_interval` - the date and time of the market interval, representing the timestamp of the reserve price record.
  - `region_name` - grid region where the reserve market price applies, such as Luzon, Visayas, or Mindanao.
  - `commodity_type` - reserve service category, such as Dispatchable, Regulating Up, Regulating Down, or Contingency.
  - `resource_name` - original IEMOP name or identifier of the unit/resource in the reserve market.
  - `Plant name` - human-readable plant name matched from the reference Google Sheet.
  - `Unit/Generator` - unit, generator, or resource classification matched from the reference Google Sheet.
  - `Location` - location of the plant or resource matched from the reference Google Sheet.
  - `Fuel` - fuel or energy type of the resource, such as coal, hydro, diesel, geothermal, solar, battery, or natural gas.
  - `Operator/Owner` - operator or owner of the plant/resource matched from the reference Google Sheet.
  - `marginal_price` - reserve market clearing price for that interval and resource, using the numeric value from IEMOP.
  - `source` - data publisher, set to IEMOP.
  - `source_url` - reference page where the data is sourced from.
  - `ingested_at_utc` - UTC timestamp of when the pipeline appended the row to the Google Sheet.
- `metadata` - contains pipeline update information.
  - `last_updated_utc` - UTC timestamp updated by the main pipeline.
  - `data_prep_last_updated_utc` - UTC timestamp when `data_prep.py` last enriched the `data` tab.
  - `data_prep_last_updated_pht` - Philippine time timestamp when `data_prep.py` last enriched the `data` tab.
  - `data_prep_rows` - number of rows processed by `data_prep.py`.
  - `data_prep_unmatched_resources` - number of resource names that could not be matched with the reference sheet.
  - `reference_gsheet_id` - Google Sheet ID of the reference mapping sheet.

### Database Update Schedule
The live Google Sheet database is updated automatically via GitHub Actions on a daily schedule.

- Schedule, `0 23 * * *` (cron)
- Runs at, 23:00 UTC daily, which is 07:00 Philippine time (PHT) daily
- What updates, the pipeline appends new rows to the Google Sheet `data` tab and updates the `metadata` timestamp

## Pipeline Overview
The following sequence outlines the end-to-end architecture for synchronizing live data with the visualization platform.

**GitHub Actions (scheduled) → Python scraper → Google Sheets (storage) → Tableau Public (dashboard refresh)**

### How to Test the Pipeline
1. Go to the repository, Actions tab  
2. Select the workflow, IPV Pipeline to Google Sheets  
3. Click Run workflow  
4. Verify the logs show Appended new rows  
5. Check the Google Sheet `data` tab for new rows, check `metadata` for updated timestamp

### Proof of Automation
See the live links to view the live outputs.

![GitHub Actions run success](assets/actions_run_success.png)

### Live Links
- [Google Sheet (Data Store)](https://docs.google.com/spreadsheets/d/1jyvx2Jh8jGOVpKoJ9tw1auh-thOSdRAVYVpUjhb3kMM/edit?gid=1648105924#gid=1648105924)
- [Tableau Public Dashboard](https://public.tableau.com/views/IEMOPDashboard/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

## Data Source
IEMOP Market Data (RTD reserve market clearing price)  
https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/
