# IPV Dynamic Dashboard — IEMOP Reserve Market (Philippines) *(Work in Progress)*

This repository contains a zero-cost, automated data pipeline that collects Philippine IEMOP reserve market clearing price data, appends it to a live Google Sheet, and powers a Tableau Public dashboard that updates without manual file uploads.

## Live Links
- **Google Sheet (Live Data Store):** [Open the live sheet](https://docs.google.com/spreadsheets/d/1jyvx2Jh8jGOVpKoJ9tw1auh-thOSdRAVYVpUjhb3kMM/edit?gid=1648105924#gid=1648105924)
- **Tableau Public Dashboard (Live):** [View the dashboard](https://public.tableau.com/views/IEMOPDashboard/Sheet1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

## Project Goal (Philippines Context)
This project aims to make Philippine electricity reserve market pricing more transparent and easier to monitor through a live dashboard, helping users track trends and potential price spikes over time.

## Pipeline Overview
**GitHub Actions (scheduled) → Python scraper → Google Sheets (storage) → Tableau Public (dashboard refresh)**

## Repository Files
- `download_iemop.py`  
  Extracts IEMOP reserve market data and exposes a callable `fetch_iemop_data()` function for the pipeline.

- `pipeline_to_sheets.py`  
  Runs the end-to-end pipeline: fetches data, cleans/standardizes fields, appends only new rows to Google Sheets, and updates the last updated timestamp.

- `requirements.txt`  
  Python dependencies for local runs and GitHub Actions.

- `.github/workflows/pipeline.yml`  
  Scheduled automation that runs the pipeline without needing a personal computer.

## Data Source
IEMOP Market Data (RTD reserve market clearing price):  
https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/

## Google Sheet Structure
The sheet contains:
- `data` tab: the live appended dataset used by Tableau
- `metadata` tab: contains `last_updated_utc` for dashboard timestamp display

## How to Run the Pipeline (Maintainers / Developers)
### Local run (optional)
1) Install dependencies:
```bash
pip install -r requirements.txt
