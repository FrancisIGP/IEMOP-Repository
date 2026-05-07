# IPV Dynamic Dashboard — IEMOP Reserve Market (Philippines)

This repository powers a zero-cost, automated data pipeline that collects IEMOP reserve market clearing price data (Philippines), writes it to a live Google Sheet, and feeds a deployed Tableau Public dashboard that updates without manual uploads.

## Live Links
- **Google Sheet (Live Data Store):** <br>https://docs.google.com/spreadsheets/d/1jyvx2Jh8jGOVpKoJ9tw1auh-thOSdRAVYVpUjhb3kMM/edit?gid=1648105924#gid=1648105924</br>
- **Tableau Public Dashboard (Live):** <br>https://public.tableau.com/newWorkbook/bdb956ee-0139-45a8-98c5-d9da21f75879#2</br>

## Project Goal
The project aims to make Philippine energy market reserve price data easier to monitor through a live dashboard, helping viewers observe trends, spikes, and patterns over time.

## Pipeline Overview
**GitHub Actions (scheduled) → Python scraper → Google Sheets (storage) → Tableau Public (dashboard refresh)**

## Repository Files
- `download_iemop.py`  
  Extracts IEMOP reserve market data and provides a callable `fetch_iemop_data()` function for the pipeline.

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

## How to Run Locally (Optional)
1) Install dependencies:
```bash
pip install -r requirements.txt
