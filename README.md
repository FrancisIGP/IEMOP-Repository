# IPV Dynamic Dashboard - IEMOP Reserve Market

This repository contains a zero-cost, automated data pipeline that collects Philippine IEMOP reserve market clearing price data, appends it to a live Google Sheet, and powers a Tableau Public dashboard that updates without manual file uploads.

### Project Goal
This project aims to make Philippine electricity reserve market pricing more transparent and easier to monitor through a live dashboard, helping users track trends and potential price spikes over time.

### Repository Structure
- `download_iemop.py`, fetches IEMOP RTD reserve market CSVs and exposes `fetch_iemop_data()`
- `pipeline_to_sheets.py`, runs the end-to-end pipeline and appends new rows to Google Sheets
- `requirements.txt`, Python dependencies
- `.github/workflows/pipeline.yml`, GitHub Actions schedule and automation
- `assets/`, proof screenshots used in the README

### Google Sheet Tab Structure
- `data`, main live table used by Tableau Public
- `metadata`, contains `last_updated_utc` timestamp updated by the pipeline

### Database Update Schedule
The live Google Sheet database is updated automatically via GitHub Actions on a daily schedule.

- Schedule, `0 23 * * *` (cron)
- Runs at, 23:00 UTC daily, which is 07:00 Philippine time (PHT) daily
- What updates, the pipeline appends new rows to the Google Sheet `data` tab and updates the `metadata` timestamp

## Proof of Automation
GitHub Actions run proof and workflow file. See live links for to view the output.

![GitHub Actions run success](assets/actions_run_success.png)

### How to Test the Pipeline (5 steps)
1. Go to the repository, Actions tab  
3. Click Run workflow  
4. Verify the logs show Appended new rows  
5. Check the Google Sheet `data` tab for new rows, check `metadata` for updated timestamp

### Pipeline Overview
GitHub Actions (scheduled) → Python scraper → Google Sheets (storage) → Tableau Public (dashboard refresh)

### Live Links
- [Google Sheet (Data Store)](https://docs.google.com/spreadsheets/d/1jyvx2Jh8jGOVpKoJ9tw1auh-thOSdRAVYVpUjhb3kMM/edit?gid=1648105924#gid=1648105924)
- [Tableau Public Dashboard](https://public.tableau.com/views/IEMOPDashboard/Sheet1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link), Work in Progress

## Data Source
IEMOP Market Data (RTD reserve market clearing price)  
https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/
