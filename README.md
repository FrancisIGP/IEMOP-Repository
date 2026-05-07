# IPV Project Repository (IEMOP Reserve Market Data)

This repository contains a simple data pipeline that downloads reserve market data from IEMOP, combines the files into a single dataset, and processes the data for analysis and visualization.

## What this project does
- Downloads publicly available IEMOP reserve market clearing price CSV files.
- Combines multiple daily files into one consolidated CSV.
- Cleans and processes the dataset in a notebook to generate basic summaries and visual checks.
- Serves as the data source for a future dashboard / MVP.

## Repository contents
- `download_iemop.py`  
  Downloads IEMOP reserve market files and outputs a combined dataset (e.g., `iemop_combined.csv`).

- `data2_processing.ipynb`  
  Loads the combined CSV, performs cleaning/transformations, and generates initial analysis/plots.

## Requirements
- Python 3.9+ recommended
- Packages:
  - `pandas`
  - `requests`
  - `tqdm` (optional, for progress display)
  - `matplotlib` (notebook plots)

You can install dependencies using:
```bash
pip install pandas requests tqdm matplotlib
