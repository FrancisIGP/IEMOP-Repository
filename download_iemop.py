import requests, base64, pandas as pd, io, re
from datetime import datetime, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_URL = "https://www.iemop.ph/market-data/rtd-reserve-market-clearing-price/"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

all_dfs, current_date, missing_count = [], datetime.today(), 0

print("Identifying server path...")

# ── AUTO-PATH DETECTION ───────────────────────────────────────────────────────
try:
    page_source = requests.get(BASE_URL, headers=HEADERS).text
    match = re.search(r'md_file=([A-Za-z0-9+/=]+)', page_source)
    if not match: raise Exception("No links found.")
    
    decoded_path = base64.b64decode(match.group(1)).decode()
    BASE_PATH = decoded_path.rsplit('/', 1)[0] + "/"
except Exception:
    # Fallback to the known path if scraping fails
    BASE_PATH = "/var/www/html/wp-content/uploads/downloads/data/MPRESERVE/"

print(f"Scanning for available CSVs...")

# ── SCANNING LOOP (Newest to Oldest) ──────────────────────────────────────────
while missing_count < 10:
    date_str = current_date.strftime("%Y%m%d")
    encoded = base64.b64encode(f"{BASE_PATH}MP_RESERVE_{date_str}.csv".encode()).decode()
    
    try:
        res = requests.get(f"{BASE_URL}?md_file={encoded}", headers=HEADERS, timeout=10)
        if res.status_code == 200 and len(res.content) > 300:
            all_dfs.append(pd.read_csv(io.BytesIO(res.content)))
            missing_count = 0
        else:
            missing_count += 1
    except:
        missing_count += 1

    print(f"\rFiles found: {len(all_dfs)} | Current scan: {date_str}", end="", flush=True)
    current_date -= timedelta(days=1)

# ── MERGE & SAVE (OVERWRITE) ──────────────────────────────────────────────────
if all_dfs:
    # Concatenate and reverse the order so the CSV is Oldest -> Latest
    # Saving to 'iemop_combined.csv' uniformly (no date in filename)
    pd.concat(reversed(all_dfs), ignore_index=True).to_csv("iemop_combined.csv", index=False)
    print(f"\n\nExtraction Completed! Total days saved: {len(all_dfs)}")
else:
    print("\n\nNo data found.")