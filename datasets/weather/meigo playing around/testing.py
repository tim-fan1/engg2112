import requests
from io import StringIO
import pandas as pd

import pandas as pd
from io import StringIO

def smart_read_csv(text):
    lines = text.split("\n")

    # find header row
    header_row = None
    for i, line in enumerate(lines):
        if "Minimum temperature (°C)" in line and "Maximum temperature (°C)" in line and "Rainfall (mm)" in line:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row")

    # keep only CSV part (important fix)
    clean_text = "\n".join(lines[header_row:])

    return pd.read_csv(StringIO(clean_text))

def fetch_csv(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status: {response.status_code} | {url}")

        if response.status_code != 200:
            return None

        # Check if BOM returned HTML instead of CSV
        if "html" in response.text[:200].lower():
            print("⚠️ Got HTML instead of CSV (blocked)")
            return None

        return smart_read_csv(response.text)
        

    except Exception as e:
        print(f"ERROR: {e}")
        return None
    
url = "http://www.bom.gov.au/climate/dwo/202601/text/IDCJDW2111.202601.csv"

df = fetch_csv(url)

if df is None:
    print("No data returned")
    exit()

df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

print(df.head())
print(df.columns)

df.to_csv('local_copy.csv', index=False)