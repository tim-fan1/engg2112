import pandas as pd
import requests
from io import StringIO

# -----------------------------
# 1. REGION MAP
# -----------------------------

station_to_region = {
    "IDCJDW2124": "Sydney_CBD",
    "IDCJDW2126": "Western_Sydney",
    "IDCJDW2133": "South_West_Sydney",
    "IDCJDW2111": "Hunter",
    "IDCJDW2144": "Wollongong",    # Added: Wollongong (Albion Park)
    "IDCJDW2801": "Canberra"       # Added: Canberra Airport
}

months = ["202509", "202510", "202511", "202512", "202601", "202602"]


# -----------------------------
# 2. ROBUST CSV LOADER
# -----------------------------

def smart_read_csv(text):
    lines = text.split("\n")

    header_row = None
    for i, line in enumerate(lines):
        if ("Minimum temperature (°C)" in line and
            "Maximum temperature (°C)" in line and
            "Rainfall (mm)" in line):
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row")

    clean_text = "\n".join(lines[header_row:])
    return pd.read_csv(StringIO(clean_text))


# -----------------------------
# 3. FETCH FUNCTION
# -----------------------------

def fetch_csv(url, station_id):
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)

    print(f"Status: {response.status_code} | {url}")

    if response.status_code != 200:
        return None

    df = smart_read_csv(response.text)

    df["station_id"] = station_id
    df["Region"] = station_to_region[station_id]

    return df


# -----------------------------
# 4. DOWNLOAD ONE STATION
# -----------------------------

def download_station_data(station_id):
    all_data = []
    for m in months:
        url = f"http://www.bom.gov.au/climate/dwo/{m}/text/{station_id}.{m}.csv"
        df = fetch_csv(url, station_id)
        if df is None:
            print("❌ Failed")
            continue

        print("✅ Success") 

        df.columns = df.columns.str.strip()
        df = df.rename(columns={
            "Date": "date",
            "Maximum temperature (°C)": "temp_max",
            "Minimum temperature (°C)": "temp_min",
            "Rainfall (mm)": "rainfall"
        })

        # --- FIX 1: Convert to actual datetime objects ---
        df["date"] = pd.to_datetime(df["date"])
        
        df["temp_max"] = pd.to_numeric(df["temp_max"], errors="coerce")
        df["temp_min"] = pd.to_numeric(df["temp_min"], errors="coerce")
        df["rainfall"] = pd.to_numeric(df["rainfall"], errors="coerce")

        df = df[["date", "temp_max", "temp_min", "rainfall", "Region"]]
        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    station_df = pd.concat(all_data)
    station_df = station_df.sort_values("date").drop_duplicates("date")

    # --- FIX 2: Set index and handle frequency safely ---
    station_df = station_df.set_index("date")
    
    # Optional: ensure we cover the whole range if months were missing
    # station_df = station_df.reindex(pd.date_range(station_df.index.min(), station_df.index.max(), freq='D'))

    # --- FIX 3: Fill everything, including Region ---
    station_df["temp_max"] = station_df["temp_max"].ffill().bfill()
    station_df["temp_min"] = station_df["temp_min"].ffill().bfill()
    station_df["rainfall"] = station_df["rainfall"].fillna(0)
    # Important: Region must be filled too!
    station_df["Region"] = station_df["Region"].ffill().bfill()

    return station_df.reset_index()

# -----------------------------
# 5. BUILD FULL WEATHER DATASET
# -----------------------------

weather_dfs = []

for station_id in station_to_region.keys():
    print(f"\nDownloading {station_id}...")

    df_station = download_station_data(station_id)

    if not df_station.empty:
        weather_dfs.append(df_station)

if len(weather_dfs) == 0:
    raise ValueError("No weather data downloaded")

weather_df = pd.concat(weather_dfs)

print("\nWeather dataset ready:", weather_df.shape)
print(weather_df.head())


# -----------------------------
# 6. SAVE OUTPUT
# -----------------------------

weather_df.to_csv("engg2112/datasets/weather/weather_only_dataset.csv", index=False)

print("\nSaved: weather_only_dataset.csv")