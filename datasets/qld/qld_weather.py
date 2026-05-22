import os
import pandas as pd
import requests
from io import StringIO

# Ensure working directory consistency
current_folder = os.path.basename(os.getcwd())
if current_folder != "engg2112":
    print("Warning: Please run this script from your main 'engg2112' workspace directory if paths mismatch.")

# -----------------------------
# 1. QUEENSLAND REGION MAP
# -----------------------------
# Maps explicit BoM weather station IDs to key Queensland market sectors
station_to_region = {
    "IDCJDW4019": "Brisbane_CBD",
    "IDCJDW4050": "Gold_Coast",
    "IDCJDW4128": "Townsville",
    "IDCJDW4024": "Cairns"
}

# Constrained to the exact unseen testing horizon (Jan 2026 - Feb 2026)
months = ["202601", "202602"]

# -----------------------------
# 2. ROBUST CSV PARSER ENGINE
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
        raise ValueError("Could not find standard Bureau of Meteorology table headers.")

    clean_text = "\n".join(lines[header_row:])
    return pd.read_csv(StringIO(clean_text))

# -----------------------------
# 3. NETWORK DATA STREAM RETRIEVAL
# -----------------------------
def fetch_csv(url, station_id):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers)
    
    print(f"Status: {response.status_code} | {url}")
    if response.status_code != 200:
        return None

    df = smart_read_csv(response.text)
    df["station_id"] = station_id
    df["Region"] = station_to_region[station_id]
    return df

# -----------------------------
# 4. DOWNLOAD STATION PROCESSING TIMELINE
# -----------------------------
def download_station_data(station_id):
    all_data = []
    for m in months:
        url = f"http://www.bom.gov.au/climate/dwo/{m}/text/{station_id}.{m}.csv"
        df = fetch_csv(url, station_id)
        if df is None:
            print(f"❌ Failed to download {station_id} for period {m}")
            continue

        print(f"✅ Downloaded {station_id} for period {m}") 

        df.columns = df.columns.str.strip()
        df = df.rename(columns={
            "Date": "date",
            "Maximum temperature (°C)": "temp_max",
            "Minimum temperature (°C)": "temp_min",
            "Rainfall (mm)": "rainfall"
        })

        # Synchronize date type mappings safely
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["temp_max"] = pd.to_numeric(df["temp_max"], errors="coerce")
        df["temp_min"] = pd.to_numeric(df["temp_min"], errors="coerce")
        df["rainfall"] = pd.to_numeric(df["rainfall"], errors="coerce")

        df = df[["date", "temp_max", "temp_min", "rainfall", "Region"]]
        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    station_df = pd.concat(all_data)
    station_df = station_df.dropna(subset=["date"]).sort_values("date").drop_duplicates("date")

    # Regularize alignment across the daily time series index
    station_df = station_df.set_index("date")
    
    # Isolate variables safely via step-wise timeline forward/backward filling
    station_df["temp_max"] = station_df["temp_max"].ffill().bfill()
    station_df["temp_min"] = station_df["temp_min"].ffill().bfill()
    station_df["rainfall"] = station_df["rainfall"].fillna(0)
    station_df["Region"] = station_df["Region"].ffill().bfill()

    return station_df.reset_index()

# -----------------------------
# 5. EXECUTE PIPELINE GENERATION
# -----------------------------
weather_dfs = []

for station_id in station_to_region.keys():
    print(f"\nProcessing Queensland Sector: {station_id} ({station_to_region[station_id]})")
    df_station = download_station_data(station_id)
    if not df_station.empty:
        weather_dfs.append(df_station)

if len(weather_dfs) == 0:
    raise ValueError("Pipeline Aborted: No valid Queensland weather arrays extracted.")

# Union all regional arrays into a single master validation file
qld_weather_df = pd.concat(weather_dfs, ignore_index=True)

# Generate a master "Regional" state profile to act as a fallback calculation anchor
# for any station postcodes that fall outside the main city hubs
print("\nGenerating state-wide 'Regional' fallback weather parameters...")
regional_fallback = qld_weather_df.groupby('date')[['temp_max', 'temp_min', 'rainfall']].mean().reset_index()
regional_fallback['Region'] = 'Regional'

# Combine structural regions with fallback parameters
final_qld_weather = pd.concat([qld_weather_df, regional_fallback], ignore_index=True)

print(f"\nQueensland Validation Weather Processed! Shape: {final_qld_weather.shape}")
print(final_qld_weather.groupby('Region').size())

# -----------------------------
# 6. SAVE DESTINATION OUTPUT
# -----------------------------
os.makedirs("datasets/weather", exist_ok=True)
output_path = "datasets/qld/qld_validation_weather.csv"
final_qld_weather.to_csv(output_path, index=False)
print(f"\nSuccess! File safely generated and stored at: {output_path}")