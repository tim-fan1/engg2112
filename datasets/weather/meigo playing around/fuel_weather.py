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
    "IDCJDW2111": "Hunter"
}

# Months you want (YYYYMM format)
months = ["202509", "202510", "202511", "202512", "202601", "202602"]

stations = station_to_region.keys()


# -----------------------------
# 2. ROBUST CSV LOADER
# -----------------------------

def smart_read_csv(text):
    lines = text.split("\n")

    # find header row (BOM is inconsistent → detect by temp columns)
    header_row = None
    for i, line in enumerate(lines):
        if "Minimum temperature" in line and "Maximum temperature" in line and "Rainfall" in line:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row")

    clean_text = "\n".join(lines[header_row:])

    return pd.read_csv(StringIO(clean_text))


def fetch_csv(url, station_id):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)

    print(f"Status: {response.status_code} | {url}")

    if response.status_code != 200:
        return None

    df = smart_read_csv(response.text)

    # attach metadata
    df["station_id"] = station_id
    df["Region"] = station_to_region[station_id]

    return df


# -----------------------------
# 3. DOWNLOAD STATION DATA
# -----------------------------

def download_station_data(station_id):
    all_data = []

    for m in months:
        url = f"http://www.bom.gov.au/climate/dwo/{m}/text/{station_id}.{m}.csv"

        df = fetch_csv(url, station_id)

        if df is None:
            continue

        print(f"Loaded: {url}")

        df.columns = df.columns.str.strip()

        df = df.rename(columns={
            "Year": "year",
            "Month": "month",
            "Day": "day",
            "Maximum temperature (Degree C)": "temp_max",
            "Minimum temperature (Degree C)": "temp_min",
            "Rainfall amount (millimetres)": "rainfall"
        })

        df["date"] = pd.to_datetime(df[["year", "month", "day"]])

        df = df[["date", "temp_max", "temp_min", "rainfall", "Region"]]

        all_data.append(df)

    if len(all_data) == 0:
        return pd.DataFrame()

    station_df = pd.concat(all_data)

    station_df = station_df.sort_values("date").drop_duplicates("date")

    station_df = station_df.set_index("date").asfreq("D")

    station_df["temp_max"] = station_df["temp_max"].ffill()
    station_df["temp_min"] = station_df["temp_min"].ffill()
    station_df["rainfall"] = station_df["rainfall"].fillna(0)

    station_df = station_df.reset_index()

    return station_df


# -----------------------------
# 4. BUILD WEATHER DATASET
# -----------------------------

weather_dfs = []

for station_id in stations:
    print(f"\nDownloading {station_id}...")
    df_station = download_station_data(station_id)

    if not df_station.empty:
        weather_dfs.append(df_station)

if len(weather_dfs) == 0:
    raise ValueError("No weather data downloaded. Check stations or URLs.")

weather_df = pd.concat(weather_dfs)

print("Weather data ready:", weather_df.shape)


# -----------------------------
# 5. LOAD FUEL DATA
# -----------------------------

fuel_df = pd.read_excel("engg2112/datasets/fuel/6-month fuel datasets final.xlsx")

fuel_df["date"] = pd.to_datetime(fuel_df["Date"])


# -----------------------------
# 6. REGION ASSIGNMENT (FUEL SIDE)
# -----------------------------

def assign_region(postcode):
    if 2000 <= postcode <= 2199:
        return "Sydney_CBD"
    elif 2745 <= postcode <= 2780:
        return "Western_Sydney"
    elif 2170 <= postcode <= 2179 or 2560 <= postcode <= 2579:
        return "South_West_Sydney"
    elif 2250 <= postcode <= 2330 or 2280 <= postcode <= 2319:
        return "Hunter"
    else:
        return "Regional"


fuel_df["Region"] = fuel_df["Postcode"].apply(assign_region)


# -----------------------------
# 7. MERGE
# -----------------------------

final_df = fuel_df.merge(
    weather_df,
    on=["date", "Region"],
    how="left"
)

print("Final dataset:", final_df.shape)


# -----------------------------
# 8. SAVE
# -----------------------------

final_df.to_csv("fuel_with_weather.csv", index=False)

print("Done. File saved: fuel_with_weather.csv")