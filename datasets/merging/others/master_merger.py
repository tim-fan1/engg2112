import pandas as pd

# -----------------------------
# 1. SETUP & FILE NAMES
# -----------------------------
FUEL_FILE = "engg2112/datasets/fuel/6-month fuel datasets final.xlsx"
WEATHER_FILE = "engg2112/datasets/weather/weather_only_dataset.csv"
OIL_FILE = "engg2112/datasets/oil/daily oil price.xlsx"
OUTPUT_FILE = "engg2112/datasets/merging/MASTER_dataset_final.csv"

def assign_region(postcode):
    """Your updated region mapping logic"""
    try:
        p = int(postcode)
        if 2745 <= p <= 2780:
            return "Western_Sydney"
        elif (2170 <= p <= 2179) or (2560 <= p <= 2579):
            return "South_West_Sydney"
        elif (2250 <= p <= 2330) or (2280 <= p <= 2319):
            return "Hunter"
        elif 2500 <= p <= 2530:
            return "Wollongong"
        elif (2600 <= p <= 2620) or (2900 <= p <= 2914):
            return "Canberra"
        elif 2000 <= p <= 2199:
            return "Sydney_CBD"
        else:
            return "Regional"
    except:
        return "Regional"

# -----------------------------
# 2. LOAD DATASETS
# -----------------------------
print("Loading all datasets...")
fuel_df = pd.read_excel(FUEL_FILE)
weather_df = pd.read_csv(WEATHER_FILE)
oil_df = pd.read_excel(OIL_FILE)

# Standardize date columns
fuel_df['date'] = pd.to_datetime(fuel_df['PriceUpdatedDate']).dt.normalize()
weather_df['date'] = pd.to_datetime(weather_df['date'])
oil_df['date'] = pd.to_datetime(oil_df['date'])

# Rename oil price to avoid conflict
oil_df = oil_df.rename(columns={'price': 'oil_price'})

# -----------------------------
# 3. ASSIGN REGIONS (FUEL)
# -----------------------------
print("Assigning regions to postcodes...")
fuel_df['Region'] = fuel_df['Postcode'].apply(assign_region)

# -----------------------------
# 4. MERGE WEATHER (Date + Region)
# -----------------------------
print("Merging weather data...")
# Inner join drops "Regional" rows that don't have weather data
df_merged = pd.merge(fuel_df, weather_df, on=['date', 'Region'], how='inner')

# -----------------------------
# 5. MERGE OIL (Date only)
# -----------------------------
print("Merging oil data...")
# Left join + forward fill to handle weekends
df_merged = pd.merge(df_merged, oil_df, on='date', how='left')

# Sort to ensure time-based filling works
df_merged = df_merged.sort_values(by=['date', 'ServiceStationName'])
df_merged['oil_price'] = df_merged['oil_price'].ffill().bfill()

# -----------------------------
# 6. FINAL CLEANING & SAVE
# -----------------------------
# Remove extra columns you don't need for the final model
final_columns = [
    'date', 'ServiceStationName', 'Brand', 'Postcode', 'Region', 
    'Price', 'temp_max', 'temp_min', 'rainfall', 'oil_price'
]
df_final = df_merged[final_columns]

df_final.to_csv(OUTPUT_FILE, index=False)

print("-" * 30)
print(f"SUCCESS: Master dataset saved to {OUTPUT_FILE}")
print(f"Total rows: {len(df_final)}")
print(f"Regions included: {df_final['Region'].unique()}")
print("-" * 30)