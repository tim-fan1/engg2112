import pandas as pd

# -----------------------------
# 1. LOAD DATA
# -----------------------------
# Adjust filenames if they are different on your computer
fuel_df = pd.read_excel("engg2112/datasets/fuel/6-month fuel datasets final.xlsx")
weather_df = pd.read_csv("engg2112/datasets/weather/weather_only_dataset.csv")

# -----------------------------
# 2. DEFINE REGION MAPPING
# -----------------------------
def assign_region(postcode):
    # Check specific regions FIRST
    if 2745 <= postcode <= 2780:
        return "Western_Sydney"
    elif (2170 <= postcode <= 2179) or (2560 <= postcode <= 2579):
        return "South_West_Sydney"
    elif (2250 <= postcode <= 2330) or (2280 <= postcode <= 2319):
        return "Hunter"
    # Check general Sydney area LAST so it doesn't "steal" SW Sydney postcodes
    elif 2000 <= postcode <= 2199:
        return "Sydney_CBD"
    else:
        return "Regional"

# Apply mapping to fuel data
fuel_df["Region"] = fuel_df["Postcode"].apply(assign_region)

# -----------------------------
# 3. NORMALIZE DATES
# -----------------------------
# Fuel data usually has time (H:M:S), weather is daily. 
# We strip the time so they match.
fuel_df["date"] = pd.to_datetime(fuel_df["PriceUpdatedDate"]).dt.normalize()
weather_df["date"] = pd.to_datetime(weather_df["date"])

# -----------------------------
# 4. MERGE
# -----------------------------
# We merge on BOTH 'date' and 'Region'
# Use 'inner' to keep only records where we have matches in both
combined_df = pd.merge(
    fuel_df, 
    weather_df, 
    on=["date", "Region"], 
    how="inner"
)

# -----------------------------
# 5. SAVE & PREVIEW
# -----------------------------
combined_df.to_csv("engg2112/datasets/weather + fuel/fuel_weather_merged.csv", index=False)

print(f"Successfully merged {len(combined_df)} rows.")
print("\nQuick look at the combined data:")
print(combined_df[['date', 'Region', 'Price', 'temp_max', 'rainfall']].head())