import pandas as pd
import numpy as np
import os

# Setup Paths
FUEL_FILE = "datasets/fuel/NSW_E10_Full_Year_25_26.csv"
WEATHER_FILE = "datasets/weather/year_weather_dataset.csv"
OIL_FILE = "datasets/oil/daily oil price.xlsx"
TGP_FILE = "datasets/tgp/petrol_tgp.csv"
FX_FILE = "datasets/exchange/2023-current.xls"
OUTPUT_FILE = "datasets/MODEL_READY_DATASET4.csv"

def assign_region(postcode):
    try:
        p = int(postcode)
        if 2745 <= p <= 2780: return "Western_Sydney"
        elif (2170 <= p <= 2179) or (2560 <= p <= 2579): return "South_West_Sydney"
        elif (2250 <= p <= 2330) or (2280 <= p <= 2319): return "Hunter"
        elif 2500 <= p <= 2530: return "Wollongong"
        elif (2600 <= p <= 2620) or (2900 <= p <= 2914): return "Canberra"
        elif 2000 <= p <= 2199: return "Sydney_CBD"
        else: return "Regional"
    except: return "Regional"

print("1. Loading and Merging...")
fuel_df = pd.read_csv(FUEL_FILE)
fuel_df['date'] = pd.to_datetime(fuel_df['PriceUpdatedDate'], dayfirst=True, format='mixed').dt.normalize()
fuel_df['Region'] = fuel_df['Postcode'].apply(assign_region)

weather_df = pd.read_csv(WEATHER_FILE)
weather_df['date'] = pd.to_datetime(weather_df['date'])

oil_df = pd.read_excel(OIL_FILE).rename(columns={'price': 'oil_price'})
oil_df['date'] = pd.to_datetime(oil_df['date'])

tgp_df = pd.read_csv(TGP_FILE)
tgp_df['date'] = pd.to_datetime(tgp_df[tgp_df.columns[0]])
tgp_df = tgp_df[['date', 'sydney_tgp']].rename(columns={'sydney_tgp': 'tgp_sydney'})

# --- Updated FX Loading Section ---
# We force all column headers to be strings to prevent the AttributeError
fx_data = pd.read_excel(FX_FILE, skiprows=11)

# Fix: Convert all column names to strings and handle potential NaNs
fx_data.columns = [str(col) for col in fx_data.columns]

# Now find the USD column
fx_col = [col for col in fx_data.columns if 'usd' in col.lower()]

if not fx_col:
    # Fallback: if 'usd' isn't found, take the second column (usually where the rate is)
    fx_col = fx_data.columns[1]
    print(f"Warning: 'usd' not found in headers, using column: {fx_col}")
else:
    fx_col = fx_col[0]

fx_data['date'] = pd.to_datetime(fx_data[fx_data.columns[0]], errors='coerce')
fx_data = fx_data.dropna(subset=['date'])
fx_data = fx_data[['date', fx_col]].rename(columns={fx_col: 'aud_usd'})
fx_data['date'] = fx_data['date'].dt.normalize()

# Merge
df = pd.merge(fuel_df, weather_df, on=['date', 'Region'], how='inner')
df = pd.merge(df, oil_df, on='date', how='left')
df = pd.merge(df, tgp_df, on='date', how='left')
df = pd.merge(df, fx_data, on='date', how='left')
df = df.sort_values(['date', 'ServiceStationName']).ffill().bfill()
df.columns = [col.lower() for col in df.columns]

print("2. Engineering Features...")
# Dynamic Features
df['retail_margin'] = df['price'] - df['tgp_sydney']
df = df.sort_values(['servicestationname', 'date'])
df['price_change_24h'] = df.groupby('servicestationname')['price'].diff()

# Averages
daily_avg = df.groupby(['postcode', 'date'])['price'].mean().reset_index().rename(columns={'price': 'post_avg'})
daily_avg['post_roll_7'] = daily_avg.groupby('postcode')['post_avg'].transform(lambda x: x.rolling(7, min_periods=1).mean())
df = df.merge(daily_avg, on=['postcode', 'date'], how='left')

# Time features
df['day_of_week'] = df['date'].dt.dayofweek
df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
df['is_hike_day'] = df['day_of_week'].isin([1, 2]).astype(int)
df['margin_hike_interaction'] = df['retail_margin'] * df['is_hike_day']

# Target
df['target_next_day_price'] = df.groupby('servicestationname')['price'].shift(-1)
df['target_date'] = df.groupby('servicestationname')['date'].shift(-1)
df = df[(df['target_date'] - df['date']).dt.days == 1]

print("3. Final Cleanup and Outlier Removal...")
# CRITICAL: Removing data errors (unrealistic price drops)
df = df[df['price_change_24h'] > -40] 

cols_to_drop = ['servicestationname', 'address', 'suburb', 'region', 'date', 'brand', 'fuelcode', 'priceupdateddate', 'target_date', 'day_of_week']
df = df.drop(columns=cols_to_drop, errors='ignore').dropna()

df.to_csv(OUTPUT_FILE, index=False)
print(f"Success! Saved {len(df)} rows to {OUTPUT_FILE}")