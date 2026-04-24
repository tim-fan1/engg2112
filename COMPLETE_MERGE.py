import pandas as pd
import os

# Check user is running this script from the 'engg2112' home directory.
current_folder = os.path.basename(os.getcwd())
if current_folder != "engg2112":
    print("Error: Please run this script from the 'engg2112' home directory.")
    exit()  # Stops the script immediately

"""
Task 1. Merge individual datasets, save to datasets/COMPLETE_DATASET.csv
Task 2. Generate features columns, like time lag or rolling averages
Task 3. Remove unnecessary columns, save to datasets/MODEL_READY_DATASET.csv
"""

# ------------------------------------------------------------------------------
#      Task 1. Merge individual datasets, save to COMPLETE_DATASET.csv
# ------------------------------------------------------------------------------

print("-" * 80)
print(f"Beginning Merging Individual Datasets, Complete Dataset...")
print("-" * 80)

# -----------------------------
# a. SETUP & FILE PATHS
# -----------------------------
# Adjust these names if your files are named differently
FUEL_FILE = "datasets/fuel/6-month fuel datasets final.xlsx"
WEATHER_FILE = "datasets/weather/weather_only_dataset.csv"
OIL_FILE = "datasets/oil/daily oil price.xlsx"
TGP_FILE = "datasets/tgp/petrol_tgp.csv"
FX_FILE = "datasets/exchange/2023-current.xls"
COMPLETE_OUTPUT_FILE = "datasets/COMPLETE_DATASET.csv"
MODEL_READY_OUTPUT_FILE = "datasets/MODEL_READY_DATASET.csv"

def assign_region(postcode):
    """Maps postcodes to your 6 specific weather regions"""
    try:
        p = int(postcode)
        if 2745 <= p <= 2780: return "Western_Sydney"
        elif (2170 <= p <= 2179) or (2560 <= p <= 2579): return "South_West_Sydney"
        elif (2250 <= p <= 2330) or (2280 <= p <= 2319): return "Hunter"
        elif 2500 <= p <= 2530: return "Wollongong"
        elif (2600 <= p <= 2620) or (2900 <= p <= 2914): return "Canberra"
        elif 2000 <= p <= 2199: return "Sydney_CBD"
        else: return "Regional"
    except:
        return "Regional"

# -----------------------------
# b. LOAD & CLEAN DATASETS
# -----------------------------
print("Step 1: Loading all datasets...")
fuel_df = pd.read_excel(FUEL_FILE)
weather_df = pd.read_csv(WEATHER_FILE)
oil_df = pd.read_excel(OIL_FILE)
tgp_df = pd.read_csv(TGP_FILE)

# FX Data has unique formatting (metadata in top 11 rows)
fx_df_raw = pd.read_excel(FX_FILE, skiprows=1, nrows=0) # Get headers
fx_data = pd.read_excel(FX_FILE, skiprows=11, names=fx_df_raw.columns) # Get data

# -----------------------------
# c. STANDARDIZE DATES & NAMES
# -----------------------------
print("Step 2: Standardizing dates...")

# Convert Fuel Date
fuel_df['date'] = pd.to_datetime(fuel_df['PriceUpdatedDate']).dt.normalize()

# Drop original column (no longer needed)
fuel_df = fuel_df.drop(columns=['PriceUpdatedDate'])

# Convert Weather Date
weather_df['date'] = pd.to_datetime(weather_df['date'])

# Convert Oil Date
oil_df['date'] = pd.to_datetime(oil_df['date'])
oil_df = oil_df.rename(columns={'price': 'oil_price'})

# Convert TGP Date (First column is date)
tgp_date_col = tgp_df.columns[0]
tgp_df['date'] = pd.to_datetime(tgp_df[tgp_date_col])
tgp_df = tgp_df[['date', 'sydney_tgp']].rename(columns={'sydney_tgp': 'tgp_sydney'})

# -----------------------------
# d. Convert FX Date
# -----------------------------

# 1. Identify likely date column (usually first column)
date_col = fx_data.columns[0]

# 2. Try parsing as standard datetime (works if already strings like "2023-01-01")
fx_data['date'] = pd.to_datetime(fx_data[date_col], errors='coerce')

# 3. If that fails, try Excel serial conversion
if fx_data['date'].isna().all():
    numeric_vals = pd.to_numeric(fx_data[date_col], errors='coerce')
    fx_data['date'] = pd.to_datetime(numeric_vals, unit='D', origin='1899-12-30')

# 4. Drop invalid rows (headers/footers/junk)
fx_data = fx_data.dropna(subset=['date'])

# 5. Automatically detect AUD/USD column
fx_col_candidates = [col for col in fx_data.columns if 'usd' in col.lower()]

if not fx_col_candidates:
    raise ValueError("Could not find AUD/USD column in FX dataset")

fx_col = fx_col_candidates[0]

# 6. Keep only needed columns and rename
fx_data = fx_data[['date', fx_col]].rename(columns={fx_col: 'aud_usd'})

# 7. Normalize date (remove time component)
fx_data['date'] = fx_data['date'].dt.normalize()

# -----------------------------
# e. MULTI-STEP MERGE
# -----------------------------
print("Step 3: Merging all data layers...")

# A. Region Mapping
fuel_df['Region'] = fuel_df['Postcode'].apply(assign_region)

# B. Weather (Date + Region)
df_final = pd.merge(fuel_df, weather_df, on=['date', 'Region'], how='inner')

# C. Oil, TGP, and FX (Date only)
df_final = pd.merge(df_final, oil_df, on='date', how='left')
df_final = pd.merge(df_final, tgp_df, on='date', how='left')
df_final = pd.merge(df_final, fx_data, on='date', how='left')

# -----------------------------
# f. HANDLE WEEKENDS (Forward Fill)
# -----------------------------
print("Step 4: Filling weekend gaps for market data...")
# Market data (Oil, TGP, FX) is missing on weekends. 
# We fill forward so Saturday/Sunday uses Friday's price.
df_final = df_final.sort_values(by=['date', 'ServiceStationName'])
cols_to_fill = ['oil_price', 'tgp_sydney', 'aud_usd']
df_final[cols_to_fill] = df_final[cols_to_fill].ffill().bfill()

# -----------------------------
# g. save to COMPLETE DATASET.csv
# -----------------------------
# Get the folder path
output_dir = os.path.dirname(COMPLETE_OUTPUT_FILE)

# Only try to create the folder if a folder path actually exists
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

df_final.to_csv(COMPLETE_OUTPUT_FILE, index=False)

print("-" * 80)
print(f"SUCCESS: {COMPLETE_OUTPUT_FILE} is ready.")
print("-" * 80)

df_final.info()

print("-" * 80)
print(f"Beginning Features Generation, Model-Ready Dataset...")
print("-" * 80)

# ------------------------------------------------------------------------------
#                    Task 2. Generate features columns
# ------------------------------------------------------------------------------

# Make all columns lowercase
df_final.columns = [col.lower() for col in df_final.columns]

# ------------------------------------------------------------------------
# a. Averages Features: postcode_daily_average and postcode_rolling_7d
# ------------------------------------------------------------------------

print("Step 1: Daily and Weekly Average Features...")
# For each (postcode, date) pair, what is the average of all prices; rows that have (postcode, date)
daily_avg_df = df_final.groupby(['postcode', 'date'])['price'].mean().reset_index()

# This is a new df isn't it; daily_avg_df. Rename the price column within this to postcode_daily_avg
daily_avg_df.rename(columns={'price': 'postcode_daily_avg'}, inplace=True)

# Sort to ensure the timeline is chronological for the rolling calculation
daily_avg_df = daily_avg_df.sort_values(['postcode', 'date'])

# Make new column that is 7-day rolling average
# Note: What to do about the first 7 days of dataset? 
# This implements a simple solution, just do the best that can do, e.g.:
# - 1st day's average is just 1st day 
# - 2nd day's average is average of 1st and 2nd day
# - 3rd day's average is average of 1st, 2nd, and 3rd day
# - ...
# Alternatively, could also download the previous month's dataset,
# but I think that's a hassle; let's assume we only have access to 
# the datasets that are in the project repository.
daily_avg_df['postcode_rolling_7d'] = daily_avg_df.groupby('postcode')['postcode_daily_avg'].transform(
    lambda x: x.rolling(window=7, min_periods=1).mean()
)

# Merge new daily_avg_df with old df_final
df_final = df_final.merge(daily_avg_df, on=['postcode', 'date'], how='left')

# --------------------------------------------------------------------------
# b. Time Lag Features: [oil|tgp|exchange]_price_lag, etc., daily and weekly
# --------------------------------------------------------------------------

print("Step 2: Daily and Weekly Time Lag Features...")
# Making sure there is just one oil_price, tgp_sydney, and aud_usd for each date
lag_df = df_final.groupby('date')[['oil_price', 'tgp_sydney', 'aud_usd']].mean().reset_index()

# And sort by date so that can then calculate the time lag features
lag_df = lag_df.sort_values('date')
lag_df['oil_price_lag_1'] = lag_df['oil_price'].shift(1) # Yesterday
lag_df['oil_price_lag_7'] = lag_df['oil_price'].shift(7) # Last week
lag_df['tgp_sydney_lag_1'] = lag_df['tgp_sydney'].shift(1)
lag_df['tgp_sydney_lag_7'] = lag_df['tgp_sydney'].shift(7)
lag_df['aud_usd_lag_1'] = lag_df['aud_usd'].shift(1)
lag_df['aud_usd_lag_7'] = lag_df['aud_usd'].shift(7)

# Use fillna to fill in missing values (the first day and first week due to lag)
cols_to_fix = [
    'oil_price_lag_1', 'oil_price_lag_7', 
    'tgp_sydney_lag_1', 'tgp_sydney_lag_7', 
    'aud_usd_lag_1', 'aud_usd_lag_7'
]
lag_df[cols_to_fix] = lag_df[cols_to_fix].bfill()

# Take the subset ['date' + cols_to_fix] of lag_df, and merge with original
df_final = df_final.merge(lag_df[['date'] + cols_to_fix], on='date', how='left')

# ---------------------------------------
# c. Day of Week Feature: day_of_week
# ---------------------------------------

print("Step 3: Day-of-Week Feature...")
# Save as a number from 0 to 6 inclusive
df_final['day_of_week'] = df_final['date'].dt.dayofweek

# --------------------------------------------
# e. TARGET FEATURE: target_next_day_price
# --------------------------------------------

print("Step 4: Building Target Feature...")
# The problem with this is that it assumes service stations report E10 every day
df_final = df_final.sort_values(['servicestationname', 'date'])
df_final['target_next_day_price'] = df_final.groupby('servicestationname')['price'].shift(-1)

print("Step 5: Include only daily consecutive rows...")
# The solution? Only include consecutive rows where the days apart is exactly 1
# Note: This cuts A LOT of rows. 
df_final['target_date'] = df_final.groupby('servicestationname')['date'].shift(-1)
df_final['days_to_target'] = (df_final['target_date'] - df_final['date']).dt.days
df_final = df_final[df_final['days_to_target'] == 1]

# Drop the temp columns
df_final = df_final.drop(columns=['target_date', 'days_to_target'])

# ------------------------------------------------------------------------------
#      Task 3. Remove unnecessary columns, save to MODEL_READY_DATASET.csv
# ------------------------------------------------------------------------------
print("Step 6: Remove non-number non-boolean columns...")
cols_to_drop = [
    'servicestationname', 
    'address', 
    'suburb', 
    'region', 
    'date',
    'brand',
    'fuelcode'
]
df_final = df_final.sort_values('date')
df_final = df_final.drop(columns=cols_to_drop)
df_final.to_csv(MODEL_READY_OUTPUT_FILE, index=False)

print("-" * 80)
print(f"SUCCESS: ${MODEL_READY_OUTPUT_FILE} is ready.")
print("-" * 80)
df_final.info()