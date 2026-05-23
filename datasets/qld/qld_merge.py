import pandas as pd
import numpy as np
import os

# Check user is running this script from the 'engg2112' home directory.
current_folder = os.path.basename(os.getcwd())
if current_folder != "engg2112":
    print("Warning: Ensure you are running this script from your main 'engg2112' workspace directory if paths mismatch.")

# -------------------------------------------------------------
# 1. FILE SYSTEM ROUTING PATHS
# -------------------------------------------------------------
QLD_JAN_FILE = "datasets/qld/fuel-prices-2026-01-changes-only.csv"
QLD_FEB_FILE = "datasets/qld/fuel-prices-2026-02-changes-only.csv"
QLD_WEATHER_FILE = "datasets/qld/qld_validation_weather.csv"
OIL_FILE = "datasets/oil/daily oil price.xlsx"
TGP_FILE = "datasets/qld/qld_tgp.csv"                 # Created via your Brisbane TGP extractor
FX_FILE = "datasets/exchange/2023-current.xls"

QLD_MODEL_READY_FILE = "datasets/QLD_MODEL_READY_VALIDATION.csv"

# -------------------------------------------------------------
# 2. DATA INGESTION & CONSOLIDATION
# -------------------------------------------------------------
print("Step 1: Loading and consolidating raw Queensland transaction blocks...")
jan_df = pd.read_csv(QLD_JAN_FILE)
feb_df = pd.read_csv(QLD_FEB_FILE)
raw_qld_df = pd.concat([jan_df, feb_df], ignore_index=True)

# FX Data has unique formatting (metadata in top 11 rows) - Loaded safely here once
fx_df_raw = pd.read_excel(FX_FILE, skiprows=1, nrows=0) # Get headers
fx_df = pd.read_excel(FX_FILE, skiprows=11, names=fx_df_raw.columns) # Get data

# Isolate E10 fuel type to stay consistent with training parameters
raw_qld_df = raw_qld_df[raw_qld_df['Fuel_Type'].str.lower().str.strip() == 'e10'].copy()

# Rename columns to align with standard core logic
raw_qld_df = raw_qld_df.rename(columns={
    'SiteId': 'servicestationname',
    'Site_Post_Code': 'postcode',
    'TransactionDateutc': 'date',
    'Price': 'fuel_price'
})

# SCALE CORRECTION: Convert integer tenths-of-a-cent to standard decimals
raw_qld_df['fuel_price'] = raw_qld_df['fuel_price'] / 10.0

# Filter out anomalous dummy placeholder entries (999.9 and typos) BEFORE timeline resampling
raw_qld_df = raw_qld_df[(raw_qld_df['fuel_price'] < 500.0) & (raw_qld_df['fuel_price'] > 100.0)]

# Normalize date strings to standard datetime format
raw_qld_df['date'] = pd.to_datetime(raw_qld_df['date'], dayfirst=True, format='mixed').dt.normalize()

# -------------------------------------------------------------
# 3. STATION TIMELINE RESAMPLING (CHANGES-ONLY TO DAILY)
# -------------------------------------------------------------
print("Step 2: Resampling 'changes-only' data to consecutive daily timelines...")

def resample_station_timeline(group, station_name):
    # Establish complete boundary limits for January & February 2026
    date_range = pd.date_range(start='2026-01-01', end='2026-02-28', freq='D')
    
    # Drop intra-day duplicates if a station changed prices multiple times in a single day
    group = group.drop_duplicates(subset=['date'], keep='last').set_index('date')
    
    # Reindex to fill sequential chronological gaps
    resampled = group.reindex(date_range)
    
    # Forward-fill prices and postcode details down the timeline
    resampled['fuel_price'] = resampled['fuel_price'].ffill().bfill()
    resampled['postcode'] = resampled['postcode'].ffill().bfill()
    
    # Manually re-attach the grouping column values securely
    resampled['servicestationname'] = station_name
    
    return resampled

processed_fuel = raw_qld_df.groupby('servicestationname', group_keys=False).apply(
    lambda g: resample_station_timeline(g, g.name), 
    include_groups=False
)
processed_fuel = processed_fuel.reset_index().rename(columns={'index': 'date'})

# -------------------------------------------------------------
# 4. REGIONAL POSTCODE MAPPING
# -------------------------------------------------------------
print("Step 3: Appending geographical weather boundaries...")

def assign_qld_region(postcode):
    try:
        p = int(postcode)
        if (4000 <= p <= 4207) or (4300 <= p <= 4305) or (4500 <= p <= 4519): 
            return "Brisbane_CBD"
        elif (4208 <= p <= 4287): 
            return "Gold_Coast"
        elif (4810 <= p <= 4819): 
            return "Townsville"
        elif (4870 <= p <= 4879): 
            return "Cairns"
        else: 
            return "Regional"
    except:
        return "Regional"

processed_fuel['Region'] = processed_fuel['postcode'].apply(assign_qld_region)

# -------------------------------------------------------------
# 5. STREAM MERGING WITH ROBUST FORECASTED GAPS
# -------------------------------------------------------------
print("Step 4: Merging external structural features...")

# --- Weather ---
weather_df = pd.read_csv(QLD_WEATHER_FILE)
weather_df['date'] = pd.to_datetime(weather_df['date'])

# --- Oil ---
oil_df = pd.read_excel(OIL_FILE)
oil_df['date'] = pd.to_datetime(oil_df['date'])
oil_df = oil_df.rename(columns={'price': 'oil_price'})

# --- TGP ---
tgp_df = pd.read_csv(TGP_FILE)
tgp_date_col = tgp_df.columns[0]
tgp_df['date'] = pd.to_datetime(tgp_df[tgp_date_col])
tgp_df = tgp_df[['date', 'brisbane_tgp']].rename(columns={'brisbane_tgp': 'tgp_sydney'})

# --- FX (Robust Horizon Forward Filling) ---
# FIXED: Removed the redundant re-read line that was erasing our clean fx_df from Step 1!
fx_date_col = fx_df.columns[0]
fx_df['date'] = pd.to_datetime(fx_df[fx_date_col], errors='coerce')

if fx_df['date'].isna().all():
    numeric_vals = pd.to_numeric(fx_df[fx_date_col], errors='coerce')
    fx_df['date'] = pd.to_datetime(numeric_vals, unit='D', origin='1899-12-30').dt.normalize()

fx_df = fx_df.dropna(subset=['date']).sort_values('date')
fx_df['date'] = fx_df['date'].dt.normalize()

fx_col_candidates = [col for col in fx_df.columns if 'usd' in str(col).lower() or 'exchange' in str(col).lower() or 'vfx' in str(col).lower()]
fx_col = fx_col_candidates[0] if fx_col_candidates else fx_df.columns[1]
fx_df = fx_df[['date', fx_col]].rename(columns={fx_col: 'aud_usd'})
fx_df['aud_usd'] = pd.to_numeric(fx_df['aud_usd'], errors='coerce')

# Create a timeline buffer that forward-fills the last known exchange rate into Jan/Feb 2026
full_timeline = pd.DataFrame({'date': pd.date_range(start=fx_df['date'].min(), end='2026-02-28', freq='D')})
fx_df = pd.merge(full_timeline, fx_df, on='date', how='left').ffill().bfill()

# --- DB Joins ---
df_final = pd.merge(processed_fuel, weather_df, on=['date', 'Region'], how='inner')
df_final = pd.merge(df_final, oil_df, on='date', how='left')
df_final = pd.merge(df_final, tgp_df, on='date', how='left')
df_final = pd.merge(df_final, fx_df, on='date', how='left')

# Fill weekend gaps for market metrics
df_final = df_final.sort_values(by=['date', 'servicestationname'])
cols_to_fill = ['oil_price', 'tgp_sydney', 'aud_usd']
df_final[cols_to_fill] = df_final[cols_to_fill].ffill().bfill()

# -------------------------------------------------------------
# 6. TIME-LAG & AGGREGATE FEATURE ENGINEERING
# -------------------------------------------------------------
print("Step 5: Extracting rolling averages, lag transformations, and temporal curves...")

lag_df = df_final.groupby('date')[['oil_price', 'tgp_sydney', 'aud_usd']].mean().reset_index().sort_values('date')

lag_df['oil_rolling_7d'] = lag_df['oil_price'].rolling(window=7, min_periods=1).mean()
lag_df['tgp_rolling_7d'] = lag_df['tgp_sydney'].rolling(window=7, min_periods=1).mean()
lag_df['aud_usd_rolling_7d'] = lag_df['aud_usd'].rolling(window=7, min_periods=1).mean()

lag_df['tgp_sydney_lag_1'] = lag_df['tgp_sydney'].shift(1)
lag_df['aud_usd_lag_1'] = lag_df['aud_usd'].shift(1)

# Deep global oil lags matching your CCF peak window
lag_df['oil_price_lag_19'] = lag_df['oil_price'].shift(19)
lag_df['oil_price_lag_21'] = lag_df['oil_price'].shift(21)
lag_df['oil_price_lag_19_to_22_mean'] = lag_df['oil_price'].shift(19).rolling(window=4).mean()

cols_to_bfill = ['tgp_sydney_lag_1', 'aud_usd_lag_1', 'oil_price_lag_19', 'oil_price_lag_21', 'oil_price_lag_19_to_22_mean']
lag_df[cols_to_bfill] = lag_df[cols_to_bfill].bfill()

df_final = pd.merge(df_final, lag_df.drop(columns=['oil_price', 'tgp_sydney', 'aud_usd']), on='date', how='left')

# Base Features
df_final['retail_margin'] = df_final['fuel_price'] - df_final['tgp_sydney']
df_final = df_final.sort_values(['servicestationname', 'date']).reset_index(drop=True)
df_final['price_change_24h'] = df_final.groupby('servicestationname')['fuel_price'].diff().fillna(0)

# --- POSTCODE AGGREGATION LOOKUP MATRIX ---
# Generate first set of postcode metrics
daily_avg_df = df_final.groupby(['postcode', 'date'])['fuel_price'].mean().reset_index().rename(columns={'fuel_price': 'fuel_postcode_daily_avg'})
daily_avg_df = daily_avg_df.sort_values(['postcode', 'date'])
daily_avg_df['fuel_postcode_price_lag_1'] = daily_avg_df.groupby('postcode')['fuel_postcode_daily_avg'].shift(1)
daily_avg_df['fuel_postcode_rolling_7d'] = daily_avg_df.groupby('postcode')['fuel_postcode_daily_avg'].transform(lambda x: x.rolling(7, min_periods=1).mean())
daily_avg_df['fuel_postcode_price_lag_1'] = daily_avg_df['fuel_postcode_price_lag_1'].fillna(daily_avg_df['fuel_postcode_daily_avg'])

# Generate second set of duplicate legacy columns to prevent structural training errors
daily_avg_legacy = df_final.groupby(['postcode', 'date'])['fuel_price'].mean().reset_index().rename(columns={'fuel_price': 'post_avg'})
daily_avg_legacy['post_roll_7'] = daily_avg_legacy.groupby('postcode')['post_avg'].transform(lambda x: x.rolling(7, min_periods=1).mean())

# Merge both groups back into df_final
df_final = pd.merge(df_final, daily_avg_df, on=['postcode', 'date'], how='left')
df_final = pd.merge(df_final, daily_avg_legacy, on=['postcode', 'date'], how='left')

df_final['day_of_week'] = df_final['date'].dt.dayofweek
df_final['day_sin'] = np.sin(2 * np.pi * df_final['day_of_week'] / 7.0)
df_final['day_cos'] = np.cos(2 * np.pi * df_final['day_of_week'] / 7.0)

df_final['is_hike_day'] = (df_final['price_change_24h'] > 15.0).astype(int)
df_final['margin_hike_interaction'] = df_final['retail_margin'] * df_final['is_hike_day']

# -------------------------------------------------------------
# 7. VALUATION TARGET ALIGNMENT & EXTRACTION
# -------------------------------------------------------------
print("Step 6: Finalizing target windows and cleaning metadata...")

# ALIGNMENT FIX: Target tomorrow is the shifted POSTCODE AVERAGE, exactly like your NSW setup!
daily_avg_df['target_fuel_price_tomorrow'] = daily_avg_df.groupby('postcode')['fuel_postcode_daily_avg'].shift(-1)
daily_avg_df['target_date'] = daily_avg_df.groupby('postcode')['date'].shift(-1)

# Keep chronological consecutive timelines clean
is_consecutive = (daily_avg_df['target_date'] - daily_avg_df['date']).dt.days == 1
daily_avg_df = daily_avg_df[is_consecutive]

# Merge the clean target series back into main df_final mapping
df_final = pd.merge(df_final, daily_avg_df[['postcode', 'date', 'target_fuel_price_tomorrow']], on=['postcode', 'date'], how='left')
df_final = df_final.dropna(subset=['target_fuel_price_tomorrow'])

# Clean text metadata headers
all_cols = df_final.columns.tolist()
meta_substrings = ['site', 'name', 'brand', 'address', 'suburb', 'state', 'latitude', 'longitude', 'type', 'date', 'region', 'fuelcode']
cols_to_drop = [c for c in all_cols if any(sub in c.lower() for sub in meta_substrings)]
df_validation = df_final.drop(columns=cols_to_drop)

# Force identical column ordering matching training schema
training_data_columns = pd.read_csv('datasets/MODEL_READY_DATASET5.csv', nrows=0).columns.tolist()
df_validation = df_validation[training_data_columns]

# Save output
df_validation.to_csv(QLD_MODEL_READY_FILE, index=False)
print(f"✅ Success! Queensland validation layout mirrors training layout exactly.")