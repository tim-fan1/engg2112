import pandas as pd
import os

# -----------------------------
# 1. SETUP & FILE PATHS
# -----------------------------
# Adjust these names if your files are named differently
FUEL_FILE = "engg2112/datasets/fuel/6-month fuel datasets final.xlsx"
WEATHER_FILE = "engg2112/datasets/weather/weather_only_dataset.csv"
OIL_FILE = "engg2112/datasets/oil/daily oil price.xlsx"
TGP_FILE = "engg2112/datasets/tgp/petrol_tgp.csv"
FX_FILE = "engg2112/datasets/exchange/2023-current.xls"
OUTPUT_FILE = "engg2112/COMPLETE_DATASET.csv"

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
# 2. LOAD & CLEAN DATASETS
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
# 3. STANDARDIZE DATES & NAMES
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
# Convert FX Date (ROBUST VERSION)
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

# Debug check (optional but recommended)
print("FX data preview:")
print(fx_data.head())
print("FX date range:", fx_data['date'].min(), "to", fx_data['date'].max())

# -----------------------------
# 4. MULTI-STEP MERGE
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
# 5. HANDLE WEEKENDS (Forward Fill)
# -----------------------------
print("Step 4: Filling weekend gaps for market data...")
# Market data (Oil, TGP, FX) is missing on weekends. 
# We fill forward so Saturday/Sunday uses Friday's price.
df_final = df_final.sort_values(by=['date', 'ServiceStationName'])
cols_to_fill = ['oil_price', 'tgp_sydney', 'aud_usd']
df_final[cols_to_fill] = df_final[cols_to_fill].ffill().bfill()

# -----------------------------
# 6. SAVE
# -----------------------------
# Get the folder path
output_dir = os.path.dirname(OUTPUT_FILE)

# Only try to create the folder if a folder path actually exists
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

df_final.to_csv(OUTPUT_FILE, index=False)

print("-" * 30)
print(f"SUCCESS: {OUTPUT_FILE} is ready.")
print(f"Total Rows: {len(df_final)}")
print(f"Variables: Price, Temp, Rain, Oil, TGP, FX (AUD/USD)")
print("-" * 30)