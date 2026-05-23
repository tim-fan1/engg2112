# Check user is running this script from the 'engg2112' home directory.
import os
current_folder = os.path.basename(os.getcwd())
if current_folder != "engg2112":
    print("Error: Please run this script from the 'engg2112' home directory.")
    exit()  # Stops the script immediately

import pandas as pd

# 1. Define your file paths and the sheet name
excel_file = 'datasets/tgp/AIP_TGP_Data_27-Mar-2026.xlsx'
sheet_to_extract = 'Petrol TGP'  
output_csv = 'datasets/qld/qld_tgp.csv'

try:
    # 2. Read the specific sheet into a DataFrame
    df = pd.read_excel(excel_file, sheet_name=sheet_to_extract)

    # 3. CRITICAL UPDATE: Extract Column 0 (Date) and Column 3 (Brisbane)
    # We use .copy() to avoid "SettingWithCopy" warnings later.
    df = df.iloc[:, [0, 3]].copy()

    # 4. Clean up the data
    # Rename columns to standard names - saving Brisbane directly
    df.columns = ['date', 'brisbane_tgp']

    # Drop any rows that are empty or contain header text instead of dates
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Ensure data values are sorted oldest-to-newest for lag computations
    df = df.sort_values('date').reset_index(drop=True)

    # 5. Export the DataFrame to a CSV file
    df.to_csv(output_csv, index=False)
    
    print("-" * 70)
    print(f"Success! Brisbane data extracted from '{sheet_to_extract}'")
    print(f"Saved to: {output_csv}")
    print("-" * 70)
    print("Preview of extracted Brisbane data:")
    print(df.tail())

except Exception as e:
    print(f"An error occurred: {e}")