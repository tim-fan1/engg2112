import pandas as pd

# 1. Define your file paths and the sheet name
excel_file = 'engg2112/datasets/tgp/AIP_TGP_Data_27-Mar-2026.xlsx'
sheet_to_extract = 'Petrol TGP'  # Can also use the integer index (e.g., 0)
output_csv = 'engg2112/datasets/tgp/petrol_tgp.csv'

try:
    # 2. Read the specific sheet into a DataFrame
    df = pd.read_excel(excel_file, sheet_name=sheet_to_extract)

    # 3. Export the DataFrame to a CSV file
    # index=False prevents pandas from adding an extra column for row numbers
    df.to_csv(output_csv, index=False)
    
    print(f"Success! '{sheet_to_extract}' has been saved to {output_csv}")

except Exception as e:
    print(f"An error occurred: {e}")