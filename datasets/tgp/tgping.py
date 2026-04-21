# Check user is running this script from the 'engg2112' home directory.
import os
current_folder = os.path.basename(os.getcwd())
if current_folder != "engg2112":
    print("Error: Please run this script from the 'engg2112' home directory.")
    exit()  # Stops the script immediately

# User is in the right folder! Continue with script.
import pandas as pd

# 1. Define your file paths and the sheet name
excel_file = 'datasets/tgp/AIP_TGP_Data_27-Mar-2026.xlsx'
sheet_to_extract = 'Petrol TGP'  # Can also use the integer index (e.g., 0)
output_csv = 'datasets/tgp/petrol_tgp.csv'

try:
    # 2. Read the specific sheet into a DataFrame
    df = pd.read_excel(excel_file, sheet_name=sheet_to_extract)

    # 3. Export the DataFrame to a CSV file
    # index=False prevents pandas from adding an extra column for row numbers
    df.to_csv(output_csv, index=False)
    
    print(f"Success! '{sheet_to_extract}' has been saved to {output_csv}")

except Exception as e:
    print(f"An error occurred: {e}")