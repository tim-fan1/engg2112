import requests
import pandas as pd
import io

# Using your exact links to bypass the naming inconsistencies
FUEL_LINKS = [
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/c350fcce-32f2-4e13-b569-a90a47c46ffa/download/price_history_checks_feb2025.csv",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/215bafd0-031e-4106-9b59-694e0f222e5f/download/fuelcheck_pricehistory_mar25.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/9931c7da-83d6-4ca7-9441-9305e0f851e1/download/fuelcheck_pricehistory_apr2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/36a03fb6-ff84-48e2-956f-dd1e0365f38b/download/fuelcheck_pricehistory_may2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/df5c9553-433c-4a90-a5a9-de19ecc543f6/download/price_history_checks_jun2025.csv",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/55416beb-6bba-46ff-ae35-a238a2001f08/download/fuelcheck_pricehistory_july2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/e8452b3b-2e02-4577-8f39-35a07b9e99b9/download/fuelcheck_pricehistory_aug2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/e12a757e-a9fe-4cbe-9034-55f00314c72b/download/fuelcheck_pricehistory_sep2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/c5ae66f9-9324-49b9-8f90-07cd6eb12d42/download/price_history_checks_oct2025.csv",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/e9dc591d-17ee-4433-9d75-da75b4f09237/download/fuelcheck_pricehistory_nov2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/6794ad02-fd2b-44a6-be9c-55f726b266c0/download/fuelcheck_pricehistory_dec2025.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/340d3518-4d5c-4e6e-a30d-e5d4be3e4edb/download/fuelcheck_pricehistory_jan2026.xlsx",
    "https://data.nsw.gov.au/data/dataset/a97a46fc-2bdd-4b90-ac7f-0cb1e8d7ac3b/resource/3786820f-8efd-4b13-b2ba-56096a9d42b3/download/price_history_checks_feb2026.csv"
]

def download_and_filter_nsw_fuel():
    master_list = []
    
    for url in FUEL_LINKS:
        file_name = url.split('/')[-1]
        print(f"Downloading {file_name}...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Use BytesIO because Excel files are binary
            file_content = io.BytesIO(response.content)
            
            # Switch reader based on extension
            if file_name.endswith('.xlsx'):
                df = pd.read_excel(file_content)
            else:
                # Some CSVs use latin-1 encoding
                df = pd.read_csv(file_content, encoding='utf-8-sig')
            
            # Filter for E10
            e10_df = df[df['FuelCode'] == 'E10'].copy()
            master_list.append(e10_df)
            print(f"  Success: Extracted {len(e10_df)} E10 records.")
            
        except Exception as e:
            print(f"  Failed to process {file_name}: {e}")

    if master_list:
        final_csv = pd.concat(master_list, ignore_index=True)
        final_csv.to_csv("datasets/fuel/NSW_E10_Full_Year_25_26.csv", index=False)
        print("\n--- Done! File saved as 'NSW_E10_Full_Year_25_26.csv' ---")

if __name__ == "__main__":
    download_and_filter_nsw_fuel()