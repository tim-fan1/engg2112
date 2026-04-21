import pandas as pd

# Load your dataset
df = pd.read_csv('engg2112/datasets/fuel/fuelcheck_pricehistory_2026_02.csv')

# Get a list of unique postcodes
unique_postcodes = df['Postcode'].unique().tolist()

print(unique_postcodes)