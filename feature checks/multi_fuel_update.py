import pandas as pd

# 1. Load the dataset
df = pd.read_csv('engg2112/COMPLETE_DATASET.csv')

# 2. Ensure date column is in datetime format
df['date'] = pd.to_datetime(df['date'])

# 3. Group by Station, Fuel Type, and Date, then count entries
update_summary = df.groupby(['ServiceStationName', 'FuelCode', 'date']).size().reset_index(name='update_count')

# 4. Filter for records where update_count is greater than 1
multiple_updates = update_summary[update_summary['update_count'] > 1]

# 5. Display the results
if not multiple_updates.empty:
    print(f"Found {len(multiple_updates)} station-days with multiple price updates.")
    print("\nTop 10 stations with the most frequent daily updates:")
    print(multiple_updates.sort_values(by='update_count', ascending=False).head(10))
    
    # Optional: Look at a specific example to see the price variance
    example = multiple_updates.iloc[0]
    specific_rows = df[
        (df['ServiceStationName'] == example['ServiceStationName']) & 
        (df['date'] == example['date'])
    ]
    print(f"\nExample details for {example['ServiceStationName']} on {example['date'].date()}:")
    print(specific_rows[['ServiceStationName', 'Price', 'date']])
else:
    print("No multiple updates found. Every station has at most one price entry per day.")