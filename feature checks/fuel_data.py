import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('engg2112/COMPLETE_DATASET.csv')
df['date'] = pd.to_datetime(df['date'])

# Generate full date range and count occurrences
all_dates = pd.date_range(start=df['date'].min(), end=df['date'].max())
daily_counts = df.groupby('date').size().reindex(all_dates, fill_value=0)

# Visualization
plt.figure(figsize=(12, 5))
daily_counts.plot(kind='line', color='blue', linewidth=1.5)
plt.title('Evidence: Daily Record Counts (Gaps indicate missing data)')
plt.xlabel('Date')
plt.ylabel('Number of Price Updates')
plt.grid(True, alpha=0.3)
plt.savefig('engg2112/feature checks/continuity_check.png')