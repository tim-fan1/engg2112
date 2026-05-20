import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv('engg2112/COMPLETE_DATASET.csv')
station_counts = df['ServiceStationName'].value_counts()

plt.figure(figsize=(10, 6))
sns.histplot(station_counts, bins=30, color='orange')
plt.axvline(x=30, color='red', linestyle='--', label='Suggested Threshold (30 days)')
plt.title('Evidence: Data Density per Station')
plt.xlabel('Number of Records per Station')
plt.legend()
plt.savefig('engg2112/feature checks/station_density.png')