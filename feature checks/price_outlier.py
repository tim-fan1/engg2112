import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv('engg2112/COMPLETE_DATASET.csv')

plt.figure(figsize=(10, 6))
sns.boxplot(x=df['Price'], color='skyblue')
plt.title('Evidence: Price Outliers (Identifying unrealistic fuel prices)')
plt.xlabel('Price (cents)')
plt.savefig('engg2112/feature checks/price_outliers.png')