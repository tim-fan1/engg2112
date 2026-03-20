import pandas as pd

# Load the user-provided dataset
df = pd.read_csv('Mental Health dataset1.csv')

# Display basic information
print(df.info())
print(df.head())
print(df.columns)