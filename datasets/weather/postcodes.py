import pandas as pd
# needed to install openpyxl to read excel files, use pip install openpyxl in terminal

# Load your dataset
df = pd.read_excel('engg2112/datasets/fuel/6-month fuel datasets final.xlsx')

postcode_counts = df['Postcode'].value_counts()

print(postcode_counts.head(10))

print(df['Postcode'].unique())