import pandas as pd

# Read the CSV file
df = pd.read_csv('your_file.csv')

# Iterate over each column
for col in df.columns:
    # Check if the column is of numeric type
    if df[col].dtype == 'object':  # 'object' is typically used for non-numeric data in pandas
        # Print unique values
        print(f"Column: {col}, Unique Values: {df[col].unique()}")
