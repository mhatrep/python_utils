import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt

# Read the data from your CSV
df = pd.read_csv('c:/data/data_file.csv')

# Track how many tables each column appears in
column_tables = defaultdict(set)

for i in range(len(df)):
    table_name = df.iloc[i]['table_name']
    column_name = df.iloc[i]['column_name']
    column_tables[column_name].add(table_name)

# Create a new DataFrame
df_new = pd.DataFrame([
    {'column_name': column_name, 'table_names': ', '.join(tables)}
    for column_name, tables in column_tables.items() if len(tables) > 1
])

# Calculate max string length for each column
column_widths = [df_new[col].str.len().max() for col in df_new.columns]
column_width_ratio = [width/max(column_widths) for width in column_widths]

# Create figure
fig, ax = plt.subplots(1, 1)

# Remove axis
ax.axis('tight')
ax.axis('off')

# Create table and display it
table = ax.table(cellText=df_new.values, colLabels=df_new.columns, cellLoc='center', loc='center')
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 1.5)

# Adjust column widths
# Adjust column widths
for idx in range(len(df_new.columns)):
    table.auto_set_column_width(idx)

plt.show()
