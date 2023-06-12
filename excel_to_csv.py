import pandas as pd

# Load the Excel file
xls = pd.ExcelFile('data_file.xlsx')

# Get the names of all sheets in the Excel file
sheet_names = xls.sheet_names

# Iterate over each sheet and save it as a separate CSV
for sheet in sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet)
    df.to_csv(sheet + '.csv', index=False) 
