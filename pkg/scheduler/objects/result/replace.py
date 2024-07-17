import pandas as pd
import glob

# Read the CSV file
csv_files = glob.glob("./*.csv")
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
df   = pd.concat(df_list, ignore_index=True)

# Replace all values in the 'CCR' column that are 0.2 with 0.25
df['CCR'] = df['CCR'].replace(0.2, 0.25)

# Save the modified DataFrame back to a CSV file
df.to_csv('output.csv', index=False)