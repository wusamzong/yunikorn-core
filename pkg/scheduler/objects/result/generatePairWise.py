import glob
import pandas as pd

def compare_algorithms(df, col1, col2):
    lower = (df[col1] < df[col2]).sum()
    equal = (df[col1] == df[col2]).sum()
    higher = (df[col1] > df[col2]).sum()

    return {'lower': lower, 'equal': equal, 'higher': higher}


path = '.'
csv_files = glob.glob(path + "/*.csv")
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
big_df = pd.concat(df_list, ignore_index=True)


df_filtered = big_df[big_df['HWS-BJ'] != 0.0]
df_filtered = big_df[big_df['MPEFT'] != 0.0]
df_filtered = big_df[big_df['IPPTS'] != 0.0]

results = {}
columns_to_compare = ['MPEFT', 'IPPTS', 'HWS-BJ']

for i, col1 in enumerate(columns_to_compare):
    for col2 in columns_to_compare[i+1:]:
        key = f'{col1} vs {col2}'
        results[key] = compare_algorithms(df_filtered, col1, col2)

# Convert results to a DataFrame for a nicer display
comparison_df = pd.DataFrame(results).T

print(comparison_df)