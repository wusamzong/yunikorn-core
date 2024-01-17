import matplotlib.pyplot as plt
import sys
import pandas as pd

def compare_algorithms(df, col1, col2):
    lower = (df[col1] < df[col2]).sum()
    equal = (df[col1] == df[col2]).sum()
    higher = (df[col1] > df[col2]).sum()

    return {'lower': lower, 'equal': equal, 'higher': higher}


print("csv path:",sys.argv[1])

# Load the CSV file
df = pd.read_csv(sys.argv[1])

df_filtered = df[df['CUSTOM'] != 0.0]

results = {}
columns_to_compare = ['MPEFT', 'IPPTS', 'CUSTOM']

for i, col1 in enumerate(columns_to_compare):
    for col2 in columns_to_compare[i+1:]:
        key = f'{col1} vs {col2}'
        results[key] = compare_algorithms(df_filtered, col1, col2)

# Convert results to a DataFrame for a nicer display
comparison_df = pd.DataFrame(results).T

print(comparison_df)