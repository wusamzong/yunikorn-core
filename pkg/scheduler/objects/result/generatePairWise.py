import glob
import pandas as pd
import random
def compare_algorithms(df, col1, col2):
    lower = (df[col1] < df[col2]).sum()
    equal = (df[col1] == df[col2]).sum()
    higher = (df[col1] > df[col2]).sum()
    sum = lower + equal + higher
    return {'lower': lower/sum, 'equal': equal/sum, 'higher': higher/sum}


path = './randomDAG'
csv_files = glob.glob(path + "/*.csv")
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
big_df = pd.concat(df_list, ignore_index=True)
# big_df = big_df[big_df['podCount'] != 1100]
# big_df = big_df[big_df['replicaCount'] != 8]
# big_df = big_df[big_df['CCR'] != 5]
# big_df = big_df[big_df['CCR'] != 4]
random.seed(5)
# big_df['MPEFT']=big_df['MPEFT'].multiply(random.uniform(0.8,1.0))
# big_df['IPPTS']=big_df['IPPTS'].multiply(random.uniform(0.87,0.95))

df_filtered = big_df[big_df['HWS'] != 0.0]
df_filtered = big_df[big_df['MPEFT'] != 0.0]
df_filtered = big_df[big_df['IPPTS'] != 0.0]
df_filtered = big_df[big_df['MACRO'] != 0.0]

results = {}
columns_to_compare = ['HWS', 'MPEFT', 'IPPTS','MACRO']

for i, col1 in enumerate(columns_to_compare):
    for col2 in columns_to_compare[i+1:]:
        key = f'{col1} vs {col2}'
        results[key] = compare_algorithms(df_filtered, col1, col2)

# Convert results to a DataFrame for a nicer display
comparison_df = pd.DataFrame(results).T

print(comparison_df.to_csv(index=True))