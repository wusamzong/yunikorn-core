import pandas as pd
import glob

# 讀取所有 CSV 文件
file_paths = glob.glob('./randomDAG/*.csv')

# 創建一個空的 DataFrame
combined_df = pd.DataFrame()

# 迭代讀取每個 CSV 文件，並將其添加到 DataFrame 中
for file_path in file_paths:
    df = pd.read_csv(file_path)
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# 依序按指定欄位進行排序
# 例如，依據 'podCount', 'replicaCount' 和 'nodeCount' 進行排序
sorted_df = combined_df.sort_values(by=['podCount', 'replicaCount', 'nodeCount','CCR','CTV','TCR','stageCount'])

sorted_df = sorted_df[sorted_df['CCR']!=4]
sorted_df = sorted_df[sorted_df['CCR']!=0.25]
sorted_df = sorted_df[sorted_df['replicaCount']!=8]

# 將結果寫入新的 CSV 文件
sorted_df.to_csv('./randomDAG/sorted_combined.csv', index=False)

print("CSV files have been combined and sorted successfully.")