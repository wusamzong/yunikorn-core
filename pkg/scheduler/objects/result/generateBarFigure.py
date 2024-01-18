import matplotlib.pyplot as plt
import pandas as pd
import glob
plt.close('all')

# Get CSV files list from a folder
path = '.'
csv_files = glob.glob(path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
big_df   = pd.concat(df_list, ignore_index=True)

# print("csv path:",sys.argv[1])

# # Load the CSV file
# df = pd.read_csv(sys.argv[1])

df_filtered = big_df[big_df['CUSTOM'] != 0.0]

# Group the filtered data by 'podCount' and calculate the average
# podCount,alpha,density,replicaCount,nodeCount,CCR,RRC,speedHete
podCount = df_filtered.groupby('podCount')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
alpha = df_filtered.groupby('alpha')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
density = df_filtered.groupby('density')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
replicaCount = df_filtered.groupby('replicaCount')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
ccr = df_filtered.groupby('CCR')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
nodeCount = df_filtered.groupby('nodeCount')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()

#========================
# Plotting the bar chart
ax = podCount.plot(x='podCount', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by podCount')
plt.xlabel('podCount')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/podCount.png')

#========================
# Plotting the bar chart
ax = ccr.plot(x='CCR', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by CCR')
plt.xlabel('CCR')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/CCR.png')

#========================
# Plotting the bar chart
ax = nodeCount.plot(x='nodeCount', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Resource Usage by # of Nodes')
plt.xlabel('# of Nodes')
plt.ylabel('Resource Usage')

# Saving the plot to a file
plt.savefig('./img/nodes.png')

#========================
# Plotting the bar chart
ax = alpha.plot(x='alpha', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by alpha')
plt.xlabel('alpha')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/alpha.png')

#========================
# Plotting the bar chart
ax = density.plot(x='density', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by density')
plt.xlabel('density')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/density.png')

#========================
# Plotting the bar chart
ax = replicaCount.plot(x='replicaCount', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by replicaCount')
plt.xlabel('replicaCount')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/replicaCount.png')