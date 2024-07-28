import matplotlib.pyplot as plt
import pandas as pd
import glob
import matplotlib.patches as mpatches
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

df_filtered = big_df[big_df['HWS'] != 0.0]
# df_filtered = df_filtered[df_filtered['CCR']!=20]
# df_filtered = df_filtered[df_filtered['CCR']!=20]

# Group the filtered data by 'podCount' and calculate the average
# podCount,alpha,density,replicaCount,nodeCount,CCR,RRC,speedHete
level = df_filtered.groupby('level')[['MPEFT', 'IPPTS', 'HWS']].mean().reset_index()
ccr = df_filtered.groupby('CCR')[['MPEFT', 'IPPTS', 'HWS']].mean().reset_index()
nodeCount = df_filtered.groupby('nodeCount')[['MPEFT', 'IPPTS', 'HWS']].mean().reset_index()

hatch_patterns = ['+', '/', '.']
colors = ['#1f77b4', '#ff7f0e', '#2ca02c']

#========================
# Plotting the bar chart
ax = level.plot(x='level', kind='bar', rot=0, figsize=(7, 4), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by level')
plt.xlabel('input Points(2^n)')
plt.ylabel('Makespan')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/level.png')

#========================
# Plotting the bar chart
ax = ccr.plot(x='CCR', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by CCR')
plt.xlabel('CCR')
plt.ylabel('Makespan')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/CCR.png')
#========================
# Plotting the bar chart
ax = nodeCount.plot(x='nodeCount', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Resource Usage by # of Nodes')
plt.xlabel('# of Nodes')
plt.ylabel('Makespan')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/nodes.png')