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

# Group the filtered data by 'podCount' and calculate the average
# podCount,alpha,density,replicaCount,nodeCount,CCR,RRC,speedHete
podCount = df_filtered.groupby('podCount')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
alpha = df_filtered.groupby('alpha')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
# density = df_filtered.groupby('density')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
replicaCount = df_filtered.groupby('replicaCount')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
ccr = df_filtered.groupby('CCR')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
speedHete = df_filtered.groupby('speedHete')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()
nodeCount = df_filtered.groupby('nodeCount')[['MPEFTSLR', 'IPPTSSLR', 'HWSSLR']].mean().reset_index()

hatch_patterns = ['+', '/', '.']
colors = ['#1f77b4', '#ff7f0e', '#2ca02c']


print(alpha)
print(podCount)

#========================
# Plotting the bar chart
ax = podCount.plot(x='podCount', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by podCount')
plt.xlabel('podCount')
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/podCount.png')

#========================
# Plotting the bar chart
ax = ccr.plot(x='CCR', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by CCR')
plt.xlabel('CCR')
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

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
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/nodes.png')

#========================
# Plotting the bar chart
ax = alpha.plot(x='alpha', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by alpha')
plt.xlabel('alpha')
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/alpha.png')

# #========================
# # Plotting the bar chart
# ax = density.plot(x='density', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# # Adding titles and labels
plt.title('Average Makespan by density')
# plt.xlabel('density')
# plt.ylabel('SLR')


# # Saving the plot to a file
# plt.savefig('./img/density.png')

#========================
# Plotting the bar chart
ax = replicaCount.plot(x='replicaCount', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by replicaCount')
plt.xlabel('replicaCount')
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/replicaCount.png')

#========================
# Plotting the bar chart
ax = speedHete.plot(x='speedHete', kind='bar', rot=0, figsize=(7, 6), color=['#1f77b4', '#ff7f0e', '#2ca02c'])

# Adding titles and labels
# plt.title('Average Makespan by Heterogeneity')
plt.xlabel('heterogeneity')
plt.ylabel('SLR')

# Applying hatch patterns to each bar
lens=len(ax.patches)/3
for i, bar in enumerate(ax.patches):
    bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
    bar.set_edgecolor('white')
    
# Creating custom legend handles
legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'HWS-BJ'])]

# Adding the custom legend to the plot
plt.legend(handles=legend_handles)

# Saving the plot to a file
plt.savefig('./img/speedHete.png')