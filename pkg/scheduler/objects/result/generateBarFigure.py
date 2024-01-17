import matplotlib.pyplot as plt
import pandas as pd
import sys
plt.close('all')

print("csv path:",sys.argv[1])

# Load the CSV file
df = pd.read_csv(sys.argv[1])

df_filtered = df[df['CUSTOM'] != 0.0]

# Group the filtered data by 'podCount' and calculate the average
# podCount,alpha,density,replicaCount,nodeCount,CCR,RRC,speedHete
podCount = df_filtered.groupby('podCount')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
ccr = df_filtered.groupby('CCR')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()
nodeCount = df_filtered.groupby('nodeCount')[['MPEFT', 'IPPTS', 'CUSTOM']].mean().reset_index()

# Plotting the bar chart
ax = podCount.plot(x='podCount', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by podCount')
plt.xlabel('podCount')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/podCount.png')

# Plotting the bar chart
ax = ccr.plot(x='CCR', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by CCR')
plt.xlabel('CCR')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/CCR.png')

# Plotting the bar chart
ax = nodeCount.plot(x='nodeCount', kind='bar', rot=0, figsize=(10, 6))

# Adding titles and labels
plt.title('Average Makespan by # of Nodes')
plt.xlabel('# of Nodes')
plt.ylabel('Makespan')

# Saving the plot to a file
plt.savefig('./img/nodes.png')