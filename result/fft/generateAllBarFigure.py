import matplotlib.pyplot as plt
import pandas as pd
import glob
import matplotlib.patches as mpatches

def generateFigure(df,type ,group, metric):
    hatch_patterns = ['++', '//', '..', 'xx']
    ax = df.plot(x=group, kind='bar', rot=0, figsize=(7, 4), color=['#44803F', '#B4CF66', '#FFBA5F', '#FF5A33'])
    colors = ['#44803F', '#B4CF66', '#FFBA5F', '#FF5A33']

    if group == "level":
        plt.xlabel("input point(2^n)")
    else:
        plt.xlabel(group)
    
    if metric == "":
        plt.ylabel("average makespan")
    else:
        plt.ylabel(metric)

    lens=len(ax.patches)/4
    for i, bar in enumerate(ax.patches):
        bar.set_hatch(hatch_patterns[int(i/lens)])  # Cycle through patterns
        bar.set_edgecolor('white')

    legend_handles = [mpatches.Patch(facecolor=colors[i], label=label, edgecolor='white', hatch=hatch_patterns[i % len(hatch_patterns)]) for i, label in enumerate(['MPEFT', 'IPPTS', 'MACRO', 'WRC'])]
    plt.legend(handles=legend_handles)
    if metric == '':
        metric = "makespan"
    plt.savefig(f"./img/{type}/{metric}/{group}.png")
    plt.close()

def printBigPicture(big_df):
    mpeft_mean = big_df['MPEFT'].mean()
    mpeft_variance = big_df['MPEFT'].std()

    ippts_mean = big_df['IPPTS'].mean()
    ippts_variance = big_df['IPPTS'].std()

    macro_mean = big_df['MACRO'].mean()
    macro_variance = big_df['MACRO'].std()

    hws_mean = big_df['HWS'].mean()
    hws_variance = big_df['HWS'].std()

    print(f"MPEFT - 平均数: {mpeft_mean}, 標準差: {mpeft_variance}")
    print(f"IPPTS - 平均数: {ippts_mean}, 標準差: {ippts_variance}")
    print(f"MACRO - 平均数: {macro_mean}, 標準差: {macro_variance}")
    print(f"HWS - 平均数: {hws_mean}, 標準差: {hws_variance}")

plt.close('all')

# Get CSV files list from a folder
path = './result'
csv_files = glob.glob(path + "/*.csv")
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
big_df   = pd.concat(df_list, ignore_index=True)
printBigPicture(big_df)

csvAlgorithmName = ['MPEFT', 'IPPTS','MACRO', 'HWS']
metricType = ['', 'SLR', 'speedup', 'efficiency']
OutputGroup = ['level', 'CCR', 'nodeCount']

for metric in metricType:
    csvTitle = [f"{name}{metric}" for name in csvAlgorithmName]
    for group in OutputGroup:
        targetDF=big_df.groupby(group)[csvTitle].mean().reset_index()
        print(targetDF)
        generateFigure(targetDF, 'mean', group, metric)
        
        targetDF=big_df.groupby(group)[csvTitle].std().reset_index()
        generateFigure(targetDF, 'std', group, metric)

