import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
mpl.rcParams.update({'font.size': 16})  # 统一设置字体大小为14，可根据需要调整
# 示例数据：每个方法在各个数据集上的压缩比
methods = ['LZ-VLH(our)', 'LZ-VLH(PLUS)', 'LZ77','LZ4(speed)', 'LZ4(ratio)', 'GZIP', 'Snappy', 'LZMA']
methods_new = ['LZ-VLH(our)', 'LZ77','LZ4(speed)', 'LZ4(ratio)']
#datasets = ['Electricity', 'GPS', 'Samsung', 'P12', 'AirQuality']
datasets = [
    'Electricity',         # electricity_new.csv
    'GPS',                 # gps51/root.sg.track13.d2_new.csv
    'Samsung',             # s-10_cleaned_new_sorted.csv
    'P12',                 # P12data.csv
    'AirQuality',          # Air-Quality_new.csv
    'CS-Sensors',          # CS-Sensors/test.csv
    'Cyber-Vehicle',       # Cyber-Vehicle/syndata_vehicle0.csv
    'EPM-Education',       # EPM-Education/epm_0.csv
    'FANYP-Sensors',       # FANYP-Sensors/data_20230626_000000_1.csv
    'GW-Magnetic',         # GW-Magnetic/syndata_magnetic0.csv
    'Metro-Traffic',       # Metro-Traffic/syndata_metro.csv
    'Nifty-Stocks',        # Nifty-Stocks/syndata_stocks0.csv
    'TH-Climate',          # TH-Climate/syndata_climate0.csv
    'TRAJET-Transport',    # TRAJET-Transport/trajet_0.csv
    'TY-Fuel',             # TY-Fuel/syndata_fuel0.csv
    'TY-Transport',        # TY-Transport/syndata_transport0.csv
    'USGS-Earthquakes'     # USGS-Earthquakes/syndata_earthquakes0.csv
]
# 生成随机压缩比数据（请根据你的真实数据替换）
np.random.seed(0)
timeRatio = np.random.rand(len(datasets), len(methods))
valueRatio = np.random.rand(len(datasets), len(methods))
timePredictRatio = np.random.rand(len(datasets), len(methods))


bar_width = 0.13
x = np.arange(len(datasets))

colors = ['red', 'midnightblue', 'mediumseagreen', 'darkorange', 'mediumpurple',
          'lightpink', 'saddlebrown', 'lightcoral']

fig, ax = plt.subplots(figsize=(14, 6))

#df = pd.read_csv('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\original_data\\ratio_all_datases.csv', header=None)  # 没有标题行
df = pd.read_csv('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\data\\output.csv', header=None)
for i in range (len(datasets)):
    for j in range (len(methods)):
        timeRatio[i][j] = df.iloc[i][3+3*j]
        valueRatio[i][j] = df.iloc[i][3+3*j+1]
        timePredictRatio[i][j] = df.iloc[i][3+3*j+2]
            
print(timeRatio)
timeRatio_new = np.zeros((len(datasets), len(methods_new)))
valueRatio_new = np.zeros((len(datasets), len(methods_new)))
timePredictRatio_new = np.zeros((len(datasets), len(methods_new)))
for i in range (len(datasets)):
    for j in range (len(methods_new)):
        index = 0
        if j==1: index = 2
        if j==2: index = 3
        if j==3: index = 4
        timeRatio_new[i][j] = timeRatio[i][index]
        valueRatio_new[i][j] = valueRatio[i][index]
        timePredictRatio_new[i][j] = timePredictRatio[i][index]

for i in range(len(methods_new)):
    # 底部：valueRatio 带斜线，表示压缩值的时间
    ax.bar(
        x + i * bar_width,
        valueRatio_new[:, i]/2,
        width=bar_width,
        color=colors[i],
        #edgecolor='dimgray',
        edgecolor='black',
        linewidth=0.5,
    )
    
    # 顶部：timePredictRatio 无斜线，堆叠在上
    ax.bar(
        x + i * bar_width,
        timePredictRatio_new[:, i]/2,
        width=bar_width,
        color=colors[i],
        bottom=valueRatio_new[:, i]/2,
        label=methods_new[i],
        alpha=0.7,
    )

ax.set_xticks(x + bar_width * (len(methods) - 1) / 2)
ax.set_xticklabels(datasets, rotation=30, ha='right')
ax.set_ylabel('Compression Ratio')
ax.set_xlabel('Dataset')
ax.set_title('Compression Ratio')
ax.set_ylim(0, 0.8)
ax.legend(ncol=7, loc='upper center')
plt.tight_layout()
plt.savefig('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\compression_ratio_alldataset_new.png')