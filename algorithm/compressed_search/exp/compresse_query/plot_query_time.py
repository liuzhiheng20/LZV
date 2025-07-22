import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
mpl.rcParams.update({'font.size': 16})  # 统一设置字体大小为14，可根据需要调整
# 示例数据：每个方法在各个数据集上的压缩比
methods = ['LZ-VLH(our)', 'LZ77','LZ4', 'decompress(ku)', 'decompress(our)']
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
query_time = np.random.rand(len(datasets), len(methods))


bar_width = 0.13
x = np.arange(len(datasets))

colors = ['red', 'midnightblue', 'mediumseagreen', 'darkorange', 'mediumpurple',
          'lightpink', 'saddlebrown', 'lightcoral']

fig, ax = plt.subplots(figsize=(14, 6))

df = pd.read_csv('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\compressed_query\\query_time.csv', header=None)  # 没有标题行
for i in range (len(datasets)):
    for j in range (len(methods)):
        query_time[i][j] = df.iloc[i][j]
            
print(query_time)

for i in range(len(methods)):
    ax.bar(x + i * bar_width, query_time[:, i] / 10**6, width=bar_width, label=methods[i], color=colors[i])

ax.set_xticks(x + bar_width * (len(methods) - 1) / 2)
ax.set_xticklabels(datasets, rotation=30, ha='right')
ax.set_ylabel('Time (ms)')
#ax.set_xlabel('Dataset')
ax.set_title('Compression Index Search Time')
ax.set_yscale('log')
ax.legend(ncol=7, loc='upper center')
plt.tight_layout()
plt.savefig('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\compressed_query\\value_index_query.png')