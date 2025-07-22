import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
mpl.rcParams.update({'font.size': 16})  # 统一设置字体大小为14，可根据需要调整
# 示例数据：每个方法在各个数据集上的压缩比
methods_lzvlh = ['LZ-VLH(our)', 'all-decompress(lzvlh)']
methods_lz4 = ['LZ4', 'all-decompress(lz4)']
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
query_time_lzvlh = np.random.rand(len(datasets), len(methods_lzvlh))
query_time_lz4 = np.random.rand(len(datasets), len(methods_lz4))


bar_width = 0.13
x = np.arange(len(datasets))

colors = ['red', 'midnightblue', 'mediumseagreen', 'darkorange', 'mediumpurple',
          'lightpink', 'saddlebrown', 'lightcoral']

fig, ax = plt.subplots(figsize=(14, 6))

df_lzvlh = pd.read_csv('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\system\\tsfile\\system_query_time.csv', header=None)  # 没有标题行
df_lz4 = pd.read_csv('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\system\\tsfile\\query_time_tsfile_lz4.csv', header=None)  # 没有标题行
for i in range (len(datasets)):
    s = df_lzvlh.iloc[i][0]
    a, b = s.split('\t')
    query_time_lzvlh[i][0] = b
    query_time_lzvlh[i][1] = a
    query_time_lz4[i][0] = df_lz4.iloc[i][1]
    query_time_lz4[i][1] = df_lz4.iloc[i][0]


data_size = [
    26303,      # electricity
    99366,      # gps51
    100001,     # samsung
    12000,      # P12data
    9357,       # Air-Quality
    99999,      # CS-Sensors
    7268,       # Cyber-Vehicle
    32211,      # EPM-Education
    2459,       # FANYP-Sensors
    18352,      # GW-Magnetic
    40573,      # Metro-Traffic
    2047,       # Nifty-Stocks
    100001,     # TH-Climate
    46717,      # TRAJET-Transport
    29729,      # TY-Fuel
    39124,      # TY-Transport
    75261       # USGS-Earthquakes
]

query_time_lz4[:, 0] = query_time_lz4[:, 0]/10**6      
query_time_lz4[:, 1] = query_time_lz4[:, 1]/10**6

query_time_lzvlh[:, 0] = query_time_lzvlh[:, 0]/10**6      
query_time_lzvlh[:, 1] = query_time_lzvlh[:, 1]/10**6

for i in range(len(methods_lzvlh)):
    ax.bar(x + i * bar_width, query_time_lzvlh[:, i] / (np.array(data_size)), width=bar_width, label=methods_lzvlh[i], color=colors[i])
for i in range(len(methods_lz4)):
    ax.bar(x + (i+2) * bar_width, query_time_lz4[:, i] / (np.array(data_size)), width=bar_width, label=methods_lz4[i], color=colors[i+2])



ax.set_xticks(x + bar_width * (4 - 1) / 2)
ax.set_xticklabels(datasets, rotation=30, ha='right')
ax.set_ylabel('Time/Point (ms/p)')
#ax.set_xlabel('Dataset')
ax.set_title('System Query Time in Apache TsFile')
ax.set_yscale('log')
ax.legend(ncol=7, loc='upper center')
plt.tight_layout()
plt.savefig('D:\\2025\\DQ\\research\\compressed_search\\compressed_search\\exp\\system\\tsfile\\system_query_normalize_all.png')