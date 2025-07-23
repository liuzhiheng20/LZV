paper:Improving LZ4 for Effective Compression and Efficient query
## Datasets

### Samsung
This dataset [^samsung] is generated using standard commercial protocols, networks, and devices commonly found in IoT applications, corresponding to several real-world usage scenarios.  
**Timestamps:** Not at consistent intervals.

### Electricity
The electricity dataset [^electricity] contains hourly power consumption data for 320 customers from July 2016 to July 2019.  
The first column of the dataset provides the timestamp.  
**Timestamps:** At consistent intervals.

### GPS
Each Excel file corresponds to the data of one device (4 devices total), equipped with 56 sensors. The sensor data for each device is aligned in sequence. Device IDs have been anonymized.  
**Timestamps:** Not at consistent intervals.

### P12
The P12/PhysioNet dataset [^p12] comprises clinical data from 11,988 ICU patients (12 removed due to issues). For each patient, it includes 36 irregularly sampled sensor observations and 5 static demographic features collected during the first 48 hours of ICU stay.  
**Timestamps:** Not at consistent intervals.

### AirQuality
This dataset [^airquality] is from the UCI Machine Learning Repository. It contains hourly sensor readings and pollutant concentrations (e.g., CO, NOx, NO₂, C₆H₆) collected in Italy from March 2004 to April 2005.  
**Timestamps:** At consistent intervals.

### REGER Datasets
We also use all 12 datasets from the study of REGER [^reger].  
A summary of these datasets is shown below:

| Dataset           | Description |
|------------------|-------------|
| EPM-Education    | Collected from subjects using a logging application while learning with an educational simulator. |
| GW-Magnetic      | Multisource and multivariate dataset for indoor localization based on WLAN and geomagnetic fields. |
| Metro-Traffic    | Contains hourly traffic volume data for a street. |
| Nifty-Stocks     | Includes open, high, low, close prices, and volume for Nifty 50 stocks. |
| USGS-Earthquakes | Captured by the US Geological Survey, with earthquake magnitude and location data across the US and surrounding areas. |
| CS-Sensors       | Collected by a ship manufacturer, monitoring ship locations via buoy sensors. |
| CyberVehicle     | Monitors engine status of concrete mixer trucks, such as travel distance, torque, and rotational speed. |
| TH-Climate       | Weather station data collected by wind sensors, including humidity, pressure, wind speed and direction. |
| TY-Fuel          | Records vehicle engine fuel consumption, including fuel amount and consumption rate per trip. |
| TY-Transport     | Contains transportation times and vehicle data such as speed and direction at specific locations. |
| FANYP-Sensors    | Data from monitoring electronic sensor equipment. |
| TRAJET-Transport | Trip data collected from different transportation modes. |



## Algorithm
We evaluated the performance of the LZV algorithm and the effectiveness of compressed-data queries at the algorithm level.
### Compression performance
In `CompressTest.java`, we tested the compression performance of LZV against LZ4, and LZ77, including compression ratio, compression time, and decompression time.
### Compressed query performance
In `QueryTest.java`, we tested the performance of compressed-data queries using fully decompression query or compressed-data query.
## System
We use the Java version of TsFile for system-level testing. The specific test code is as follows.

### Config
In `TsFileConfig.java`, you can configure data storage with specific encoding and compression methods by setting the `timeEncoding` and `compressor` parameters.
### Data prepare
In `DataPrepare.java`, data from the Dataset is written into TsFile files under the corresponding directories using the specified compression method, in preparation for later query testing.
### Data query
In `DataQuery.java`, we tested the time performance of two different query methods for compressed data in TsFile: fully decompressed query and compressed-data query.