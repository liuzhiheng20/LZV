package org.apache.tsfile.compressedQuery;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataPrepare {
  // 将数据按照对齐时间序列的方式写入文件
  private static final List<String> pathList = new ArrayList<>();
  private static final List<Integer> columnList = new ArrayList<>();
  private static final List<String> timePathList = new ArrayList<>();
  private static final List<Integer> timeColumnList = new ArrayList<>();

  public static int CSV_NUM = 0;
  private static final Logger logger = LoggerFactory.getLogger(DataPrepare.class);

  static final String SENSOR_1 = "sensor_1";
  static final String SENSOR_7 = "sensor_7";

  static final String DEVICE_PREFIX = "device_";
  static final String DEVICE_1 = "root.sg.device_1";
  static final String DEVICE_2 = "root.sg.device_2";

  public static void main(String[] args) throws IOException {
    initializeList();
    System.out.println("Current working directory: " + System.getProperty("user.dir"));
    for (int i = 0; i < CSV_NUM; i++) {
      File file = new File(pathList.get(i));
      String parentPath = file.getParent(); // 得到 "/home/user/data/files"
      int lastFolderIndex = parentPath.lastIndexOf(File.separator) + 1;
      System.out.println("Last folder name: " + parentPath.substring(lastFolderIndex));
      File f =
          FSFactoryProducer.getFSFactory()
              .getFile(
                  "D:\\senior\\DQ\\research\\compressed_search_paper\\code\\tsfile\\tsfile_data\\tsfile_data_lz77\\"
                      + parentPath.substring(lastFolderIndex)
                      + ".tsfile");
      if (f.exists()) {
        try {
          Files.delete(f.toPath());
        } catch (IOException e) {
          throw new IOException("can not delete " + f.getAbsolutePath());
        }
      }

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
        measurementSchemas.add(new MeasurementSchema(SENSOR_1, TSDataType.INT64, TSEncoding.PLAIN));
        // register timeseries
        tsFileWriter.registerAlignedTimeseries(new Path(DEVICE_1), measurementSchemas);

        // example1
        writeAligned(tsFileWriter, DEVICE_1, measurementSchemas, pathList.get(i));
      } catch (WriteProcessException e) {
        logger.error("write TSRecord failed", e);
      }
    }
  }

  private static void writeAligned(
      TsFileWriter tsFileWriter, String deviceId, List<IMeasurementSchema> schemas, String filePath)
      throws IOException, WriteProcessException {
    long timeNow = -100;
    List<Long> times = readColumnByIndex(filePath, 0);
    List<Long> values = readColumnByIndex(filePath, 1);
    for (int i = 0; i < times.size(); i++) {
      // construct TsRecord, 清除乱序的情况
      if (times.get(i) > timeNow) {
        timeNow = times.get(i);
        System.out.println(times.get(i));
        TSRecord tsRecord = new TSRecord(deviceId, times.get(i));
        for (IMeasurementSchema schema : schemas) {
          tsRecord.addTuple(
              DataPoint.getDataPoint(
                  schema.getType(),
                  schema.getMeasurementName(),
                  Objects.requireNonNull(values.get(i)).toString()));
        }
        // write
        tsFileWriter.writeRecord(tsRecord);
      }
    }
    tsFileWriter.flush();
  }

  public static List<Long> readColumnByIndex(String filePath, int columnIndex) throws IOException {
    List<Long> columnData = new ArrayList<>();

    try (BufferedReader br =
        Files.newBufferedReader(Paths.get(filePath), Charset.forName("ISO-8859-1"))) {
      // 跳过第一行标题
      br.readLine();

      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        if (tokens.length > columnIndex) {
          try {
            double value = Double.parseDouble(tokens[columnIndex]);
            long value2 = (long) value;
            columnData.add(value2);
          } catch (NumberFormatException e) {
            throw new IOException("解析错误 [" + tokens[columnIndex] + "] in line: " + line);
          }
        }
      }
    }
    return columnData;
  }

  public static void initializeList() {
    String content = null;
    try {
      content =
          new String(
              Files.readAllBytes(
                  Paths.get(
                      "D:\\senior\\DQ\\research\\compressed_search_paper\\code\\data\\data\\config.json")));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // 简单查找匹配
    String[] lines = content.split("\\{");
    for (String line : lines) {
      if (line.contains("\"path\"")) {
        String path = line.split("\"path\"\\s*:\\s*\"")[1].split("\"")[0];
        String columnStr = line.split("\"column\"\\s*:\\s*")[1].split("}")[0].trim();
        int column = Integer.parseInt(columnStr.replace(",", ""));
        System.out.println("Path: " + path + ", Column: " + column);
        pathList.add(path);
        columnList.add(column);
        CSV_NUM++;
      }
    }
  }
}
