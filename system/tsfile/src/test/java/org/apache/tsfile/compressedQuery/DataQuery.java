package org.apache.tsfile.compressedQuery;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.CompressedTsFileExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataQuery {
  // 现有方法对压缩数据进行查询
  private static final Logger LOGGER = LoggerFactory.getLogger(DataQuery.class);
  private static final List<String> pathsList = new ArrayList<>();
  static final String SENSOR_1 = "sensor_1";

  static final String DEVICE_PREFIX = "device_";
  static final String DEVICE_1 = "root.sg.device_1";
  static final String DEVICE_2 = "root.sg.device_2";

  private static void queryAndPrint(
      ArrayList<Path> paths, TsFileReader readTsFile, IExpression statement) throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      String next = queryDataSet.next().toString();
      LOGGER.info(next);
    }
    LOGGER.info("----------------");
  }

  private static long Query(List<Long> times, ArrayList<Path> paths, String path)
      throws IOException {
    // 对tsfile中times中对应的全部时间戳进行查询
    long startTime = System.nanoTime();
    for (Long time : times) {
      TsFileSequenceReader reader = new TsFileSequenceReader(path);
      TsFileReader readTsFile = new TsFileReader(reader);
      // System.out.println(time);
      IExpression timeFilter = new GlobalTimeExpression(TimeFilterApi.eq(time));
      QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
      if (time == 1407240584099L) {
        System.out.println(time);
      }
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      boolean isFound = false;
      while (queryDataSet.hasNext()) {
        String next = queryDataSet.next().toString();
        // System.out.println(next);
        // LOGGER.info(next);
        isFound = true; // 1407240420589
      }
      if (!isFound) {
        break;
      }
      LOGGER.info("----------------");
    }
    long endTime = System.nanoTime();
    long duration = endTime - startTime;
    System.out.println("解压查询运行时间: " + duration + " 毫秒");
    return duration;
  }

  private static long CompressedQuery(List<Long> times, ArrayList<Path> paths, String path)
      throws IOException {
    // 对tsfile中times中对应的全部时间戳进行查询
    long startTime = System.nanoTime();
    for (Long time : times) {
      // System.out.println(time);
      TsFileSequenceReader reader = new TsFileSequenceReader(path);
      MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(reader);
      CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(reader);
      IExpression timeFilter = new GlobalTimeExpression(TimeFilterApi.eq(time));
      QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
      BatchData queryDataSet =
          new CompressedTsFileExecutor(metadataQuerier, chunkLoader)
              .compressedExecute(queryExpression);
      if (queryDataSet == null) {
        System.out.println("没有找到数据");
        break;
      }
      boolean isFound = false;
      for (int i = 0; i < queryDataSet.length(); i++) {
        String next = queryDataSet.getTimeByIndex(i) + " " + queryDataSet.getLongByIndex(i);
        // System.out.println(next);
        // LOGGER.info(next);
        isFound = true;
      }
      if (!isFound) {
        break;
      }
      LOGGER.info("----------------");
    }
    long endTime = System.nanoTime();
    long duration = endTime - startTime;
    System.out.println("压缩查询时间: " + duration + " 毫秒");
    return duration;
  }

  public static void main(String[] args) throws IOException {
    initializeList();
    List<List<Long>> res = new ArrayList<>();
    String basicPath = System.getProperty("user.dir");
    for (int i = 0; i < pathsList.size(); i++) {
      File file = new File("..\\..\\data_LZV\\"+pathsList.get(i));
      String parentPath = file.getParent(); // 得到 "/home/user/data/files"
      int lastFolderIndex = parentPath.lastIndexOf(File.separator) + 1;
      System.out.println("Last folder name: " + parentPath.substring(lastFolderIndex));
      String path =basicPath+
          "\\tsfile_data\\tsfile_data_lz77\\"
              + pathsList.get(i).substring(0, pathsList.get(i).length() - 4)
              + ".tsfile";

      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(DEVICE_1, SENSOR_1, true));

      List<Long> times = readColumnByIndex("..\\..\\data_LZV\\"+pathsList.get(i), 0);
      List<Long> rowTime = new ArrayList<>();
      System.out.println(times.size());
      rowTime.add(Query(times, paths, path));
      rowTime.add(CompressedQuery(times, paths, path));
      rowTime.add((long) times.size());
      res.add(rowTime);
      writeLongCSV(res, true);
    }
    writeLongCSV(res, true);
  }

  public static List<Long> readColumnByIndex(String filePath, int columnIndex) throws IOException {
    List<Long> columnData = new ArrayList<>();

    try (BufferedReader br =
        Files.newBufferedReader(Paths.get(filePath), Charset.forName("ISO-8859-1"))) {
      // 跳过第一行标题
      br.readLine();
      long dataTimeNow = 0;
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        if (tokens.length > columnIndex) {
          try {
            double value = Double.parseDouble(tokens[columnIndex]);
            long value2 = (long) value;
            if (value2 > dataTimeNow) {
              dataTimeNow = value2;
              columnData.add(value2);
            }
          } catch (NumberFormatException e) {
            throw new IOException("解析错误 [" + tokens[columnIndex] + "] in line: " + line);
          }
        }
      }
    }
    return columnData;
  }

  public static void writeLongCSV(List<List<Long>> data, boolean withBom) throws IOException {
    String filename = System.getProperty("user.dir") + "\\tsfile_data\\query_time_tsfile_lzvlh.csv"; // 输出文件路径

    try (OutputStreamWriter writer =
            new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(writer)) {
      if (withBom) {
        bw.write('\uFEFF');
      }  

      for (List<Long> row : data) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
          if (i > 0) line.append(",");
          Long value = row.get(i);
          line.append(value != null ? value.toString() : "");
        }
        bw.write(line.toString());
        bw.newLine();
      }
    }
  }

  public static void initializeList() {
    String content = null;
    try {
      content =
          new String(
              Files.readAllBytes(
                  Paths.get(
                      "..\\..\\data_LZV\\config.json")));
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
        pathsList.add(path);
      }
    }
  }
}
