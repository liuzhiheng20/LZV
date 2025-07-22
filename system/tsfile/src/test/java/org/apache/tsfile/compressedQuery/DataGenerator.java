package org.apache.tsfile.compressedQuery;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import java.time.LocalDate;

public class DataGenerator {
  public static Object generate(TSDataType type, int index) {
    switch (type) {
      case INT32:
        return index;
      case INT64:
      case TIMESTAMP:
        return (long) index;
      case FLOAT:
        return (float) index;
      case DOUBLE:
        return (double) index;
      case BOOLEAN:
        return index % 2 == 0;
      case DATE:
        return LocalDate.of(2024, 1, index % 30 + 1);
      case TEXT:
      case STRING:
      case BLOB:
        return new Binary(String.valueOf(index), TSFileConfig.STRING_CHARSET);
      default:
        return null;
    }
  }
}
