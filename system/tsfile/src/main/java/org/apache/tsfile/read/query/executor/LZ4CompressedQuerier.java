package org.apache.tsfile.read.query.executor;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;

public class LZ4CompressedQuerier {
  private int queryLeft = 0;
  private int queryRight = 0;
  private int findIterate = 0;
  private int findCost = 0;
  private int findSpan = 0;
  private byte[] compressData = null;
  private ByteArrayOutputStream res = null;
  public IntBuffer indexBuffer = null;
  public IntBuffer infoBuffer = null;
  public int[] index;
  public int[] info;
  int timeColumnFindCost = 0;

  public LZ4CompressedQuerier(byte[] compressData) {
    this.compressData = compressData;
    this.indexBuffer = IntBuffer.allocate(Math.min(1000000, compressData.length));
    this.infoBuffer = IntBuffer.allocate(Math.min(1000000, compressData.length));
    this.res = new ByteArrayOutputStream();
    indexCompressedData();
  }

  public void indexCompressedData() {
    int token;
    int literalLen = 0;
    int matchLen = 0;
    int i = 0;
    int len = 0;
    indexBuffer.put(len);
    while (i < compressData.length) {
      token = compressData[i] & 0xFF;
      literalLen = token >>> 4;
      matchLen = (token & 0x0F) + 4; // MIN_MATCH = 4
      i += 1;

      if (literalLen == 0x0F) {
        int add;
        while ((add = compressData[i] & 0xFF) == 0xFF) {
          literalLen += 0xFF;
          i += 1;
        }
        i += 1;
        literalLen += add;
      }
      infoBuffer.put(i); // 对于literal部分，记录literral在压缩序列中的位置
      len += literalLen;
      i += literalLen;
      indexBuffer.put(len);
      if (i >= compressData.length - 1) break;
      // 读取offset
      int offset = (compressData[i] & 0xFF) | ((compressData[i + 1] & 0xFF) << 8);
      i += 2;
      infoBuffer.put(offset); // 对于match部分，记录offset的长度
      if (matchLen == 0x0F + 4) {
        int add;
        while ((add = compressData[i] & 0xFF) == 0xFF) {
          matchLen += 0xFF;
          i += 1;
        }
        i += 1;
        matchLen += add;
      }
      len += matchLen;
      indexBuffer.put(len);
    }
    if (indexBuffer.position() == indexBuffer.capacity()) {
      index = indexBuffer.array();
    } else {
      index = new int[indexBuffer.position()];
      indexBuffer.rewind();
      indexBuffer.get(index);
    }
    if (infoBuffer.position() == infoBuffer.capacity()) {
      info = infoBuffer.array();
    } else {
      info = new int[infoBuffer.position()];
      infoBuffer.rewind();
      infoBuffer.get(info);
    }
  }

  // 二分查找
  private int findLeIndex(int target) {
    int idx = Arrays.binarySearch(index, target);
    if (idx < 0) {
      idx = -idx - 2;
    }
    if (idx >= 0 && idx + 1 < index.length) {
      int temp_1 = index[idx];
      int temp_2 = index[idx + 1];
      if (temp_1 == temp_2) {
        idx = idx + 1;
      }
    }
    // System.out.println(target-index[idx]);
    return idx < 0 ? -1 : idx;
  }

  // 查询的核心类
  public void query(int left, int right) {
    if (right <= left) return;
    if (left < this.queryLeft) this.queryLeft = left;
    this.findIterate++;

    int nowpos = left;
    int leftIndex = findLeIndex(left);
    int rightIndex = findLeIndex(right - 1);
    this.findCost += rightIndex - leftIndex + 1;

    for (int i = leftIndex; i <= rightIndex; i++) {
      // literal/ match /literal/match/...(/literal)交替分布
      int temp = info[i];
      if (i % 2 == 0) { // literal
        @SuppressWarnings("unchecked")
        int start = Math.max(nowpos - index[i], 0);
        int end = Math.min(right - index[i], index[i + 1] - index[i]);
        for (int j = start; j < end; j++) {
          res.write(compressData[j + temp]);
        }
        nowpos = index[i] + end;
      } else { // match
        // 对match这里进一步优化，在一个match块中只进行一次的跳转
        int leftSide = index[i];
        int tranformNum = (nowpos - leftSide) / temp + 1; // 取整关系是什么样的
        int jumpDis = temp * tranformNum;
        int matchStart = Math.max(nowpos - jumpDis, 0);
        int matchEnd = Math.min(right - jumpDis, index[i + 1] - jumpDis);
        // 递归处理匹配区域
        query(matchStart, matchEnd);
        nowpos = index[i] + (matchEnd - matchStart);
      }
    }
  }

  // 首次查询入口
  public byte[] firstQuery(int left, int right) {
    resetState();
    this.queryLeft = left;
    this.queryRight = right;
    query(left, right);
    return res.toByteArray();
  }

  // 查询int32值
  public int queryInt32(int pos) {
    byte[] bytes = firstQuery(4 * pos, 4 * (pos + 1));
    this.findSpan = this.queryRight - this.queryLeft;
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
  }

  public int getFindCost() {
    return this.findIterate;
  }

  public void resetState() {
    this.queryLeft = 0;
    this.queryRight = 0;
    this.findIterate = 0;
    this.findCost = 0;
    this.res.reset();
  }

  public int compressedBinarySearch(int target, int right) { // totalNum表示原数据由多少个long类型的值组成
    int left = 0;
    right = right - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      query(4 * mid, 4 * mid + 4);
      int temp = utils.bytesToInt(res.toByteArray());
      resetState();
      if (temp == target) {
        return mid;
      } else if (temp < target) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    return -1; // 没找到返回 -1
  }

  public int compressedPredictSearch(int target, int right) {
    int opeNum = 0; // Operation counter
    int left = 0;
    right = right - 1;
    query(4 * right, 4 * right + 4);
    int maxData = utils.bytesToInt(res.toByteArray());
    resetState();
    query(4 * left, 4 * left + 4);
    int minData = utils.bytesToInt(res.toByteArray());
    while (left <= right) {
      opeNum++;
      // Avoid division by zero
      if (maxData == minData) {
        if (minData == target) {
          // System.out.println("Operations: " + opeNum);
          return left;
        } else {
          break;
        }
      }
      // Predict the index
      int predictIndex =
          left + (int) ((double) (target - minData) / (maxData - minData) * (right - left));
      if (predictIndex == left) predictIndex++;
      if (predictIndex == right) predictIndex--;
      if (predictIndex <= left || predictIndex >= right) {
        break; // Out of bounds
      }
      resetState();
      query(4 * predictIndex, 4 * predictIndex + 4);
      int predictData = utils.bytesToInt(res.toByteArray());
      // System.out.println("PredictIndex: " + predictIndex);

      if (predictData == target) {
        return predictIndex;
      } else if (predictData < target) {
        left = predictIndex;
        minData = predictData;
      } else {
        right = predictIndex;
        maxData = predictData;
      }
    }

    // System.out.println("Operations: " + opeNum);
    return -1; // Not found
  }

  public int compressedPredictBinaryDeltaSearch(
      long target, int right, long minData, long maxData) {
    // 奇数次用预测查询，偶数次用二分查询
    int opeNum = 0; // Operation counter
    int left = 0;
    right = right - 1;
    int length = right;
    long min = minData;
    long max = maxData;
    resetState();
    boolean isPrediction = true;
    // timeColumnFindCost = 0;
    while (left <= right) {
      opeNum++;
      // Avoid division by zero
      if (maxData == minData) {
        if (minData == target) {
          // System.out.println("Operations: " + opeNum);
          return left;
        } else {
          break;
        }
      }
      // Predict the index
      int predictIndex = 0;
      if (isPrediction) {
        predictIndex =
            left + (int) ((double) (target - minData) / (maxData - minData) * (right - left));
        isPrediction = false;
      } else {
        predictIndex = (left + right) / 2;
        isPrediction = true;
      }
      if (predictIndex == left) predictIndex++;
      if (predictIndex == right) predictIndex--;
      if (predictIndex <= left || predictIndex >= right) {
        break; // Out of bounds
      }
      resetState();
      query(8 * predictIndex, 8 * predictIndex + 8);
      timeColumnFindCost++;
      long delta = utils.bytesToLong(res.toByteArray());
      double temp = ((double) (max - min) * predictIndex) / length;
      long predictData = roundHalfToEven(temp) + min + delta;
      // System.out.println("PredictIndex: " + predictIndex);

      if (predictData == target) {
        // System.out.println("Operations: " + timeColumnFindCost);
        return predictIndex;
      } else if (predictData < target) {
        left = predictIndex;
        minData = predictData;
      } else {
        right = predictIndex;
        maxData = predictData;
      }
    }

    System.out.println("Operations: " + timeColumnFindCost);
    return -1; // Not found
  }

  public static long roundHalfToEven(double x) {
    long floor = (long) Math.floor(x);
    double fraction = x - floor;

    if (fraction > 0.5) {
      return floor + 1;
    } else if (fraction < 0.5) {
      return floor;
    } else {
      // fraction == 0.5
      return (floor % 2 == 0) ? floor : floor + 1;
    }
  }
}
