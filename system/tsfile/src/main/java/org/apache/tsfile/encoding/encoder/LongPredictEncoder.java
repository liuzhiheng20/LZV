package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class LongPredictEncoder extends Encoder {
  private static final Logger logger = LoggerFactory.getLogger(LongPredictEncoder.class);

  /** Bitmap Encoder stores all current values in a list temporally. */
  private List<Long> values;

  public LongPredictEncoder(TSEncoding type) {
    super(type);
    this.values = new ArrayList<Long>();
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    long minData = values.get(0);
    long maxData = values.get(values.size() - 1);
    out.write(longToBytes(minData));
    int length = values.size() - 1;
    for (int i = 1; i < values.size() - 1; i++) {
      long data = values.get(i);
      double temp = ((double) (maxData - minData) * i) / length;
      long predict = minData + roundHalfToEven(temp);
      out.write(longToBytes(data - predict));
    }
    out.write(longToBytes(maxData));
    this.values = new ArrayList<Long>();
  }

  public void encode(long value, ByteArrayOutputStream out) {
    values.add(value);
  }

  // 实现通过线性模型的编码
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

  public static byte[] longToBytes(long data) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(data);
    return buffer.array();
  }

  @Override
  public long getMaxByteSize() {
    return 8 * values.size() + 100;
  }
}
