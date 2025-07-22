package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongPredictDecoder extends Decoder {
  long maxData;
  long minData;
  boolean isFirst = true;
  int nowIndex = 0;
  int length = 0;

  public LongPredictDecoder(TSEncoding type) {
    super(type);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (isFirst) {
      if (buffer.remaining() < 8) {
        return false;
      }
      minData = buffer.getLong();
      maxData = buffer.getLong(buffer.limit() - 8);
      nowIndex = 0;
      length = buffer.limit() / 8 - 1;
    }
    return buffer.remaining() >= 8;
  }

  @Override
  public void reset() {
    this.isFirst = true;
    this.maxData = 0;
    ;
    this.minData = 0;
    this.nowIndex = 0;
    this.length = 0;
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (isFirst) {
      isFirst = false;
      return minData;
    }
    if (buffer.remaining() == 8) {
      buffer.getLong();
      return maxData;
    } else {
      nowIndex++;
      long delta = buffer.getLong();
      double temp = ((double) (maxData - minData) * nowIndex) / length;
      return roundHalfToEven(temp) + minData + delta;
    }
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
