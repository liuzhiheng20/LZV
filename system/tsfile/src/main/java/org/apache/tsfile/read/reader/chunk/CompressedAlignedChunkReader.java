package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.query.executor.LZ4CompressedQuerier;
import org.apache.tsfile.read.query.executor.utils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompressedAlignedChunkReader extends AbstractChunkReader {
  // 用来实现对于数据的压缩查询
  // 考虑对齐时间序列，一个时间列，一个值列的简单情况
  private final ChunkHeader timeChunkHeader;
  // chunk data of the time column
  private final ByteBuffer timeChunkDataBuffer;

  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();
  // chunk data of all the sub sensors
  private final List<ByteBuffer> valueChunkDataBufferList = new ArrayList<>();
  // deleted intervals of all the sub sensors
  private final List<List<TimeRange>> valueDeleteIntervalsList = new ArrayList<>();
  // deleted intervals of time column
  protected final List<TimeRange> timeDeleteIntervalList;
  private final List<BatchData> pageResultList = new ArrayList<>();

  private final EncryptParameter encryptParam;

  public CompressedAlignedChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, long readStopTime, Filter queryFilter)
      throws IOException {
    super(readStopTime, queryFilter);
    this.timeChunkHeader = timeChunk.getHeader();
    this.timeChunkDataBuffer = timeChunk.getData();
    this.timeDeleteIntervalList = timeChunk.getDeleteIntervalList();

    List<Statistics<? extends Serializable>> valueChunkStatisticsList = new ArrayList<>();
    valueChunkList.forEach(
        chunk -> {
          this.valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          this.valueChunkDataBufferList.add(chunk == null ? null : chunk.getData());
          this.valueDeleteIntervalsList.add(chunk == null ? null : chunk.getDeleteIntervalList());

          valueChunkStatisticsList.add(chunk == null ? null : chunk.getChunkStatistic());
        });
    this.encryptParam = timeChunk.getEncryptParam();
    initAllPageReaders(timeChunk.getChunkStatistic(), valueChunkStatisticsList);
  }

  /** construct all the page readers in this chunk */
  private void initAllPageReaders(
      Statistics<? extends Serializable> timeChunkStatistics,
      List<Statistics<? extends Serializable>> valueChunkStatisticsList)
      throws IOException {
    if (isSinglePageChunk()) {
      BatchData batchData =
          deserializeFromSinglePageChunk(timeChunkStatistics, valueChunkStatisticsList);
      if (batchData != null) {
        pageResultList.add(batchData);
      }
    } else {
      // construct next satisfied page header
      while (timeChunkDataBuffer.remaining() > 0) {
        // deserialize PageHeader from chunkDataBuffer
        BatchData batchData = deserializeFromMultiPageChunk(); // 默认时间列数据块是多页数据块
        if (batchData != null) {
          pageResultList.add(batchData);
        }
      }
    }
  }

  private boolean isSinglePageChunk() {
    return (timeChunkHeader.getChunkType() & 0x3F) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
  }

  public BatchData getPageResult() {
    if (pageResultList.isEmpty()) {
      return null;
    }
    BatchData batchData = new BatchData(TSDataType.INT64);
    for (BatchData batchData1 : pageResultList) {
      for (int i = 0; i < batchData1.length(); i++) {
        batchData.putLong(batchData1.getTimeByIndex(i), batchData1.getLongByIndex(i));
      }
    }
    return batchData;
  }

  private BatchData deserializeFromMultiPageChunk() throws IOException {
    PageHeader timePageHeader =
        PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkHeader.getDataType());
    List<PageHeader> valuePageHeaderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
      if (valueChunkDataBufferList.get(i) != null) {
        isAllNull = false;
        valuePageHeaderList.add(
            PageHeader.deserializeFrom(
                valueChunkDataBufferList.get(i),
                valueChunkHeaderList.get(i).getDataType())); // 在这里增加了valueBuffer的position
      } else {
        valuePageHeaderList.add(null);
      }
    }

    if (needSkipForMultiPageChunk(isAllNull, timePageHeader)) {
      skipCurrentPage(timePageHeader, valuePageHeaderList);
      return null;
    }
    // return constructAlignedBatchData(timePageHeader, valuePageHeaderList);
    return constructCompressedAlignedBatchData(timePageHeader, valuePageHeaderList);
  }

  private BatchData deserializeFromSinglePageChunk(
      Statistics<? extends Serializable> timeChunkStatistics,
      List<Statistics<? extends Serializable>> valueChunkStatisticsList)
      throws IOException {
    PageHeader timePageHeader =
        PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkStatistics);
    List<PageHeader> valuePageHeaderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
      if (valueChunkDataBufferList.get(i) != null) {
        isAllNull = false;
        valuePageHeaderList.add(
            PageHeader.deserializeFrom(
                valueChunkDataBufferList.get(i), valueChunkStatisticsList.get(i)));
      } else {
        valuePageHeaderList.add(null);
      }
    }

    if (needSkipForMultiPageChunk(isAllNull, timePageHeader)) {
      skipCurrentPage(timePageHeader, valuePageHeaderList);
      return null;
    }
    // return constructAlignedBatchData(timePageHeader, valuePageHeaderList);
    return constructCompressedAlignedBatchData(timePageHeader, valuePageHeaderList);
  }

  boolean needSkipForMultiPageChunk(boolean isAllNull, PageHeader timePageHeader) {
    return isAllNull || isEarlierThanReadStopTime(timePageHeader) || pageCanSkip(timePageHeader);
  }

  boolean canSkip(boolean isAllNull, PageHeader timePageHeader) {
    return isAllNull;
  }

  protected boolean pageCanSkip(PageHeader pageHeader) {
    return queryFilter != null
        && !queryFilter.satisfyStartEndTime(pageHeader.getStartTime(), pageHeader.getEndTime());
  }

  protected boolean isEarlierThanReadStopTime(final PageHeader timePageHeader) {
    return timePageHeader.getEndTime() < readStopTime;
  }

  private void skipCurrentPage(PageHeader timePageHeader, List<PageHeader> valuePageHeader) {
    timeChunkDataBuffer.position(
        timeChunkDataBuffer.position() + timePageHeader.getCompressedSize());
    for (int i = 0; i < valuePageHeader.size(); i++) {
      if (valuePageHeader.get(i) != null) {
        valueChunkDataBufferList
            .get(i)
            .position(
                valueChunkDataBufferList.get(i).position()
                    + valuePageHeader.get(i).getCompressedSize());
      }
    }
  }

  private BatchData constructAlignedBatchData(
      PageHeader timePageHeader, List<PageHeader> rawValuePageHeaderList) throws IOException {
    // 对比上面的函数，将压缩查询后得到的数据封装为BatchData返回
    // 先按照现有的逻辑跑通
    // todo：压缩查询的实现位置
    IDecryptor decrytor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer timePageData =
        ChunkReader.deserializePageData(
            timePageHeader, timeChunkDataBuffer, timeChunkHeader, decrytor); // todo：不能在这里将数据解压
    Decoder valueDecoder =
        Decoder.getDecoderByType(timeChunkHeader.getEncodingType(), timeChunkHeader.getDataType());
    Decoder timeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    int index = 0;
    List<Integer> indexList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    List<Long> valueList = new ArrayList<>();
    while (timeDecoder.hasNext(timePageData)) {
      long time = timeDecoder.readLong(timePageData);
      if (queryFilter.satisfyLong(time, 0)) {
        indexList.add(index);
        timeList.add(time);
      }
      index++;
    }
    index = 0;
    PageHeader valuePageHeader = rawValuePageHeaderList.get(0);
    ByteBuffer pageData =
        ChunkReader.deserializePageData(
            valuePageHeader,
            valueChunkDataBufferList.get(0),
            valueChunkHeaderList.get(0),
            decrytor);
    splitDataToBitmapAndValue(pageData);
    ByteBuffer valuePageData = pageData;

    while (valueDecoder.hasNext(valuePageData)) {
      long value = valueDecoder.readLong(valuePageData);
      if (indexList.contains(index)) {
        valueList.add(value);
      }
      index++;
    }
    BatchData batchData = new BatchData(TSDataType.INT64);
    for (int i = 0; i < indexList.size(); i++) {
      batchData.putLong(timeList.get(i), valueList.get(i));
    }
    return batchData;
  }

  private BatchData constructCompressedAlignedBatchData(
      PageHeader timePageHeader, List<PageHeader> rawValuePageHeaderList) throws IOException {
    // 对比上面的函数，将压缩查询后得到的数据封装为BatchData返回
    // 使用压缩查询的方案
    // todo：压缩查询的实现位置
    IDecryptor decrytor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer timePageData =
        ChunkReader.readCompressedPageData(timePageHeader, timeChunkDataBuffer);
    //        ByteBuffer timePageData =
    //                ChunkReader.deserializePageData(
    //                        timePageHeader, timeChunkDataBuffer, timeChunkHeader, decrytor);    //
    // todo：不能在这里将数据解压
    //        Decoder timeDecoder =
    //                Decoder.getDecoderByType(
    //                        TSEncoding.valueOf(
    //                                TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
    //                        TSDataType.INT64);
    int index = 0;
    List<Integer> indexList = new ArrayList<>();
    List<Long> timeList = new ArrayList<>();
    List<Long> valueList = new ArrayList<>();
    //        while(timeDecoder.hasNext(timePageData)) {
    //            long time = timeDecoder.readLong(timePageData);
    //            if (queryFilter.satisfyLong(time, 0)) {
    //                indexList.add(index);
    //                timeList.add(time);
    //            }
    //            index++;
    //        }
    byte[] timeBytes = timePageData.array();
    LZ4CompressedQuerier timeCompressedQuerier = new LZ4CompressedQuerier(timeBytes);
    long time = queryFilter.getTimeRanges().get(0).getMax();
    int getindex;
    if (time == timePageHeader.getStartTime()) {
      getindex = 0;
    } else if (time == timePageHeader.getEndTime()) {
      getindex = timePageHeader.getUncompressedSize() / 8 - 1;
    } else {
      getindex =
          timeCompressedQuerier.compressedPredictBinaryDeltaSearch(
              time,
              timePageHeader.getUncompressedSize() / 8,
              timePageHeader.getStartTime(),
              timePageHeader.getEndTime());
    }
    if (getindex == -1) {
      System.out.println("没有找到数据");
      indexList.add(0);
      timeList.add(time);
    } else {
      indexList.add(getindex);
      timeList.add(time);
    }
    // 数值列的查询
    // valueList.add(0L);
    PageHeader valuePageHeader = rawValuePageHeaderList.get(0);
    ByteBuffer pageData =
        ChunkReader.readCompressedPageData(valuePageHeader, valueChunkDataBufferList.get(0));
    byte[] compressedData = pageData.array();
    int jumpSize = BitmapLength(compressedData);
    LZ4CompressedQuerier compressedQuerier = new LZ4CompressedQuerier(compressedData);

    for (int index1 : indexList) {
      byte[] longBytes =
          compressedQuerier.firstQuery(jumpSize + 8 * index1, jumpSize + 8 * (index1 + 1));
      long value = utils.bytesToLong(longBytes);
      valueList.add(value);
    }

    BatchData batchData = new BatchData(TSDataType.INT64);
    for (int i = 0; i < indexList.size(); i++) {
      batchData.putLong(timeList.get(i), valueList.get(i));
    }
    return batchData;
  }

  private ByteBuffer splitDataToBitmapAndValue(ByteBuffer pageData) {
    if (!pageData.hasRemaining()) { // Empty Page
      return null;
    }
    int size = ReadWriteIOUtils.readInt(pageData);
    byte[] bitmap = new byte[(size + 7) / 8];
    pageData.get(bitmap);
    return pageData.slice();
  }

  private int BitmapLength(byte[] pageData) {
    if (pageData.length == 0) { // Empty Page
      return 0;
    }
    int size =
        ((pageData[1] & 0xFF) << 24)
            | ((pageData[2] & 0xFF) << 16)
            | ((pageData[3] & 0xFF) << 8)
            | ((pageData[4] & 0xFF)); // todo：不能直接读，但可以在第一个literal的位置直接读四个数
    size = 4 + (size + 7) / 8;
    return size;
  }
}
