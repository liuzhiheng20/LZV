package org.apache.tsfile.read.query.executor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.chunk.CompressedAlignedChunkReader;
import org.apache.tsfile.utils.BloomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompressedTsFileExecutor implements QueryExecutor {
  // 实现从压缩的tsfile中按照filter的过滤条件读取内容
  // 参照TsFileExcutor的
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;

  public CompressedTsFileExecutor(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  public BatchData compressedExecute(QueryExpression queryExpression) throws IOException {
    // bloom filter
    BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
    List<Path> filteredSeriesPath = new ArrayList<>();
    if (bloomFilter != null) {
      for (Path path : queryExpression.getSelectedSeries()) {
        if (bloomFilter.contains(path.getFullPath())) {
          filteredSeriesPath.add(path);
        }
      }
      queryExpression.setSelectSeries(filteredSeriesPath);
    }

    metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression expression = queryExpression.getExpression();
        IExpression regularIExpression =
            ExpressionOptimizer.getInstance()
                .optimize(expression, queryExpression.getSelectedSeries());
        queryExpression.setExpression(regularIExpression);

        if (regularIExpression instanceof GlobalTimeExpression) {
          return executeMayAttachTimeFiler(
              queryExpression.getSelectedSeries(), (GlobalTimeExpression) regularIExpression);
        } else {
          return null;
        }

      } catch (QueryFilterOptimizationException e) {
        throw new IOException(e);
      } catch (NoMeasurementException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private BatchData executeMayAttachTimeFiler(
      List<Path> selectedPathList, GlobalTimeExpression timeExpression)
      throws IOException, NoMeasurementException {
    List<CompressedAlignedChunkReader> readersOfSelectedCompressedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : selectedPathList) {
      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(path);
      CompressedAlignedChunkReader compressedAlignedChunkReader = null;
      IChunkMetadata iChunkMetadata = chunkMetadataList.get(0);
      compressedAlignedChunkReader = initChunkReader(iChunkMetadata, timeExpression.getFilter());
      readersOfSelectedCompressedSeries.add(compressedAlignedChunkReader);
    }
    return readersOfSelectedCompressedSeries.get(0).getPageResult();
  }

  protected CompressedAlignedChunkReader initChunkReader(
      IChunkMetadata chunkMetaData, Filter filter) throws IOException {
    AbstractAlignedChunkMetadata alignedChunkMetadata =
        (AbstractAlignedChunkMetadata) chunkMetaData;
    Chunk timeChunk =
        chunkLoader.loadChunk((ChunkMetadata) (alignedChunkMetadata.getTimeChunkMetadata()));
    List<Chunk> valueChunkList = new ArrayList<>();
    for (IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (metadata != null) {
        valueChunkList.add(chunkLoader.loadChunk((ChunkMetadata) metadata));
        continue;
      }
      valueChunkList.add(null);
    }
    CompressedAlignedChunkReader chunkReader =
        new CompressedAlignedChunkReader(timeChunk, valueChunkList, 1000, filter);
    return chunkReader;
  }

  @Override
  public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
    return null;
  }
}
