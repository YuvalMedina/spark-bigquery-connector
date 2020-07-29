package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ExponentialBackOffFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class BigQueryInsertAllDataWriterFactory implements DataWriterFactory<InternalRow> {

  private final BigQueryClientFactory bigQueryClientFactory;
  private final Table table;
  private final StructType sparkSchema;
  private final boolean ignoreInputs;
  private final int numberOfFirstRowsToEstimate;
  private final long maxWriteBatchSizeInBytes;
  private final int maxWriteBatchRowCount;
  private final ExponentialBackOffFactory exponentialBackOffFactory;

  public BigQueryInsertAllDataWriterFactory(
      BigQueryClientFactory bigQueryClientFactory,
      Table table,
      StructType sparkSchema,
      boolean ignoreInputs,
      int numberOfFirstRowsToEstimate,
      long maxWriteBatchSizeInBytes,
      int maxWriteBatchRowCount,
      ExponentialBackOffFactory exponentialBackOffFactory) {
    this.bigQueryClientFactory = bigQueryClientFactory;
    // TODO: can be null:
    this.table = table;
    this.sparkSchema = sparkSchema;
    this.ignoreInputs = ignoreInputs;
    this.numberOfFirstRowsToEstimate = numberOfFirstRowsToEstimate;
    this.maxWriteBatchSizeInBytes = maxWriteBatchSizeInBytes;
    this.maxWriteBatchRowCount = maxWriteBatchRowCount;
    this.exponentialBackOffFactory = exponentialBackOffFactory;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    if (ignoreInputs) {
      return new NoOpDataWriter();
    }
    return new BigQueryInsertAllDataWriter(
        bigQueryClientFactory,
        table.getTableId(),
        sparkSchema,
        partitionId,
        taskId,
        epochId,
        numberOfFirstRowsToEstimate,
        maxWriteBatchSizeInBytes,
        maxWriteBatchRowCount,
        exponentialBackOffFactory);
  }
}
