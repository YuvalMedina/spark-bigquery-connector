package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class BigQueryInsertAllDataWriterFactory implements DataWriterFactory<InternalRow> {

  private final BigQueryClientFactory bigQueryClientFactory;
  private final Table table;
  private final StructType sparkSchema;
  private final boolean ignoreInputs;

  public BigQueryInsertAllDataWriterFactory(
      BigQueryClientFactory bigQueryClientFactory,
      Table table,
      StructType sparkSchema,
      boolean ignoreInputs) {
    this.bigQueryClientFactory = bigQueryClientFactory;
    // TODO: can be null:
    this.table = table;
    this.sparkSchema = sparkSchema;
    this.ignoreInputs = ignoreInputs;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    if (ignoreInputs) {
      return new NoOpDataSourceWriter();
    }
    return new BigQueryInsertAllDataWriter(
        bigQueryClientFactory, table.getTableId(), sparkSchema, partitionId, taskId, epochId);
  }
}
