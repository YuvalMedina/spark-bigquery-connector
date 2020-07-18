package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;

import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryInsertAllDataSourceWriter implements DataSourceWriter {
  final Logger logger = LoggerFactory.getLogger(BigQueryInsertAllDataSourceWriter.class);

  private final BigQueryClientFactory bigQueryClientFactory;
  private final BigQueryClient bigQueryClient;

  private final TableId destinationTableId;
  private final StructType sparkSchema;
  private final Schema bigQuerySchema;
  private final String writeUUID;

  private Table temporaryTable;

  enum AppendOrOverwrite {
    DESTINATION_DOES_NOT_EXIST,
    OVERWRITE,
    APPEND;
  }

  private boolean ignoreInputs = false;
  private AppendOrOverwrite appendOrOverwrite = AppendOrOverwrite.DESTINATION_DOES_NOT_EXIST;

  public BigQueryInsertAllDataSourceWriter(
      BigQueryClientFactory bigQueryClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema) {
    this.bigQueryClient = bigQueryClientFactory.createBigQueryClient();
    this.bigQueryClientFactory = bigQueryClientFactory;

    this.destinationTableId = destinationTableId;
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.bigQuerySchema = toBigQuerySchema(sparkSchema);

    this.temporaryTable = getOrCreateTable(saveMode);
  }

  private Table getOrCreateTable(SaveMode saveMode) {
    if (bigQueryClient.tableExists(destinationTableId)) { // TODO schema validation.
      switch (saveMode) {
        case Ignore:
          ignoreInputs = true;
          return null;
        case Append:
          this.appendOrOverwrite = AppendOrOverwrite.APPEND;
          break;
        case Overwrite:
          this.appendOrOverwrite = AppendOrOverwrite.OVERWRITE;
          break;
        case ErrorIfExists:
          throw new RuntimeException(
              "Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
      }
      Preconditions.checkArgument(
          bigQueryClient
              .getTable(destinationTableId)
              .getDefinition()
              .getSchema()
              .equals(bigQuerySchema),
          new RuntimeException("Destination table's schema is not compatible."));
    } else {
      appendOrOverwrite = AppendOrOverwrite.DESTINATION_DOES_NOT_EXIST;
    }

    return bigQueryClient.createTempTable(destinationTableId, bigQuerySchema);
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryInsertAllDataWriterFactory(
        bigQueryClientFactory, temporaryTable, sparkSchema, ignoreInputs);
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {}

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (ignoreInputs) return;

    long totalRowCount = 0;
    for (WriterCommitMessage message : messages) {
      totalRowCount += ((BigQueryInsertAllWriterCommitMessage) message).getRowCount();
    }

    Job copyJob;
    switch (appendOrOverwrite) {
      case APPEND:
        copyJob =
            bigQueryClient.appendFromTemporaryToDestination(
                temporaryTable.getTableId(), destinationTableId);
        break;
      case OVERWRITE:
        copyJob =
            bigQueryClient.overwriteDestinationWithTemporary(
                temporaryTable.getTableId(), destinationTableId);
        break;
      default:
        copyJob =
            bigQueryClient.createDestinationFromTemporary(
                temporaryTable.getTableId(), destinationTableId, bigQuerySchema);
        break;
    }

    try {
      Job completedJob =
          copyJob.waitFor(
              RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob == null && completedJob.getStatus().getError() != null) {
        throw new IOException(completedJob.getStatus().getError().toString());
      }
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(
          "Could not copy table from temporary sink to destination table.", e);
    }

    deleteTemporaryTable();

    logger.info("BigQuery DataSource committed with row count: {}", totalRowCount);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
    if (ignoreInputs) return;

    if (appendOrOverwrite == AppendOrOverwrite.DESTINATION_DOES_NOT_EXIST) {
      deleteDestinationTable();
    }
    if (bigQueryClient.tableExists(temporaryTable.getTableId())) {
      deleteTemporaryTable();
    }
  }

  void deleteTemporaryTable() {
    Preconditions.checkArgument(
        bigQueryClient.deleteTable(temporaryTable.getTableId()),
        new RuntimeException(
            String.format("Could not delete temporary table %s.", temporaryTable.getTableId())));
  }

  void deleteDestinationTable() {
    Preconditions.checkArgument(
        bigQueryClient.deleteTable(destinationTableId),
        new RuntimeException(
            String.format("Could not delete destination table %s.", destinationTableId)));
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }
}
