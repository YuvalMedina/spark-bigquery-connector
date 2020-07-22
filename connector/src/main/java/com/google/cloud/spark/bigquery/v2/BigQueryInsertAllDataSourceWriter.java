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
  private final String writeUUID;

  private Table writingTable;

  enum WriteMode {
    IGNORE_INPUTS,
    OVERWRITE,
    APPEND,
    DESTINATION_DID_NOT_EXIST;
  }

  private WriteMode writeMode = WriteMode.DESTINATION_DID_NOT_EXIST;

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

    Schema bigQuerySchema = toBigQuerySchema(sparkSchema);
    this.writingTable = getOrCreateTable(saveMode, destinationTableId, bigQuerySchema);
  }

  private Table getOrCreateTable(
      SaveMode saveMode, TableId destinationTableId, Schema bigQuerySchema) {
    if (bigQueryClient.tableExists(destinationTableId)) { // TODO schema validation.
      switch (saveMode) {
        case Ignore:
          this.writeMode = WriteMode.IGNORE_INPUTS;
          return null;
        case Append:
          this.writeMode = WriteMode.APPEND;
          break;
        case Overwrite:
          this.writeMode = WriteMode.OVERWRITE;
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
      // If the table exists, whether we append or overwrite, writing is done to a temporary table
      // first.
      return bigQueryClient.createTempTable(destinationTableId, bigQuerySchema);
    }
    writeMode = WriteMode.DESTINATION_DID_NOT_EXIST;
    return bigQueryClient.createTable(destinationTableId, bigQuerySchema);
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryInsertAllDataWriterFactory(
        bigQueryClientFactory,
        writingTable,
        sparkSchema,
        writeMode.equals(WriteMode.IGNORE_INPUTS));
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {}

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (writeMode.equals(WriteMode.IGNORE_INPUTS)) return;

    long totalRowCount = 0;
    for (WriterCommitMessage message : messages) {
      totalRowCount += ((BigQueryInsertAllWriterCommitMessage) message).getRowCount();
    }

    // If we made a temporary table because of an append / overwrite save-mode and a pre-existing
    // destination table:
    if (!writeMode.equals(WriteMode.DESTINATION_DID_NOT_EXIST)) {
      Job copyJob;
      if (writeMode.equals(WriteMode.APPEND)) {
        copyJob =
            bigQueryClient.appendFromTemporaryToDestination(
                writingTable.getTableId(), destinationTableId);
      } else {
        // overwrite:
        copyJob =
            bigQueryClient.overwriteDestinationWithTemporary(
                writingTable.getTableId(), destinationTableId);
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

      // delete our temporary writing table:
      bigQueryClient.deleteTable(writingTable.getTableId());
    }

    logger.info("BigQuery DataSource committed with row count: {}", totalRowCount);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
    if (writeMode.equals(WriteMode.IGNORE_INPUTS)) return;

    // this will delete either the temporary table created (in case of a preexisting destination
    // table) or the the
    // destination table itself, if it did not exist prior.
    bigQueryClient.deleteTable(writingTable.getTableId());
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }
}
