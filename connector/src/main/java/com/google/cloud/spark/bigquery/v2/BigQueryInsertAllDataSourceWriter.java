package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryFactory;
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
import java.util.Arrays;

import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryInsertAllDataSourceWriter implements DataSourceWriter {
    final Logger logger = LoggerFactory.getLogger(BigQueryInsertAllDataSourceWriter.class);

    private final BigQueryFactory bigQueryFactory;
    private final BigQueryClient bigQueryClient;
    private final BigQuery bigQuery;

    private final TableId destinationTableId;
    private final StructType sparkSchema;
    private final Schema bigQuerySchema;
    private final String writeUUID;

    private Table temporaryTable;
    private TableId temporaryTableId;

    private boolean ignoreInputs = false;
    private boolean overwrite = false;

    public BigQueryInsertAllDataSourceWriter(BigQueryClient bigQueryClient, BigQueryFactory bigQueryFactory,
                                             TableId destinationTableId, String writeUUID, SaveMode saveMode, StructType sparkSchema) {
        this.bigQueryClient = bigQueryClient;
        this.bigQueryFactory = bigQueryFactory;
        this.bigQuery = bigQueryFactory.createBigQuery();

        this.destinationTableId = destinationTableId;
        this.temporaryTableId = destinationTableId;
        this.writeUUID = writeUUID;
        this.sparkSchema = sparkSchema;
        this.bigQuerySchema = toBigQuerySchema(sparkSchema);

        this.temporaryTable = getOrCreateTable(saveMode);
    }

    private Table getOrCreateTable(SaveMode saveMode) {
        if(bigQueryClient.tableExists(destinationTableId)) {
            switch (saveMode) {
                case Append:
                    break;
                case Overwrite:
                    overwrite = true;
                    this.temporaryTableId = TableId.of(destinationTableId.getDataset(), destinationTableId.getTable()+"tmp123456789"); // TODO: create some sort of temporary tableID.
                    return bigQueryClient.createTable(temporaryTableId, bigQuerySchema);
                case Ignore:
                    ignoreInputs = true;
                    break;
                case ErrorIfExists:
                    throw new RuntimeException("Table already exists in BigQuery."); // TODO: should this be a RuntimeException?
            }
            return (Table)bigQueryClient.getTable(temporaryTableId);
        }
        else {
            return bigQueryClient.createTable(temporaryTableId, bigQuerySchema);
        }
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new BigQueryInsertAllDataWriterFactory(bigQueryFactory, temporaryTableId, sparkSchema, ignoreInputs);
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {

    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        logger.info("BigQuery DataSource writer {} committed with messages:\n{}", writeUUID, Arrays.toString(messages));

        if (!ignoreInputs) {
            long totalRowCount = 0;
            for(WriterCommitMessage message : messages) {
                totalRowCount += ((BigQueryInsertAllWriterCommitMessage)message).getRowCount();
            }

            if (overwrite) {
                Preconditions.checkArgument(bigQueryClient.deleteTable(destinationTableId),
                        new IOException("Could not delete table to be overwritten."));
            }

            Job rename = bigQuery.getTable(temporaryTableId).copy(destinationTableId);
            try {
                Job completedJob =
                        rename.waitFor(
                                RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                                RetryOption.totalTimeout(Duration.ofMinutes(3)));
                if (completedJob == null && completedJob.getStatus().getError() != null) {
                    throw new IOException(completedJob.getStatus().getError().toString());
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Could not copy table from temporary sink to destination table.", e);
            }

            logger.info("BigQuery DataSource committed with row count: {}", totalRowCount);
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        logger.warn("BigQuery Data Source writer {} aborted.", writeUUID);
        bigQueryClient.deleteTable(temporaryTableId);
    }

    @Override
    public boolean useCommitCoordinator() {
        return false;
    }
}
