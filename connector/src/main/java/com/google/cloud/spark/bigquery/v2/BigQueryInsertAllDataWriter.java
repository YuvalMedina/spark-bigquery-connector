package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryFactory;
import com.google.cloud.spark.bigquery.SparkInsertAllBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BigQueryInsertAllDataWriter implements DataWriter<InternalRow> {

    final Logger logger = LoggerFactory.getLogger(BigQueryInsertAllDataWriter.class);

    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final BigQuery bigQuery;
    private final TableId tableId;
    private final StructType sparkSchema;
    private final boolean ignoreInputs;

    private SparkInsertAllBuilder sparkInsertAllBuilder;

    public BigQueryInsertAllDataWriter(BigQueryFactory bigQueryFactory, TableId tableId,
                                       StructType sparkSchema, boolean ignoreInputs, int partitionId, long taskId,
                                       long epochId) {
        this.bigQuery = bigQueryFactory.createBigQuery();
        this.tableId = tableId;
        this.sparkSchema = sparkSchema;
        this.ignoreInputs = ignoreInputs;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;

        if(ignoreInputs) return;

        this.sparkInsertAllBuilder = new SparkInsertAllBuilder(sparkSchema, tableId, bigQuery);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        if(ignoreInputs) return;

        //if(partitionId == 3) abort(); FIXME: for debugging purposes.

        sparkInsertAllBuilder.addRow(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        logger.debug("Data Writer {} commit()", partitionId);

        Long rowCount = null;

        if (!ignoreInputs) {
            sparkInsertAllBuilder.commit();

            rowCount = sparkInsertAllBuilder.getCommittedRows();

            logger.debug("Data Writer {}'s write-stream has finalized with row count: {}", partitionId, rowCount);
        }

        return new BigQueryInsertAllWriterCommitMessage(partitionId, taskId, epochId, rowCount);
    }

    @Override
    public void abort() throws IOException {
        logger.debug("Data Writer {} abort()", partitionId);
        sparkInsertAllBuilder.abort();
    }
}
