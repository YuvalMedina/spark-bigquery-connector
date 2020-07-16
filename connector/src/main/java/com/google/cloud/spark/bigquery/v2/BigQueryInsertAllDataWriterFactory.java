package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class BigQueryInsertAllDataWriterFactory implements DataWriterFactory<InternalRow> {


    private final BigQueryFactory bigQueryFactory;
    private final TableId tableId;
    private final StructType sparkSchema;
    private final boolean ignoreInputs;

    public BigQueryInsertAllDataWriterFactory(BigQueryFactory bigQueryFactory, TableId tableId,
                                              StructType sparkSchema, boolean ignoreInputs) {
        this.bigQueryFactory = bigQueryFactory;
        this.tableId = tableId;
        this.sparkSchema = sparkSchema;
        this.ignoreInputs = ignoreInputs;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new BigQueryInsertAllDataWriter(bigQueryFactory, tableId, sparkSchema, ignoreInputs, partitionId,
                taskId, epochId);
    }
}
