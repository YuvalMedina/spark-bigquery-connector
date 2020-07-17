package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceWriter;
import com.google.inject.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryWriteClientModule implements Module {

    final String writeUUID;
    final SaveMode saveMode;
    final StructType sparkSchema;

    public BigQueryWriteClientModule(String writeUUID, SaveMode saveMode, StructType sparkSchema) {
        this.writeUUID = writeUUID;
        this.saveMode = saveMode;
        this.sparkSchema = sparkSchema;
    }

    @Override
    public void configure(Binder binder) {
        // BigQuery related
        binder.bind(BigQueryFactory.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public BigQueryInsertAllDataSourceWriter provideBigQueryInsertAllDataSourceWriter(
            BigQueryClient bigQueryClient,
            BigQueryFactory bigQueryFactory,
            SparkBigQueryConfig config) {
        TableId tableId = config.getTableId();
        return new BigQueryInsertAllDataSourceWriter(
                bigQueryClient,
                bigQueryFactory,
                tableId,
                writeUUID,
                saveMode,
                sparkSchema);
    }
}
