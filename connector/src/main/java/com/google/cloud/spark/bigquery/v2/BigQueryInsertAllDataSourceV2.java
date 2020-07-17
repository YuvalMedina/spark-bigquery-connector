package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BigQueryInsertAllDataSourceV2 implements DataSourceV2, WriteSupport {
    final Logger logger = LoggerFactory.getLogger(BigQueryInsertAllDataSourceV2.class);

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        logger.trace("createWriter({}, {}, {}, {})", writeUUID, schema, mode, options);

        SparkSession spark = getDefaultSparkSessionOrCreate();

        Injector injector =
                Guice.createInjector(
                        new BigQueryClientModule(),
                        new BigQueryWriteClientModule(writeUUID, mode, schema),
                        new SparkBigQueryConnectorModule(spark, options, Optional.of(schema)));

        BigQueryInsertAllDataSourceWriter writer = injector.getInstance(BigQueryInsertAllDataSourceWriter.class);
        return Optional.of(writer);
    }

    private SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSpareSession = SparkSession.getDefaultSession();
        if (defaultSpareSession.isDefined()) {
            return defaultSpareSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
    }
}
