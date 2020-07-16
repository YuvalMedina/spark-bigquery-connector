package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.inject.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryInsertAllDataSourceWriterModule implements Module {
  public BigQueryInsertAllDataSourceWriterModule(
      String writeUUID, SaveMode mode, StructType schema) {}

  @Override
  public void configure(Binder binder) {
    binder.bind(BigQueryInsertAllDataSourceWriter.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  public BigQueryInsertAllDataSourceWriter provideBigQueryInsertAllDataSourceWriter(

  ) {
    return new BigQueryInsertAllDataSourceWriter()
  }
}
