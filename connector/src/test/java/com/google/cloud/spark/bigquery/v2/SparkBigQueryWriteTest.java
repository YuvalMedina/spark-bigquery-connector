package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.fail;

public class SparkBigQueryWriteTest {

    // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    private final static int BQ_NUMERIC_PRECISION = 38;
    private final static int BQ_NUMERIC_SCALE = 9;

    public static final Logger logger = LogManager.getLogger("com.google.cloud");

    public static final String PROJECT = ServiceOptions.getDefaultProjectId();
    public static final String DATASET = "spark_bigquery_insertAll_it_"+System.nanoTime();
    public static final String TABLE = "testTable";
    public static final String DESCRIPTION = "Spark BigQuery connector write session tests.";

    public static final String BIGQUERY_PUBLIC_DATA = "bigquery-public-data";
    public static final String SMALL_DATA_DATASET = "san_francisco_bikeshare";
    public static final String SMALL_DATA_TABLE = "bikeshare_station_status";
    public static final String SMALL_DATA_ID = BIGQUERY_PUBLIC_DATA+":"+SMALL_DATA_DATASET+"."+SMALL_DATA_TABLE;
    public static final String MB20_DATASET = "baseball";
    public static final String MB20_TABLE = "games_post_wide";
    public static final String MB20_ID = BIGQUERY_PUBLIC_DATA+":"+MB20_DATASET+"."+MB20_TABLE;
    public static final String MB100_DATASET = "open_images";
    public static final String MB100_TABLE = "annotations_bbox";
    public static final String MB100_ID = BIGQUERY_PUBLIC_DATA+":"+MB100_DATASET+"."+MB100_TABLE; // 156 MB
    public static final String GB3_DATASET = "open_images";
    public static final String GB3_TABLE = "images";
    public static final String GB3_ID = BIGQUERY_PUBLIC_DATA+":"+GB3_DATASET+"."+GB3_TABLE; // 3.56 GB
    public static final String GB20_DATASET = "samples";
    public static final String GB20_TABLE = "natality";
    public static final String GB20_ID = BIGQUERY_PUBLIC_DATA+":"+GB20_DATASET+"."+GB20_TABLE; // 21 GB
    public static final String GB250_DATASET = "samples";
    public static final String GB250_TABLE = "trigrams";
    public static final String GB250_ID = BIGQUERY_PUBLIC_DATA+":"+GB250_DATASET+"."+GB250_TABLE; //256 GB

    public static SparkSession spark;
    public static BigQuery bigquery;

    public static Dataset<Row> allTypesDf;
    public static Dataset<Row> smallDataDf;
    public static Dataset<Row> MB20Df;
    public static Dataset<Row> MB100Df;
    public static Dataset<Row> GB3Df;
    public static Dataset<Row> GB20Df;
    public static Dataset<Row> GB250Df;


    @BeforeClass
    public static void init() throws Exception {
        logger.setLevel(Level.DEBUG);
        spark = SparkSession
                .builder()
                .appName("Application Name")
                .config("some-config", "some-value")
                .master("local[*]")
                .getOrCreate();
        allTypesDf = spark.createDataFrame(Arrays.asList(ALL_TYPES_ROWS), ALL_TYPES_SCHEMA);
        smallDataDf = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", SMALL_DATA_ID)
                .load();
        MB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB20_ID)
                .load().drop("startTime").drop("createdAt").drop("updatedAt")/*.coalesce(20)*/.toDF();
        MB100Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB100_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB3Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB3_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB20_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB250Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB250_ID)
                .load()/*.coalesce(20).toDF()*/;

        bigquery = BigQueryOptions.getDefaultInstance().getService();
        DatasetInfo datasetInfo =
                DatasetInfo.newBuilder(/* datasetId = */ DATASET).setDescription(DESCRIPTION).build();
        bigquery.create(datasetInfo);
        logger.info("Created test dataset: " + DATASET);
    }

    @AfterClass
    public static void close() throws Exception {
        /*if (bigquery != null) {
            RemoteBigQueryHelper.forceDelete(bigquery, DATASET);
            logger.info("Deleted test dataset: " + DATASET);
        }*/
    }

    @Test
    public void testSparkBigQueryWriteAllTypes() throws Exception {
        String writeTo = "all_types";

        Dataset<Row> expectedDF = allTypesDf;

        expectedDF.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> actualDF = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        assertThat(actualDF.schema()).isEqualTo(ALL_TYPES_SCHEMA_BIGQUERY_REPRESENTATION);

        Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
        assertThat(intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
    }

    @Test
    public void testSparkWriteSaveModes() throws Exception {
        String writeTo = "append";

        Dataset<Row> twiceAsBigDf = smallDataDf.unionAll(smallDataDf);

        Dataset<Row> expectedDF = twiceAsBigDf;

        smallDataDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.ErrorIfExists)
                .save();

        smallDataDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> actualDF = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
        // append was successful:
        assertThat(intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());


        expectedDF = smallDataDf;

        smallDataDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        actualDF = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        intersection = actualDF.intersectAll(expectedDF);
        // overwrite was successful:
        assertThat(intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());


        expectedDF = smallDataDf;

        twiceAsBigDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Ignore)
                .save();

        actualDF = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        intersection = actualDF.intersectAll(expectedDF);
        // overwrite was successful:
        assertThat(intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());


        try {
            twiceAsBigDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                    .option("table", writeTo)
                    .option("dataset", DATASET)
                    .option("project", PROJECT)
                    .mode(SaveMode.ErrorIfExists)
                    .save();
            fail("Did not throw an error for ErrorIfExists");
        } catch (RuntimeException e) {
            // Successfully threw an exception for ErrorIfExists
        }
    }

    @Test
    public void testSparkBigQueryWriterAbort() throws Exception {

    }

    @Test
    public void testSparkBigQueryWrite20MB() throws Exception {
        String writeTo = "20MB";

        MB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        // TODO: simple num bytes print line

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB20_DATASET, MB20_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite100MB() throws Exception {
        String writeTo = "100MB";

        MB100Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB100_DATASET, MB100_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite3GB() throws Exception {
        String writeTo = "3GB";

        GB3Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB3_DATASET, GB3_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite20GB() throws Exception {
        String writeTo = "20GB";

        GB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB20_DATASET, GB20_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite250GB() throws Exception {
        String writeTo = "250GB";

        GB250Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryInsertAllDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB250_DATASET, GB250_TABLE)).getNumBytes());
    }

    public static final StructType ALL_TYPES_SCHEMA = new StructType()
            .add(new StructField("int_req", IntegerType,false, new MetadataBuilder().putString("description", "required integer").build()))
            .add(new StructField("int_null", IntegerType, true, Metadata.empty()))
            .add(new StructField("long", LongType, true, Metadata.empty()))
            .add(new StructField("short", ShortType, true, Metadata.empty()))
            .add(new StructField("bytenum", ByteType, true, Metadata.empty()))
            .add(new StructField("bool", BooleanType, true, Metadata.empty()))
            .add(new StructField("str", StringType, true, Metadata.empty()))
            .add(new StructField("date", DateType, true, Metadata.empty()))
            .add(new StructField("timestamp", TimestampType, true, Metadata.empty()))
            .add(new StructField("binary", BinaryType, true, Metadata.empty()))
            .add(new StructField("float", DoubleType, true, Metadata.empty()))
            .add(new StructField("nums", new StructType()
                    .add(new StructField("min", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("max", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("pi", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("big_pi", new DecimalType(38,9), true, Metadata.empty())),
                    true, Metadata.empty()))
            .add(new StructField("int_arr",
                    new ArrayType(IntegerType,true),
                    true, Metadata.empty()))
            .add(new StructField("int_struct_arr",
                    new ArrayType(new StructType()
                            .add(new StructField("i", IntegerType, true, Metadata.empty())),
                            true),
                    true, Metadata.empty()));

    // same as ALL_TYPES_SCHEMA, except all IntegerType's are LongType's.
    public static final StructType ALL_TYPES_SCHEMA_BIGQUERY_REPRESENTATION = new StructType()
            .add(new StructField("int_req", LongType,false, new MetadataBuilder().putString("description", "required integer").build()))
            .add(new StructField("int_null", LongType, true, Metadata.empty()))
            .add(new StructField("long", LongType, true, Metadata.empty()))
            .add(new StructField("short", LongType, true, Metadata.empty()))
            .add(new StructField("bytenum", LongType, true, Metadata.empty()))
            .add(new StructField("bool", BooleanType, true, Metadata.empty()))
            .add(new StructField("str", StringType, true, Metadata.empty()))
            .add(new StructField("date", DateType, true, Metadata.empty()))
            .add(new StructField("timestamp", TimestampType, true, Metadata.empty()))
            .add(new StructField("binary", BinaryType, true, Metadata.empty()))
            .add(new StructField("float", DoubleType, true, Metadata.empty()))
            .add(new StructField("nums", new StructType()
                    .add(new StructField("min", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("max", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("pi", new DecimalType(38,9), true, Metadata.empty()))
                    .add(new StructField("big_pi", new DecimalType(38,9), true, Metadata.empty())),
                    true, Metadata.empty()))
            .add(new StructField("int_arr",
                    new ArrayType(LongType,true),
                    true, Metadata.empty()))
            .add(new StructField("int_struct_arr",
                    new ArrayType(new StructType()
                            .add(new StructField("i", LongType, true, Metadata.empty())),
                            true),
                    true, Metadata.empty()));

    public static final Row[] ALL_TYPES_ROWS = new Row[]{
            RowFactory.create(
                    123456789,
                    null,
                    123456789L,
                    (short)1024,
                    (byte)127,
                    true,
                    "new byte[]{3, 4, 5, 6}",
                    new Date(1595010664123L),
                    new Timestamp(1595010664123L),
                    new byte[]{1, 2, 3, 4},
                    1.2345,
                    RowFactory.create(
                            Decimal.apply(new BigDecimal("-99999999999999999999999999999.999999999")),
                            Decimal.apply(new BigDecimal("99999999999999999999999999999.999999999")),
                            Decimal.apply(new BigDecimal("3.14")),
                            Decimal.apply(new BigDecimal("31415926535897932384626433832.795028841"))
                    ),
                    new int[]{1,2,3,4},
                    new Row[]{
                            RowFactory.create(
                                    1
                            )
                    }
            )
    };
}
