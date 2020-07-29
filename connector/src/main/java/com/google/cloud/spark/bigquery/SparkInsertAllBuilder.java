package com.google.cloud.spark.bigquery;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SparkInsertAllBuilder {

  final Logger logger = LoggerFactory.getLogger(SparkInsertAllBuilder.class);

  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String MAPTYPE_ERROR_MESSAGE = "MapType is unsupported.";

  private final StructType sparkSchema;
  private final TableId tableId;
  private final BigQueryClient bigQueryClient;
  private final long maxBatchRowCount;
  private final long maxBatchSizeInBytes;

  private ExponentialBackOff exponentialBackOff;
  private Sleeper sleeper;

  // This is a changing limit based on the estimated average row-size in bytes, as calculated by
  // recordSizeEstimator.
  private long actualRowLimit;

  private InsertAllRequest.Builder insertAllRequestBuilder;
  private long currentRequestRowCount = 0;

  private long committedRowCount = 0;

  private RowSizeEstimator rowSizeEstimator;

  public SparkInsertAllBuilder(
      StructType sparkSchema,
      TableId tableId,
      BigQueryClient bigQueryClient,
      int numberOfFirstRowsToEstimate,
      long maxWriteBatchSizeInBytes,
      int maxWriteBatchRowCount,
      ExponentialBackOff exponentialBackOff) {
    this.sparkSchema = sparkSchema;
    this.tableId = tableId;
    this.bigQueryClient = bigQueryClient;
    this.maxBatchRowCount = maxWriteBatchRowCount;
    this.maxBatchSizeInBytes = maxWriteBatchSizeInBytes;

    this.actualRowLimit = maxWriteBatchRowCount;
    this.insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
    this.rowSizeEstimator = new RowSizeEstimator(numberOfFirstRowsToEstimate);

    this.exponentialBackOff = exponentialBackOff;
    this.sleeper = Sleeper.DEFAULT;
  }

  public void addRow(InternalRow record) throws IOException {
    Map<String, Object> insertAllRecord = internalRowToInsertAllRecord(sparkSchema, record);

    rowSizeEstimator.updateRowSizeEstimateAndActualRowLimit(insertAllRecord);
    insertAllRequestBuilder.addRow(insertAllRecord);
    currentRequestRowCount++;

    if (currentRequestRowCount == actualRowLimit) {
      // TODO: Add mechanism / error for a single row that might be exceeding size quotas...
      commit();
    }
  }

  public void commit() throws IOException {
    if (currentRequestRowCount == 0) {
      return;
    }

    insertAll(insertAllRequestBuilder.build());

    insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
    committedRowCount += currentRequestRowCount;
    currentRequestRowCount = 0;
  }

  private void insertAll(InsertAllRequest insertAllRequest) throws IOException {
    exponentialBackOff.reset();
    while (true) {
      InsertAllResponse insertAllResponse = bigQueryClient.insertAll(insertAllRequest);
      if (!insertAllResponse.hasErrors()) {
        break;
      }
      logger.error(insertAllResponse.getInsertErrors().toString());
      long nextBackOffMillis = exponentialBackOff.nextBackOffMillis();
      if (nextBackOffMillis == BackOff.STOP) {
        throw new SparkInsertAllException(insertAllResponse.getInsertErrors());
      }
      try {
        sleeper.sleep(nextBackOffMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Insert All request was interrupted during back-off on Spark side.", e);
      }
    }
  }

  public long getCommittedRows() {
    return committedRowCount;
  }

  public void abort() throws IOException {
    insertAllRequestBuilder = null;
  }

  public static Map<String, Object> internalRowToInsertAllRecord(
      StructType sparkSchema, InternalRow sparkRow) {
    Map<String, Object> insertAllRecord = new HashMap<>();
    int fieldIndex = 0;
    for (StructField field : sparkSchema.fields()) {
      DataType sparkType = field.dataType();
      String name = field.name();
      Object sparkValue = sparkRow.get(fieldIndex, sparkType);
      boolean nullable = field.nullable();

      insertAllRecord.put(name, toInsertAllType(sparkType, sparkValue, nullable));

      fieldIndex++;
    }
    return insertAllRecord;
  }

  public static Object toInsertAllType(DataType sparkType, Object sparkValue, boolean nullable) {
    if (sparkValue == null) {
      if (!nullable) {
        throw new IllegalArgumentException("Non-nullable field was null.");
      } else {
        return null;
      }
    }

    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      DataType elementType = arrayType.elementType();
      Object[] sparkArrayData = ((ArrayData) sparkValue).toObjectArray(elementType);
      boolean containsNull = arrayType.containsNull();
      List<Object> insertAllValue = new ArrayList<>();
      for (Object sparkElement : sparkArrayData) {
        Object converted = toInsertAllType(elementType, sparkElement, containsNull);
        if (converted == null) {
          continue;
        }
        insertAllValue.add(converted);
      }
      return insertAllValue;
    }

    if (sparkType instanceof StructType) {
      return internalRowToInsertAllRecord((StructType) sparkType, (InternalRow) sparkValue);
    }

    if (sparkType instanceof ByteType
        || sparkType instanceof ShortType
        || sparkType instanceof IntegerType
        || sparkType instanceof LongType) {
      return ((Number) sparkValue).longValue();
    } // TODO: CalendarInterval

    if (sparkType instanceof DateType) {
      return new String(
          DateTimeUtils$.MODULE$.dateToString(((Number) sparkValue).intValue()).getBytes(), UTF_8);
    }

    if (sparkType instanceof TimestampType) {
      return new String(
          DateTimeUtils$.MODULE$.timestampToString(((Number) sparkValue).longValue()).getBytes(),
          UTF_8);
    }

    if (sparkType instanceof FloatType || sparkType instanceof DoubleType) {
      return ((Number) sparkValue).doubleValue();
    }

    if (sparkType instanceof DecimalType) {
      return new String(sparkValue.toString().getBytes(), UTF_8);
    }

    if (sparkType instanceof BooleanType) {
      return sparkValue;
    }

    if (sparkType instanceof BinaryType) {
      return new String(Base64.getEncoder().encode((byte[]) sparkValue), UTF_8);
    }

    if (sparkType instanceof StringType) {
      return new String(((UTF8String) sparkValue).getBytes(), UTF_8);
    }

    if (sparkType instanceof MapType) {
      throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
    }

    throw new IllegalStateException("Unexpected type: " + sparkType);
  }

  static class SparkInsertAllException extends IOException {

    SparkInsertAllException(Map<Long, List<BigQueryError>> insertErrors) {
      super(createMessage(insertErrors));
    }

    static String createMessage(Map<Long, List<BigQueryError>> insertErrors) {
      StringBuilder stringBuilder = new StringBuilder();
      for (long row : insertErrors.keySet()) {
        stringBuilder.append(String.format("Row: %d\n\tError: %s\n\n", row, insertErrors.get(row)));
      }
      return stringBuilder.toString();
    }
  }

  class RowSizeEstimator {
    final int numberOfRowsToEstimate;

    AverageFraction averageRowSizeInBytes = new AverageFraction(); // in bytes
    int estimatesCount;

    RowSizeEstimator(int numberOfRowsToEstimate) {
      this.numberOfRowsToEstimate = numberOfRowsToEstimate;
      this.estimatesCount = 0;
    }

    void updateRowSizeEstimateAndActualRowLimit(Map<String, Object> insertAllRecord) {
      if (estimatesCount < numberOfRowsToEstimate) {
        averageRowSizeInBytes.addToAverage(SizeEstimator.estimate(insertAllRecord));
        // TODO: take 3 sizes: Shakespeare w 1 byte per row, other tables w 1kB row, 10kB rows. Need
        // to adjust max_batch_size?
        // TODO: fundamental assumption: all records are ~same size... magic number: 10? 1? batch
        // size? max_byte? make parameters.

        // pick the smaller of the two limits:
        // 1. how many rows would fit within the maximum number of bytes per batch
        // 2. the actual limit on the number of rows per batch
        actualRowLimit =
            Math.min(maxBatchSizeInBytes / averageRowSizeInBytes.getAverage(), maxBatchRowCount);

        estimatesCount++;

        logger.debug(
            "Average record size estimate in bytes: {}. Actual row limit: {}",
            averageRowSizeInBytes.getAverage(),
            actualRowLimit);
      }
    }

    class AverageFraction {
      List<Long> sizes = new ArrayList<>();
      long count = 0;

      AverageFraction() {}

      void addToAverage(long size) {
        sizes.add(size);
        count++;
      }

      long getAverage() {
        if (count == 0) {
          throw new RuntimeException("Cannot access a moving average of no variables.");
        }
        long sum = 0;
        for (long size : sizes) { // TODO: catch overflow.
          sum += size;
        }
        return sum / count;
      }
    }
  }
}
