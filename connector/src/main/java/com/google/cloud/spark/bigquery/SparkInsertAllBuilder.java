package com.google.cloud.spark.bigquery;

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
  private static final long MAX_BATCH_ROW_COUNT = 500;
  private static final long MAX_BATCH_BYTES = 5L * 1000L * 1000L; // 5MB

  private final StructType sparkSchema;
  private final TableId tableId;
  private final BigQueryClient bigQueryClient;

  private InsertAllRequest.Builder insertAllRequestBuilder;
  private long currentRequestRowCount = 0;

  private long committedRowCount = 0;

  private RecordSizeEstimator recordSizeEstimator = new RecordSizeEstimator();

  public SparkInsertAllBuilder(
      StructType sparkSchema, TableId tableId, BigQueryClient bigQueryClient) {
    this.sparkSchema = sparkSchema;
    this.tableId = tableId;
    this.bigQueryClient = bigQueryClient;

    this.insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
  }

  public void addRow(InternalRow record) throws IOException {
    Map<String, Object> insertAllRecord = internalRowToInsertAllRecord(sparkSchema, record);

    recordSizeEstimator.updateEstimate(insertAllRecord);
    insertAllRequestBuilder.addRow(insertAllRecord);
    currentRequestRowCount++;

    if (currentRequestRowCount == MAX_BATCH_ROW_COUNT
        || currentRequestRowCount * recordSizeEstimator.averageRecordSize >= MAX_BATCH_BYTES) {
      // TODO: Add mechanism / error for a single row that might be exceeding size quotas...
      commit();
    }
  }

  public void commit() throws IOException {
    if (currentRequestRowCount == 0) {
      return;
    }

    // logger.debug("Commit with rowcount {} and bytecount {}", currentRequestRowCount,
    // currentRequestByteCount);

    InsertAllResponse insertAllResponse = bigQueryClient.insertAll(insertAllRequestBuilder.build());
    if (insertAllResponse.hasErrors()) {
      throw new SparkInsertAllException(insertAllResponse.getInsertErrors());
    }
    insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
    committedRowCount += currentRequestRowCount;
    currentRequestRowCount = 0;
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
    } // TODO:

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

  class RecordSizeEstimator {
    long averageRecordSize; // in bytes
    int estimatesDone;

    RecordSizeEstimator() {
      estimatesDone = 0;
    }

    void updateEstimate(Map<String, Object> insertAllRecord) {
      if (estimatesDone < 10) {
        averageRecordSize =
            ((averageRecordSize * estimatesDone) + estimateOneRecord(insertAllRecord))
                / (estimatesDone + 1);
        estimatesDone++;
        // logger.debug("Current record estimate: {}", averageRecordSize);
      }
    }

    long estimateOneRecord(Map<String, Object> insertAllRecord) {
      long recordByteCounter = 0;
      for (Object value : insertAllRecord.values()) {
        recordByteCounter += SizeEstimator.estimate(value);
      }
      return recordByteCounter;
    }
  }
}
