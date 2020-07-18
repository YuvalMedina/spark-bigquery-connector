package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class BigQueryInsertAllWriterCommitMessage implements WriterCommitMessage {

  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final long rowCount;

  public BigQueryInsertAllWriterCommitMessage(
      int partitionId, long taskId, long epochId, long rowCount) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.rowCount = rowCount;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getTaskId() {
    return taskId;
  }

  public long getEpochId() {
    return epochId;
  }

  public long getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "BigQueryInsertAllWriterCommitMessage{"
        + "partitionId="
        + partitionId
        + ", taskId="
        + taskId
        + ", epochId="
        + epochId
        + ", rowCount="
        + rowCount
        + '}';
  }
}
