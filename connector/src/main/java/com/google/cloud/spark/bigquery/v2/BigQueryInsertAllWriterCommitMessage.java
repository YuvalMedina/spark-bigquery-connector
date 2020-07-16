package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class BigQueryInsertAllWriterCommitMessage implements WriterCommitMessage {


  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final Long rowCount;

  public BigQueryInsertAllWriterCommitMessage(int partitionId, long taskId, long epochId,
                                              Long rowCount) {
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

  public long getRowCount() throws NoSuchFieldError {
    if(rowCount == null) {
      throw new NoSuchFieldError("Data Writer did not commit any rows.");
    }
    return rowCount;
  }

  @Override
  public String toString() {
    if(rowCount == null) {
      return "BigQueryInsertAllWriterCommitMessage{" +
              "partitionId=" + partitionId +
              ", taskId=" + taskId +
              ", epochId=" + epochId +
              '}';
    }
    return "BigQueryInsertAllWriterCommitMessage{" +
            "partitionId=" + partitionId +
            ", taskId=" + taskId +
            ", epochId=" + epochId +
            ", rowCount=" + rowCount +
            '}';
  }
}
