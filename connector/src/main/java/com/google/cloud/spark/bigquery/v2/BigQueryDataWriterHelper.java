/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.api.client.util.Sleeper;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.connector.common.WriteStreamPool;
import com.google.cloud.bigquery.storage.v1alpha2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BigQueryDataWriterHelper {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterHelper.class);
  final long APPEND_REQUEST_SIZE = 1000L * 1000L; // 1MB limit for each append
  final Sleeper sleeper = Sleeper.DEFAULT;
  final ExecutorService validateThread = Executors.newCachedThreadPool();

  private final BigQueryWriteClient writeClient;
  private final String writeStreamName;
  private final ProtoBufProto.ProtoSchema protoSchema;

  private StreamWriter streamWriter;
  private ProtoBufProto.ProtoRows.Builder protoRows;

  private long appendRows = 0; // number of rows waiting for the next append request
  private long appendBytes = 0; // number of bytes waiting for the next append request

  private long writeStreamBytes = 0; // total bytes of the current write-stream
  private long writeStreamRows = 0; // total offset / rows of the current write-stream

  protected BigQueryDataWriterHelper(
      BigQueryWriteClientFactory writeClientFactory,
      WriteStreamPool writeStreamPool,
      ProtoBufProto.ProtoSchema protoSchema,
      int partitionId) {
    this.writeClient = writeClientFactory.createBigQueryWriteClient();
    this.writeStreamName = writeStreamPool.getWriteStreamName(partitionId, writeClient);
    this.protoSchema = protoSchema;

    try {
      long timeout = (long) (Math.random() * 60000L * partitionId / 1000);
      sleeper.sleep(timeout);
      this.streamWriter = StreamWriter.newBuilder(writeStreamName).build();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Could not get stream-writer.", e);
    }
    this.protoRows = ProtoBufProto.ProtoRows.newBuilder();
  }

  protected void addRow(ByteString message) {
    int messageSize = message.size();

    if (appendBytes + messageSize > APPEND_REQUEST_SIZE) {
      appendRequest();
      appendRows = 0;
      appendBytes = 0;
    }

    protoRows.addSerializedRows(message);
    appendBytes += messageSize;
    appendRows++;
  }

  private void appendRequest() {
    Storage.AppendRowsRequest.Builder requestBuilder =
        Storage.AppendRowsRequest.newBuilder().setOffset(Int64Value.of(writeStreamRows));

    Storage.AppendRowsRequest.ProtoData.Builder dataBuilder =
        Storage.AppendRowsRequest.ProtoData.newBuilder();
    dataBuilder.setWriterSchema(protoSchema);
    dataBuilder.setRows(protoRows.build());

    requestBuilder.setProtoRows(dataBuilder.build()).setWriteStream(writeStreamName);

    ApiFuture<Storage.AppendRowsResponse> response = streamWriter.append(requestBuilder.build());

    validateThread.submit(new ValidateResponses(response, this.writeStreamRows));

    clearProtoRows();
    this.writeStreamRows += appendRows; // add the # of rows appended to writeStreamRows
    this.writeStreamBytes += appendBytes;
  }

  protected void finalizeStream() {
    if (this.appendRows != 0 || this.appendBytes != 0) {
      appendRequest();
    }

    try {
      validateThread.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Validation of API responses was interrupted.", e);
    }

    Storage.FinalizeWriteStreamResponse finalizeResponse =
        writeClient.finalizeWriteStream(
            Storage.FinalizeWriteStreamRequest.newBuilder().setName(writeStreamName).build());

    if (finalizeResponse.getRowCount() != writeStreamRows) {
      throw new RuntimeException("Finalize response had an unexpected row count.");
    }

    writeClient.shutdown();

    logger.debug(
        "Write-stream {} finalized with row-count {}",
        writeStreamName,
        finalizeResponse.getRowCount());
  }

  private void clearProtoRows() {
    this.protoRows.clear();
  }

  protected String getWriteStreamName() {
    return writeStreamName;
  }

  protected long getDataWriterRows() {
    return writeStreamRows;
  }

  protected void abort() {
    if (streamWriter != null) {
      streamWriter.close();
    }
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
  }

  static class ValidateResponses implements Runnable {

    final long offset;
    final ApiFuture<Storage.AppendRowsResponse> response;

    ValidateResponses(ApiFuture<Storage.AppendRowsResponse> response, long offset) {
      this.offset = offset;
      this.response = response;
    }

    @Override
    public void run() {
      try {
        if (this.offset != this.response.get().getOffset()) {
          throw new RuntimeException("Append request offset did not match expected offset.");
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Could not get offset for append request.", e);
      }
    }
  }
}
