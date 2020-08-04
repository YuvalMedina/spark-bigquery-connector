package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.cloud.bigquery.storage.v1alpha2.Stream;

import java.io.Serializable;

public class WriteStreamPool implements Serializable {

    private final int NUM_WRITE_STREAMS = 100;
    // A pool of 100 write-stream names:
    private final String[] writeStreamPool = new String[NUM_WRITE_STREAMS];
    private final String tablePath;

    public WriteStreamPool(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getWriteStreamName(int partitionId, BigQueryWriteClient writeClient) {
        int writeStreamIndex = partitionId % NUM_WRITE_STREAMS;
        if (writeStreamPool[writeStreamIndex] == null) {
            Stream.WriteStream writeStream =
                    writeClient.createWriteStream(
                            Storage.CreateWriteStreamRequest.newBuilder()
                                    .setParent(tablePath)
                                    .setWriteStream(
                                            Stream.WriteStream.newBuilder()
                                                    .setType(Stream.WriteStream.Type.PENDING)
                                                    .build())
                                    .build());
            writeStreamPool[writeStreamIndex] = writeStream.getName();
        }
        return writeStreamPool[writeStreamIndex];
    }
}
