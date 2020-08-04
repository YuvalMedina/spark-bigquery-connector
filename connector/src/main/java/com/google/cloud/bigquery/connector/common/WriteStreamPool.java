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
