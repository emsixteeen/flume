/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.sink.hdfs.HDFSBadSeqWriter;
import org.apache.flume.sink.hdfs.HDFSBadDataStream;

public class HDFSBadWriterFactory extends HDFSWriterFactory {
  static final String BadSequenceFileType = "SequenceFile";
  static final String BadDataStreamType = "DataStream";
  static final String BadCompStreamType = "CompressedStream";

  public HDFSWriter getWriter(String fileType) throws IOException {
    if (fileType == BadSequenceFileType) {
      return new HDFSBadSeqWriter();
    } else if (fileType == BadDataStreamType) {
      return new HDFSBadDataStream();
    } else {
      throw new IOException("File type " + fileType + " not supported");
    }
  }
}
