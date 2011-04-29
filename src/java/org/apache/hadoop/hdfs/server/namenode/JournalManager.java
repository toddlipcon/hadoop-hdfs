/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * A JournalManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
public interface JournalManager {

  /**
   * @return the StorageDirectory associated with this journal,
   * or null if this is not a disk-based journal 
   */
  StorageDirectory getStorageDirectory();

  /**
   * @return the current stream to which to direct edits
   */
  EditLogOutputStream getCurrentStream();

  /**
   * TODO
   */
  void startLogSegment(long txId) throws IOException;
  void endLogSegment(long firstTxid, long lastTxid) throws IOException;
  void abortCurrentSegment() throws IOException;

  /**
   * Set the amount of memory that this stream should use to buffer edits
   */
  void setBufferCapacity(int size);

  boolean canRestore();

  void close();


}
