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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

import com.google.common.base.Preconditions;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
public class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private EditLogFileOutputStream currentStream;
  private int sizeOutputFlushBuffer = 512*1024;

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }
  
  @Override
  public void startLogSegment(long txid) throws IOException {
    Preconditions.checkState(currentStream == null,
        "Trying to begin log segment for txid " + txid +
        " when current stream is " + currentStream);
    
    File newInProgress = NNStorage.getInProgressEditsFile(sd, txid);
    currentStream = new EditLogFileOutputStream(newInProgress,
                                                sizeOutputFlushBuffer );
    currentStream.create();
  }

  /**
   * TODO
   */  
  @Override
  public void endLogSegment(long firstTxid,
      long lastTxid) throws IOException {
    closeCurrentStream();
    finalizeEditsFile(firstTxid, lastTxid);
  }
  
  @Override
  public void abortCurrentSegment() throws IOException {
    if (currentStream != null) {
      closeCurrentStream();
    }
  }
  
  @Override
  public void close() {
    assert currentStream == null :
      "Closing journal manager when in the middle of segment: " + currentStream;
  }

  /**
   * Close the current log segment without finalizing the filename
   */
  private void closeCurrentStream() throws IOException {
    Preconditions.checkNotNull(currentStream,
        "No stream for %s", this);
    
    currentStream.setReadyToFlush();
    currentStream.flush();
    currentStream.close();
    currentStream = null;
  }
  
  private void finalizeEditsFile(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = NNStorage.getInProgressEditsFile(
        sd, firstTxId);
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.debug("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");
    if (!inprogressFile.renameTo(dstFile)) {
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
  }
 
  @Override
  public StorageDirectory getStorageDirectory() {
    return sd;
  }
  
  
  @Override // TODO do we need this?
  public boolean canRestore() {
    return sd.getCurrentDir().canWrite();
  }

  @Override
  public String toString() {
    return "FileJournalManager for " +
      sd + " (currentStream=" + currentStream + ")"; 
  }

  @Override
  public EditLogOutputStream getCurrentStream() {
    Preconditions.checkNotNull(currentStream,
        "FileJournalManager %s is currently in between rolls",
        this);
    return currentStream;
  }

  @Override
  public void setBufferCapacity(int size) {
    this.sizeOutputFlushBuffer = size;
  }

  void setCurrentStreamForTests(EditLogFileOutputStream injectedStream) {
    this.currentStream = injectedStream;
  }
}
