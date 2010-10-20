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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.LongWritable;

@InterfaceAudience.Private
public class BackupStorage extends FSImage {
  /** Backup input stream for loading edits into memory */
  private EditLogBackupInputStream backupInputStream;
  
  private final FSEditLogLoader logLoader;
  
  final BackupNode backupNode;
  
  /**
   *  Is journal spooling in progress
   *
   * If journalState == IN_SYNC, then this means:
   *   - namesystemReflectsLogsThrough == receivingLogsForIndex
   *   - we are actively following the same log as the primary NN
   * If journalState == JOURNALING:
   *   - namesystemReflectsLogsThrough < receivingLogsForIndex
   *   - the edits log referenced by this variable is finalized on primary NN
   */
  volatile JournalState journalState;

  // The log messages going into journal() correspond
  // to this log index inprogress
  volatile int receivingLogsForIndex = -1;

  /**
   * If set, then the backup storage will transition
   * from IN_SYNC -> JOURNALING the next time it is told that
   * the master has rolled edits.
   */
  volatile boolean stopApplyingLogsAtNextRoll = false;

  /**
   * TODO - I think this could be moved to FSImage and replace currentLogsIndex
   */
  
  static enum JournalState {
    IN_SYNC,
    JOURNALING;
  }

  /**
   */
  BackupStorage(BackupNode backupNode) {
    super();
    this.backupNode = backupNode;
    logLoader = new FSEditLogLoader(namesystem);
    journalState = JournalState.JOURNALING;
    backupInputStream = new EditLogBackupInputStream("TODO name me");
  }
  
  @Override
  public boolean isConversionNeeded(StorageDirectory sd) {
    return false;
  }

  boolean needsBootstrapping(CheckpointSignature sig) {
    return namesystemReflectsLogsThrough < sig.newestImageIndex;
  }
  
  /**
   * Analyze backup storage directories for consistency.<br>
   * Recover from incomplete checkpoints if required.<br>
   * Read VERSION and fstime files if exist.<br>
   * Do not load image or edits.
   * 
   * @param imageDirs list of image directories as URI.
   * @param editsDirs list of edits directories URI.
   * @throws IOException if the node should shutdown.
   */
  void recoverCreateRead(Collection<URI> imageDirs,
                         Collection<URI> editsDirs) throws IOException {
    setStorageDirectories(imageDirs, editsDirs);
    for(Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // fail if any of the configured storage dirs are inaccessible 
          throw new InconsistentFSStateException(sd.getRoot(),
                "checkpoint directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          // for backup node all directories may be unformatted initially
          LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
          LOG.info("Formatting ...");
          sd.clearDirectory(); // create empty current
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);
        }
        if(curState != StorageState.NOT_FORMATTED) {
          sd.read(); // read and verify consistency with other directories
        }
      } catch(IOException ioe) {
        sd.unlock();
        throw ioe;
      }
    }
  }

  /**
   * Resets the namespace, does not change on disk storage
   */
  synchronized void reset() throws IOException {
    // reset NameSpace tree
    FSDirectory fsDir = getFSNamesystem().dir;
    fsDir.reset();
    namesystemReflectsLogsThrough = -1;
  }

  /**
   * Load checkpoint from local files only if the memory state is empty.<br>
   * Set new checkpoint time received from the name-node.<br>
   * Move <code>lastcheckpoint.tmp</code> to <code>previous.checkpoint</code>.
   * @throws IOException
   */
  void loadCheckpoint(CheckpointSignature sig) throws IOException {
    // load current image and journal if it is not in memory already

    FSDirectory fsDir = getFSNamesystem().dir;
    if(fsDir.isEmpty()) {
      Iterator<StorageDirectory> itImage = dirIterator(NameNodeDirType.IMAGE);
      Iterator<StorageDirectory> itEdits = dirIterator(NameNodeDirType.EDITS);
      if(!itImage.hasNext() || ! itEdits.hasNext())
        throw new IOException("Could not locate checkpoint directories");
      StorageDirectory sdName = itImage.next();
      StorageDirectory sdEdits = itEdits.next();

      getFSDirectoryRootLock().writeLock();
      try { // load image under rootDir lock
	loadFSImage(FSImage.getImageFile(sdName, NameNodeFile.IMAGE, sig.newestImageIndex));
      } finally {
        getFSDirectoryRootLock().writeUnlock();
      }
      
      List<File> editsFiles = new ArrayList<File>();
      for (int i = (int)sig.newestImageIndex; i <= (int)sig.newestFinalizedEditLogIndex; i++) {
        editsFiles.add(getFinalizedEditsFile(sdEdits, i));
      }
      loadFSEdits(editsFiles); 
    }

    // set storage fields
    setStorageInfo(sig);
    mostRecentSavedImageIndex = sig.newestImageIndex;
    namesystemReflectsLogsThrough = sig.newestFinalizedEditLogIndex + 1;
  }
  
  // TODO move me to FSImage in general?
  void applyLog(int logIndex) throws IOException {
    if (namesystemReflectsLogsThrough != logIndex - 1) {
      throw new IllegalStateException(
          "Trying to apply log index " + logIndex + " when only have " +
          "data up to log index " + namesystemReflectsLogsThrough);
    }
    File logToLoad = getFirstReadableEditsFile(logIndex);
    loadEditsFromFile(logToLoad);
    namesystemReflectsLogsThrough = logIndex;
  }

  void loadEditsFromFile(File logToLoad) throws IOException {
    assert logToLoad != null && logToLoad.exists();
    loadFSEdits(Collections.<File>singletonList(logToLoad));    
  }
  
  /**
   * Save meta-data into fsimage files.
   * and create empty edits.
   */
  void saveCheckpoint() throws IOException {
    saveFSImage(namesystemReflectsLogsThrough + 1);
  }
  
  synchronized void stopReplicationOnNextRoll() {
    stopApplyingLogsAtNextRoll = true;
  }
  
  synchronized void waitForReplicationToStop() {
    if (journalState != JournalState.JOURNALING) {
      throw new IllegalStateException(
          "We've tried to stop replication but it didn't work");
    }
  }

  private FSDirectory getFSDirectoryRootLock() {
    return getFSNamesystem().dir;
  }

  synchronized void masterRolledLogs(int targetLogIndex) throws IOException {
    LOG.info("Master has rolled logs to index: " + targetLogIndex);
    // If we've never opened our logs, open them now
    if (editLog == null) {
      assert receivingLogsForIndex == -1;
      LOG.info("We've never opened logs before, opening new ones now!");
      receivingLogsForIndex = targetLogIndex;
      editLog = new FSEditLog(this, targetLogIndex);
      editLog.openNewLogs(targetLogIndex);
    } else {
      LOG.info("Rolling our logs to match");
      assert targetLogIndex == receivingLogsForIndex + 1;
      receivingLogsForIndex = targetLogIndex;
      editLog.rollEditLog(targetLogIndex);
    }
    if (stopApplyingLogsAtNextRoll) {
      LOG.info("Stopped log application at log index " + targetLogIndex);
      journalState = JournalState.JOURNALING;
      stopApplyingLogsAtNextRoll = false;
    }    
    // If we're still in sync, then update the fact that we're now syncing
    // the new log. It's important that this come after the switch from
    // IN_SYNC -> JOURNALING, because if we're just journaling we don't want to
    // claim that we have applied edits from the next log.
    if (journalState == JournalState.IN_SYNC) {
      namesystemReflectsLogsThrough = targetLogIndex;
    }
  }

  
  /**
   * Journal writer journals new meta-data state.
   * <ol>
   * <li> If Journal Spool state is OFF then journal records (edits)
   * are applied directly to meta-data state in memory and are written 
   * to the edits file(s).</li>
   * <li> If Journal Spool state is INPROGRESS then records are only 
   * written to edits.new file, which is called Spooling.</li>
   * <li> Journal Spool state WAIT blocks journaling until the
   * Journal Spool reader finalizes merging of the spooled data and
   * switches to applying journal to memory.</li>
   * </ol>
   * @param length length of data.
   * @param data serialized journal records.
   * @throws IOException
   * @see #convergeJournalSpool()
   */
  synchronized void journal(int length, byte[] data) throws IOException {
    if (editLog == null) {
      assert receivingLogsForIndex == -1;
      LOG.info("Not journaling yet");
      return;
    }
    assert receivingLogsForIndex != -1;

    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      if (journalState == JournalState.IN_SYNC) {
          backupInputStream.setBytes(data);
          logLoader.loadEditRecords(
              getLayoutVersion(),
              backupInputStream.getDataInputStream(), true);
          getFSNamesystem().dir.updateCountForINodeWithQuota(); // inefficient!
      }
      
      // write to files
      editLog.logEdit(length, data);
      editLog.logSync();
    } finally {
      backupInputStream.clear();
    }
  }
  
  /**
   * This is called after we've performed a checkpoint, and we assume that
   * the edits got ahead of us.
   */
  void catchupSynchronization() throws IOException {
    if (backupNode.isRole(NamenodeRole.CHECKPOINT)) {
      // Checkpoint nodes don't stream edits!
      return;
    }
    assert journalState == JournalState.JOURNALING;
    assert receivingLogsForIndex != -1;
    
    boolean converged = false;
    while (namesystemReflectsLogsThrough < receivingLogsForIndex && !converged) {
      int nextLogToApply = namesystemReflectsLogsThrough + 1;
      if (nextLogToApply < receivingLogsForIndex) {
        fetchLogIfNecessary(nextLogToApply);
        applyLog(nextLogToApply);
      } else {
        converged = tryConverge(nextLogToApply);
      }
    }
    // TODO need to update quota info?
  }
  
  private boolean tryConverge(int logIndex) throws IOException {
    LOG.info("Trying to converge on log index: " + logIndex);
    File logFile = getFirstReadableFile(NameNodeDirType.EDITS,
        NameNodeFile.EDITS_INPROGRESS, logIndex);
    if (logFile == null) {
      // the file got finalized
      assertEditsWereFinalized(logIndex);
      return false;
    }
    
    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
    EditLogFileInputStream inEditStream;
    try {
      inEditStream = new EditLogFileInputStream(logFile);
    } catch (FileNotFoundException fnfe) {
      assertEditsWereFinalized(logIndex);
      return false;      
    }
    DataInputStream editsDIS = inEditStream.getDataInputStream();
    int numLoaded = loader.loadFSEdits(editsDIS, false);
    LOG.info("Converging input streams on index " + logIndex + ": got " +
        numLoaded + " edits from inprogress edits at " + logFile);
    synchronized (this) {
      if (receivingLogsForIndex <= logIndex) {
        LOG.info("Still receiving edits for this same log, reading last edits");
        assert receivingLogsForIndex == logIndex;
        numLoaded = loader.loadEditRecords(getLayoutVersion(), editsDIS, true);
        LOG.info("Read " + numLoaded + " more edits from log #" + logIndex);
        journalState = JournalState.IN_SYNC;
        namesystemReflectsLogsThrough = logIndex;
        return true;
      }
    }
    // if we got here, then the edits must have rolled while we were reading them.
    // Still have to read the tail of this file
    assertEditsWereFinalized(logIndex);
    LOG.info("Edits #" + logIndex + " rolled while we were converging");
    numLoaded = loader.loadEditRecords(getLayoutVersion(), editsDIS, true);
    LOG.info("Loaded " + numLoaded + " more edits, but failed to converge.");
    // TODO need to update quota info?
    return false;
  }
  
  private void assertEditsWereFinalized(int logIndex) throws IOException {
    File finalizedFile = getFirstReadableEditsFile(logIndex);
    assert finalizedFile != null && finalizedFile.exists() :
      "In progress file disappeared, expect it was finalized";
  }
  
  void fetchLogIfNecessary(int logIndex) throws IOException {
    File finalTarget = getFirstReadableEditsFile(logIndex);
    if (finalTarget != null && finalTarget.exists()) {
      LOG.info("No need to download log index #" + logIndex);
      return;
    }
    
    Collection<File> list = getFiles(
        NameNodeFile.EDITS_INPROGRESS, NameNodeDirType.EDITS, logIndex);
    TransferFsImage.getFileClient(backupNode.nnHttpAddress,
        "getedit=" + logIndex,
        list.toArray(new File[list.size()]));
    LOG.info("Downloaded edits log " + logIndex);
    // Finalize the log
    FSEditLog.finalizeEditsFile(this, logIndex);
  }
  
  
  void fetchImageIfNecessary(int imageIndex) throws IOException {
    File finalTarget = getFirstReadableFsImageFile(imageIndex);
    if (finalTarget != null && finalTarget.exists()) {
      LOG.info("No need to download fsimage_" + imageIndex);
      return;
    }
    
    // get fsimage
    String fileid = "getimage=" + imageIndex;
  
    Collection<File> list = getFiles(
      NameNodeFile.IMAGE_NEW, NameNodeDirType.IMAGE, imageIndex);
    File[] srcNames = list.toArray(new File[list.size()]);
    assert srcNames.length > 0 : "No checkpoint targets.";
    TransferFsImage.getFileClient(backupNode.nnHttpAddress, fileid, srcNames);
  
    LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
             srcNames[0].length() + " bytes.");
    rollFSImage(imageIndex);
  }

}
