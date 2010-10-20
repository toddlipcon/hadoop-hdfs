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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog {
  
  abstract class Ops {
    public  static final byte OP_INVALID = -1;
    public static final byte OP_ADD = 0;
    public static final byte OP_RENAME_OLD = 1;  // rename
    public static final byte OP_DELETE = 2;  // delete
    public static final byte OP_MKDIR = 3;   // create directory
    public static final byte OP_SET_REPLICATION = 4; // set replication
    //the following two are used only for backward compatibility :
    @Deprecated public static final byte OP_DATANODE_ADD = 5;
    @Deprecated public static final byte OP_DATANODE_REMOVE = 6;
    public static final byte OP_SET_PERMISSIONS = 7;
    public static final byte OP_SET_OWNER = 8;
    public static final byte OP_CLOSE = 9;    // close after write
    public static final byte OP_SET_GENSTAMP = 10;    // store genstamp
    /* The following two are not used any more. Should be removed once
     * LAST_UPGRADABLE_LAYOUT_VERSION is -17 or newer. */
    public static final byte OP_SET_NS_QUOTA = 11; // set namespace quota
    public static final byte OP_CLEAR_NS_QUOTA = 12; // clear namespace quota
    public static final byte OP_TIMES = 13; // sets mod & access time on a file
    public static final byte OP_SET_QUOTA = 14; // sets name and disk quotas.
    public static final byte OP_RENAME = 15;  // new rename
    public static final byte OP_CONCAT_DELETE = 16; // concat files.
    public static final byte OP_SYMLINK = 17; // a symbolic link
    public static final byte OP_GET_DELEGATION_TOKEN = 18; //new delegation token
    public static final byte OP_RENEW_DELEGATION_TOKEN = 19; //renew delegation token
    public static final byte OP_CANCEL_DELEGATION_TOKEN = 20; //cancel delegation token
    public static final byte OP_UPDATE_MASTER_KEY = 21; //update master key
  
    /* 
     * The following operations are used to control remote edit log streams,
     * and not logged into file streams.
     */
    static final byte OP_ROLL_LOGS = NamenodeProtocol.JA_ROLL_LOGS;// primary NN has rolled edit logs
  }

  static final String NO_JOURNAL_STREAMS_WARNING = "!!! WARNING !!!" +
      " File system changes are not persistent. No journal streams.";

  private static int DEFAULT_OUTPUT_FLUSH_BUFFER = 512*1024;
  private volatile int sizeOutputFlushBuffer = DEFAULT_OUTPUT_FLUSH_BUFFER;

  private ArrayList<EditLogOutputStream> editStreams = null;
  private FSImage fsimage = null;

  // the index of the log currently being written to, or -1 if closed
  private int currentLogIndex;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // is an automatic sync scheduled?
  private volatile boolean isAutoSyncScheduled = false;

  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  FSEditLog(FSImage image, int logIndex) {
    fsimage = image;
    isSyncRunning = false;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();    
    currentLogIndex = logIndex;
  }


  private int getNumEditsDirs() {
   return fsimage.getNumStorageDirs(NameNodeDirType.EDITS);
  }

  synchronized int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  /**
   * Return the currently active edit streams.
   * This should be used only by unit tests.
   */
  ArrayList<EditLogOutputStream> getEditStreams() {
    return editStreams;
  }

  boolean isOpen() {
    return getNumEditStreams() > 0;
  }

  private void checkClosed() throws IOException {
    if (isOpen()) {
      throw new IOException("Logs must be closed.");
    }
  }
  
  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  synchronized void openNewLogs(int index) throws IOException {
    checkClosed();
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
    if (editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();

    currentLogIndex = index;

    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        createAndAddEditLogStream(sd, index);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Unable to create new log file in sd " + sd.getRoot(), e);
        // Remove the directory from list of storage directories
        if(al == null) al = new ArrayList<StorageDirectory>(1);
        al.add(sd);
      }
    }

    if(al != null) fsimage.processIOError(al, false);
  }

  synchronized void createAndAddEditLogStream(StorageDirectory sd,
                                              int index) throws IOException {
    if (index != currentLogIndex) {
      throw new IllegalStateException(
        "Current index is " +  currentLogIndex + " but trying to add " +
        "new edit index " + index);
    }

    File eFile = FSImage.getInProgressEditsFile(sd, index);
    if (eFile.exists()) {
      throw new IOException("Cannot create inprogress file with index " +
                            index + " in " + sd + " since inprogress file " +
                            "already exists");
    }
    if (FSImage.existsFinalizedEditsFile(sd, index)) {
      throw new IOException("Cannot create inprogress file with index " +
                            index + " in " + sd + " since finalized file " +
                            "already exists");
    }

    EditLogOutputStream eStream = new EditLogFileOutputStream(eFile,
        sizeOutputFlushBuffer);
    eStream.create();
    editStreams.add(eStream);
  }

  /**
   * Add an edit stream to transfer edits to the backup node.
   * 
   * Note that the beginning of the stream will start in the middle of some
   * edit file, so the backup node should throw them away until it gets a
   * OP_ROLL message.
   */
  synchronized void addBackupStream(
      NamenodeRegistration bnReg, NamenodeRegistration nnReg) throws IOException {
    EditLogOutputStream boStream = new EditLogBackupOutputStream(bnReg, nnReg);
    editStreams.add(boStream);
  }
  
  static void createEditLogFile(File name) throws IOException {
    if (name.exists()) {
      throw new IOException("Refusing to overwrite edits file at " + name);
    }
    EditLogOutputStream eStream = new EditLogFileOutputStream(name,
        DEFAULT_OUTPUT_FLUSH_BUFFER);
    eStream.create();
    eStream.close();
  }

  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    waitForSyncToFinish();
    if (editStreams == null || editStreams.isEmpty()) {
      return;
    }
    printStatistics(true);
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
    currentLogIndex = -1;
    
    ArrayList<EditLogOutputStream> errorStreams = null;
    Iterator<EditLogOutputStream> it = getOutputStreamIterator(null);
    while(it.hasNext()) {
      EditLogOutputStream eStream = it.next();
      try {
        closeStream(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("FSEditLog:close - failed to close stream " 
            + eStream.getName());
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
    editStreams.clear();
  }

  /**
   * Close and remove edit log stream.
   * @param index of the stream
   */
  synchronized private void removeStream(int index) {
    EditLogOutputStream eStream = editStreams.get(index);
    try {
      eStream.close();
    } catch (Exception e) {}
    editStreams.remove(index);
  }

  /**
   * The specified streams have IO errors. Close and remove them.
   * If propagate is true - close related StorageDirectories.
   * (is called with propagate value true from everywhere
   *  except fsimage.processIOError)
   */
  synchronized void processIOError(
      ArrayList<EditLogOutputStream> errorStreams,
      boolean propagate) {
    
    if (errorStreams == null || errorStreams.size() == 0) {
      return;                       // nothing to do
    }

    String lsd = fsimage.listStorageDirectories();
    FSNamesystem.LOG.info("current list of storage dirs:" + lsd);

    ArrayList<StorageDirectory> al = null;
    for (EditLogOutputStream eStream : errorStreams) {
      FSNamesystem.LOG.error("Unable to log edits to " + eStream.getName()
          + "; removing it");     

      StorageDirectory storageDir;
      if(propagate && eStream.getType() == JournalType.FILE && //find SD
          (storageDir = getStorage(eStream)) != null) {
        FSNamesystem.LOG.info("about to remove corresponding storage:" 
            + storageDir.getRoot().getAbsolutePath());
        // remove corresponding storage dir
        if(al == null) al = new ArrayList<StorageDirectory>(1);
        al.add(storageDir);
      }
      Iterator<EditLogOutputStream> ies = editStreams.iterator();
      while (ies.hasNext()) {
        EditLogOutputStream es = ies.next();
        if (es == eStream) {  
          try { eStream.close(); } catch (IOException e) {
            // nothing to do.
            FSNamesystem.LOG.warn("Failed to close eStream " + eStream.getName()
                + " before removing it (might be ok)");
          }
          ies.remove();
          break;
        }
      } 
    }
    
    if (editStreams == null || editStreams.size() <= 0) {
      String msg = "Fatal Error: All storage directories are inaccessible.";
      FSNamesystem.LOG.fatal(msg, new IOException(msg)); 
      Runtime.getRuntime().exit(-1);
    }

    // removed failed SDs
    if(propagate && al != null) fsimage.processIOError(al, false);
    
    // TODO roll here, maybe only if propagate true?
    
    lsd = fsimage.listStorageDirectories();
    FSNamesystem.LOG.info("at the end current list of storage dirs:" + lsd);
  }


  /**
   * get an editStream corresponding to a sd
   * @param es - stream to remove
   * @return the matching stream
   */
  StorageDirectory getStorage(EditLogOutputStream es) {
    String parentStorageDir = ((EditLogFileOutputStream)es).getFile()
    .getParentFile().getParentFile().getAbsolutePath();

    Iterator<StorageDirectory> it = fsimage.dirIterator(); 
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      FSNamesystem.LOG.info("comparing: " + parentStorageDir + " and " + sd.getRoot().getAbsolutePath()); 
      if (parentStorageDir.equals(sd.getRoot().getAbsolutePath()))
        return sd;
    }
    return null;
  }
  
  /**
   * get an editStream corresponding to a sd
   * @param sd
   * @return the matching stream
   */
  synchronized EditLogOutputStream getEditsStream(StorageDirectory sd) {
    for (EditLogOutputStream es : editStreams) {
      File parentStorageDir = ((EditLogFileOutputStream)es).getFile()
        .getParentFile().getParentFile();
      if (parentStorageDir.getName().equals(sd.getRoot().getName()))
        return es;
    }
    return null;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  void logEdit(byte op, Writable ... writables) {
    synchronized (this) {
      // wait if an automatic sync is scheduled
      waitIfAutoSyncScheduled();
      
      if(getNumEditStreams() == 0)
        throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
      ArrayList<EditLogOutputStream> errorStreams = null;
      long start = now();
      for(EditLogOutputStream eStream : editStreams) {
        if(FSImage.LOG.isDebugEnabled()) {
          FSImage.LOG.debug("loggin edits into " + eStream.getName() +
              " stream");
        }
        if(!eStream.isOperationSupported(op))
          continue;
        try {
          eStream.write(op, writables);
        } catch (IOException ie) {
          FSImage.LOG.warn("logEdit: removing "+ eStream.getName(), ie);
          if(errorStreams == null)
            errorStreams = new ArrayList<EditLogOutputStream>(1);
          errorStreams.add(eStream);
        }
      }
      processIOError(errorStreams, true);
      recordTransaction(start);
      
      // check if it is time to schedule an automatic sync
      if (!shouldForceSync()) {
        return;
      }
      isAutoSyncScheduled = true;
    }
    
    // sync buffered edit log entries to persistent store
    logSync();
  }

  /**
   * Wait if an automatic sync is scheduled
   * @throws InterruptedException
   */
  synchronized void waitIfAutoSyncScheduled() {
    try {
      while (isAutoSyncScheduled) {
        this.wait(1000);
      }
    } catch (InterruptedException e) {
    }
  }
  
  /**
   * Signal that an automatic sync scheduling is done if it is scheduled
   */
  synchronized void doneWithAutoSyncScheduling() {
    if (isAutoSyncScheduled) {
      isAutoSyncScheduled = false;
      notifyAll();
    }
  }
  
  /**
   * Check if should automatically sync buffered edits to 
   * persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    for (EditLogOutputStream eStream : editStreams) {
      if (eStream.shouldForceSync()) {
        return true;
      }
    }
    return false;
  }
  
  private void recordTransaction(long start) {
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;

    // update statistics
    long end = now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.transactions.inc((end-start));
  }

  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() throws IOException {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   */
  public void logSync() {
    ArrayList<EditLogOutputStream> errorStreams = null;
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;
    ArrayList<EditLogOutputStream> streams = new ArrayList<EditLogOutputStream>();
    boolean sync = false;
    try {
      synchronized (this) {
        try {
        assert editStreams.size() > 0 : "no editlog streams";
        printStatistics(false);
  
        // if somebody is already syncing, then wait
        while (mytxid > synctxid && isSyncRunning) {
          try {
            wait(1000);
          } catch (InterruptedException ie) { 
          }
        }
  
        //
        // If this transaction was already flushed, then nothing to do
        //
        if (mytxid <= synctxid) {
          numTransactionsBatchedInSync++;
          if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.transactionsBatchedInSync.inc();
          return;
        }
     
        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        sync = true;
  
        // swap buffers
        for(EditLogOutputStream eStream : editStreams) {
          try {
            eStream.setReadyToFlush();
            streams.add(eStream);
          } catch (IOException ie) {
            FSNamesystem.LOG.error("Unable to get ready to flush.", ie);
            //
            // remember the streams that encountered an error.
            //
            if (errorStreams == null) {
              errorStreams = new ArrayList<EditLogOutputStream>(1);
            }
            errorStreams.add(eStream);
          }
        }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
      }
  
      // do the sync
      long start = now();
      for (EditLogOutputStream eStream : streams) {
        try {
          eStream.flush();
        } catch (IOException ie) {
          FSNamesystem.LOG.error("Unable to sync edit log.", ie);
          //
          // remember the streams that encountered an error.
          //
          if (errorStreams == null) {
            errorStreams = new ArrayList<EditLogOutputStream>(1);
          }
          errorStreams.add(eStream);
        }
      }
      long elapsed = now() - start;
      processIOError(errorStreams, true);
  
      if (metrics != null) // Metrics non-null only when used inside name node
        metrics.syncs.inc(elapsed);
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 
      synchronized (this) {
        synctxid = syncStart;
        if (sync) {
          isSyncRunning = false;
        }
        this.notifyAll();
     }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (editStreams == null || editStreams.size()==0) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append("Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editStreams.get(0).getNumSync());
    buf.append(" SyncTimes(ms): ");

    int numEditStreams = editStreams.size();
    for (int idx = 0; idx < numEditStreams; idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    FSNamesystem.LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) {

    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(Ops.OP_ADD,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus(),
            new DeprecatedUTF8(newNode.getClientName()),
            new DeprecatedUTF8(newNode.getClientMachine()));
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(Ops.OP_CLOSE,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime())
    };
    logEdit(Ops.OP_MKDIR, new ArrayWritable(DeprecatedUTF8.class, info),
        newNode.getPermissionStatus());
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(Ops.OP_RENAME_OLD, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, Options.Rename... options) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(Ops.OP_RENAME, new ArrayWritable(DeprecatedUTF8.class, info),
        toBytesWritable(options));
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(Ops.OP_SET_REPLICATION, 
            new DeprecatedUTF8(src), 
            FSEditLog.toLogReplication(replication));
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    logEdit(Ops.OP_SET_QUOTA, new DeprecatedUTF8(src), 
            new LongWritable(nsQuota), new LongWritable(dsQuota));
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    logEdit(Ops.OP_SET_PERMISSIONS, new DeprecatedUTF8(src), permissions);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    DeprecatedUTF8 u = new DeprecatedUTF8(username == null? "": username);
    DeprecatedUTF8 g = new DeprecatedUTF8(groupname == null? "": groupname);
    logEdit(Ops.OP_SET_OWNER, new DeprecatedUTF8(src), u, g);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    int size = 1 + srcs.length + 1; // trg, srcs, timestamp
    DeprecatedUTF8 info[] = new DeprecatedUTF8[size];
    int idx = 0;
    info[idx++] = new DeprecatedUTF8(trg);
    for(int i=0; i<srcs.length; i++) {
      info[idx++] = new DeprecatedUTF8(srcs[i]);
    }
    info[idx] = FSEditLog.toLogLong(timestamp);
    logEdit(Ops.OP_CONCAT_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(timestamp)};
    logEdit(Ops.OP_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add generation stamp record to edit log
   */
  void logGenerationStamp(long genstamp) {
    logEdit(Ops.OP_SET_GENSTAMP, new LongWritable(genstamp));
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(Ops.OP_TIMES, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, 
                  long atime, INodeSymlink node) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path),
      new DeprecatedUTF8(value),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(Ops.OP_SYMLINK, 
            new ArrayWritable(DeprecatedUTF8.class, info),
            node.getPermissionStatus());
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   * @return
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(Ops.OP_GET_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(Ops.OP_RENEW_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    logEdit(Ops.OP_CANCEL_DELEGATION_TOKEN, id);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    logEdit(Ops.OP_UPDATE_MASTER_KEY, key);
  }
  
  static private DeprecatedUTF8 toLogReplication(short replication) {
    return new DeprecatedUTF8(Short.toString(replication));
  }
  
  static private DeprecatedUTF8 toLogLong(long timestamp) {
    return new DeprecatedUTF8(Long.toString(timestamp));
  }

  /**
   * Return the size of the current EditLog starting with the
   * log at the given index
   *
   * @param startLogIndex the index to start counting from, typically
   * the index of the current image
   */
  synchronized long getTotalEditLogSize(int startLogIndex) throws IOException {
    assert getNumEditsDirs() <= getNumEditStreams() : 
        "Number of edits directories should not exceed the number of streams.";
    long size = 0;

    for (int logIndex = startLogIndex; logIndex < currentLogIndex; logIndex++) {
      size += getSingleEditLogSize(logIndex);
    }
    size += getCurrentEditLogSize();
    return size;
  }

  private synchronized long getSingleEditLogSize(int logIndex) throws IOException {
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    if(!it.hasNext()) 
      throw new IOException("No storage directories!");

    long size = 0;
    boolean gotValid = false;
    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      File f = FSImage.getFinalizedEditsFile(sd, logIndex);
      if (f.exists()) {
        long curSize = f.length();
        assert (size == 0 || size == curSize || curSize ==0) :
        "Wrong streams size";
        size = Math.max(size, curSize);
        gotValid = true;
      }
    }

    if (!gotValid) {
      throw new IOException("No valid edit logs for edits index " + logIndex);
    }

    return size;
  }

  /**
   * Return the size in bytes of the edit log currently being written
   */
  private synchronized long getCurrentEditLogSize() {
    ArrayList<EditLogOutputStream> al = null;
    long size = 0;
    for (int idx = 0; idx < getNumEditStreams(); idx++) {
      EditLogOutputStream es = editStreams.get(idx);
      try {
        long curSize = es.length();
        assert (size == 0 || size == curSize || curSize ==0) :
          "Wrong streams size";
        size = Math.max(size, curSize);
      } catch (IOException e) {
        FSImage.LOG.warn("getCurrentEditLogSize: editstream.length failed. " +
                         "Removing editlog (" + idx + ") " + es.getName());
        if(al==null) al = new ArrayList<EditLogOutputStream>(1);
        al.add(es);
      }
    }
    if(al!=null) processIOError(al, true);
    return size;
  }
  
  /**
   * Closes the current edit log and opens a new one.
   * Returns the index of the new edit log being written
   * @param expectedNewIndex the index to roll to - used as a safeguard
   * that the image agrees with the edit log about current index
   */
  synchronized void rollEditLog(int expectedNewIndex) throws IOException {
    waitForSyncToFinish();
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    if(!it.hasNext()) 
      throw new IOException("No storage directories!");

    if (FSImage.LOG.isDebugEnabled()) {
      FSImage.LOG.debug("Rolling edit log, current index = " + currentLogIndex + ", next expected index = " + expectedNewIndex);
    }
    int nextIndex = currentLogIndex + 1;
    if (expectedNewIndex != nextIndex) {
      throw new IOException("Image wanted to roll to edit #" + expectedNewIndex +
          " but edit log expected index " + nextIndex);
    }
    // Check that we don't already have an edit log with the new index
    // in any of our directories. This is all a sanity check and could be
    // turned into assertions if it becomes a bottleneck.

    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      // Current finalized file should not exist
      if (FSImage.existsFinalizedEditsFile(sd, currentLogIndex)) {
        throw new IOException(FSImage.getFinalizedEditsFile(sd, currentLogIndex) + "should not exist");
      }
      // New finalized file should not exist
      if (FSImage.existsFinalizedEditsFile(sd, nextIndex)) {
        throw new IOException(FSImage.getFinalizedEditsFile(sd, nextIndex) + "should not exist");
      }
      // New in progress file should not exist
      if (FSImage.existsInProgressEditsFile(sd, nextIndex)) {
        throw new IOException(FSImage.getInProgressEditsFile(sd, nextIndex) + "should not exist");
      }
    }

    // check if any of failed storage is now available and put it back
    fsimage.attemptRestoreRemovedStorage(nextIndex);
    divertFileStreams(currentLogIndex);
    FSImage.LOG.info("====> Telling slaves that we rolled our logs to " + nextIndex);
    logEdit(Ops.OP_ROLL_LOGS, new IntWritable(nextIndex));
    currentLogIndex = nextIndex;
  }

  /**
   * For any edits files that were in progress, try to recover them.
   * The logic is as follows:
   *
   * For each log index:
   * If there are any finalized logs:
   *   delete/move aside any inprogress logs
   * If there are no finalized logs, but there are in progress logs
   *   try to recover the set

   */
  static void recoverInProgressLogs(
      FSImage fsimage,
      int startIndex,
      int endIndex) throws IOException {
    for (int i = startIndex; i <= endIndex; i++) {
      boolean hasInProgress = false;
      boolean hasFinalized = false;

      // Scan all dirs to figure out what state is available
      Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
      while (it.hasNext()) {
        StorageDirectory sd = it.next();

        boolean thisDirHasFinalized = FSImage.existsFinalizedEditsFile(sd, i);
        boolean thisDirHasInProgress = FSImage.existsInProgressEditsFile(sd, i);

        // Sanity check that we have one or the other but not both
        if (thisDirHasFinalized && thisDirHasInProgress) {
          throw new IOException(
            "Bad state in dir " + sd.getRoot() + ": " +
            "both finalized and in progress logs exist for " +
            "index " + i);
        }

        hasInProgress = hasInProgress || thisDirHasInProgress;
        hasFinalized = hasFinalized || thisDirHasFinalized;
      }

      // If we have both finalized and unfinalized, then the finalized ones
      // are correct, and the unfinalized ones are from directories that
      // were failed during the course of writing the log
      if (hasInProgress && hasFinalized) {
        it = fsimage.dirIterator(NameNodeDirType.EDITS);
        while (it.hasNext()) {
          StorageDirectory sd = it.next();
          if (FSImage.existsInProgressEditsFile(sd, i)) {
            markPossiblyCorruptInProgressEditsFile(sd, i);
          }
        }
      }

      // If we have only inprogress, then we can finalize all of them.
      // TODO: here is the opportunity to do consistency checks and
      // synchronize the files (eg in case we crashed while one has synced
      // and another hasn't)
      if (hasInProgress && !hasFinalized) {
        it = fsimage.dirIterator(NameNodeDirType.EDITS);
        while (it.hasNext()) {
          StorageDirectory sd = it.next();
          if (FSImage.existsInProgressEditsFile(sd, i)) {
            finalizeEditsFile(sd, i);
          }
        }
      }

      // If we have neither inprogress nor finalized, then we have no logs
      // for this index, and we must abort
      if (!hasInProgress && !hasFinalized) {
        throw new IOException("Could not recover logs for index " + i +
          ": no finalized or inprogress edits files found in storage dirs!");
      }
    }
  }

  /**
   * Mark an inprogress edit file as possibly corrupt.
   * We should be able to delete these files, but to be extra safe we just move
   * them aside and lazily GC them later.
   */
  private static void markPossiblyCorruptInProgressEditsFile(StorageDirectory sd, int index)
    throws IOException {
    File f = FSImage.getFinalizedEditsFile(sd, index);
    FSImage.LOG.warn("Marking possibly corrupt edits file " + f);

    Storage.rename(f, new File(f.getParentFile(), f.getName() + ".corrupt"));
  }


  private static void finalizeEditsFile(StorageDirectory sd, int index)
    throws IOException
  {
    FSImage.LOG.info("Finalizing edits file #" + index + " in " + sd.getRoot());
    File inProgressFile = FSImage.getInProgressEditsFile(sd, index);
    File finalizedFile = FSImage.getFinalizedEditsFile(sd, index);

    if (finalizedFile.exists()) {
      throw new IOException(
        "Can't finalize file " + inProgressFile +
        " since target " + finalizedFile + " exists");
    }

    if(!inProgressFile.renameTo(finalizedFile)) {
      throw new IOException("Finalization of edits file " + 
                            index + " failed for " + sd.getRoot());
    }
  }
  
  // TODO move these functions into FSImage, so FSEditLog is only doing
  // writing stuff?
  static void finalizeEditsFile(Storage storage, int index)
    throws IOException {
    Iterator<StorageDirectory> itD = 
      storage.dirIterator(NameNodeDirType.EDITS);
    while (itD.hasNext()) {
      StorageDirectory sd = itD.next();
      finalizeEditsFile(sd, index);
    }
  }


  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Divert file streams from file edits to file edits.new.<p>
   * Close file streams, which are currently writing into edits files.
   * Create new streams based on file getRoot()/dest.
   * @param dest new stream path relative to the storage directory root.
   * @throws IOException
   */
  synchronized void divertFileStreams(int oldIndex) throws IOException {
    assert getNumEditStreams() >= getNumEditsDirs() :
      "Inconsistent number of streams";
    waitForSyncToFinish();

    ArrayList<EditLogOutputStream> errorStreams = null;
    EditStreamIterator itE = 
      (EditStreamIterator)getOutputStreamIterator(JournalType.FILE);
    Iterator<StorageDirectory> itD = 
      fsimage.dirIterator(NameNodeDirType.EDITS);
    while(itE.hasNext() && itD.hasNext()) {
      EditLogOutputStream eStream = itE.next();
      StorageDirectory sd = itD.next();

      // Check that the edit stream is pointing to a file inside the
      // storage directory.
      if(!eStream.getName().startsWith(sd.getRoot().getPath())) {
        throw new IOException("Inconsistent order of edit streams: " +
          "log " + eStream.getName() + " does not start with root " +
          sd.getRoot().getPath());
      }

      try {
        // close old stream
        closeStream(eStream);

        finalizeEditsFile(sd, oldIndex);

        // create new stream
        eStream = new EditLogFileOutputStream(
          FSImage.getInProgressEditsFile(sd, oldIndex + 1),
          sizeOutputFlushBuffer);
        eStream.create();
        // replace by the new stream
        itE.replace(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Error in editStream " + eStream.getName(), e);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
  }

  /**
   * Return the name of the edit file
   */
  @Deprecated
  synchronized File getFsEditName() {
    throw new RuntimeException("TODO");
  }

  /**
   * Returns the timestamp of the edit log
   * TODO rename me
   */
  synchronized long getFsEditTime() {
    return currentLogIndex;
  }

  // sets the initial capacity of the flush buffer.
  public void setBufferCapacity(int size) {
    sizeOutputFlushBuffer = size;
  }

  boolean isEmpty() throws IOException {
    // TODO used? wouldn't this always be at least 4? (hdr len)
    // is Current really what we want? in what context is this used
    return getCurrentEditLogSize() == 0;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  synchronized void logEdit(int length, byte[] data) {
    if(getNumEditStreams() == 0)
      throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
    ArrayList<EditLogOutputStream> errorStreams = null;
    long start = now();
    for(EditLogOutputStream eStream : editStreams) {
      try {
        eStream.write(data, 0, length);
      } catch (IOException ie) {
        FSNamesystem.LOG.warn("Error in editStream " + eStream.getName(), ie);
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
      }
    }
    processIOError(errorStreams, true);
    recordTransaction(start);
  }

  /**
   * Iterates output streams based of the same type.
   * Type null will iterate over all streams.
   */
  private class EditStreamIterator implements Iterator<EditLogOutputStream> {
    JournalType type;
    int prevIndex; // for remove()
    int nextIndex; // for next()

    EditStreamIterator(JournalType streamType) {
      this.type = streamType;
      this.nextIndex = 0;
      this.prevIndex = 0;
    }

    public boolean hasNext() {
      synchronized(FSEditLog.this) {
        if(editStreams == null || 
           editStreams.isEmpty() || nextIndex >= editStreams.size())
          return false;
        while(nextIndex < editStreams.size()
              && !editStreams.get(nextIndex).getType().isOfType(type))
          nextIndex++;
        return nextIndex < editStreams.size();
      }
    }

    public EditLogOutputStream next() {
      EditLogOutputStream stream = null;
      synchronized(FSEditLog.this) {
        stream = editStreams.get(nextIndex);
        prevIndex = nextIndex;
        nextIndex++;
        while(nextIndex < editStreams.size()
            && !editStreams.get(nextIndex).getType().isOfType(type))
        nextIndex++;
      }
      return stream;
    }

    public void remove() {
      nextIndex = prevIndex; // restore previous state
      removeStream(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }

    void replace(EditLogOutputStream newStream) {
      synchronized (FSEditLog.this) {
        assert 0 <= prevIndex && prevIndex < editStreams.size() :
                                                          "Index out of bound.";
        editStreams.set(prevIndex, newStream);
      }
    }
  }

  /**
   * Get stream iterator for the specified type.
   */
  public Iterator<EditLogOutputStream>
  getOutputStreamIterator(JournalType streamType) {
    return new EditStreamIterator(streamType);
  }

  private void closeStream(EditLogOutputStream eStream) throws IOException {
    eStream.setReadyToFlush();
    eStream.flush();
    eStream.close();
  }

  synchronized void releaseBackupStream(NamenodeRegistration registration) {
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    ArrayList<EditLogOutputStream> errorStreams = null;
    NamenodeRegistration backupNode = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
            backupNode.isRole(registration.getRole())) {
        errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        break;
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    processIOError(errorStreams, true);
  }

  /**
   * Return whether a given backup node is allowed to register
   * with the NN. This prevents multiple BNs from registering with
   * the same NN.
   * @return true if the registration should be allowed.
   */
  synchronized boolean checkBackupRegistration(
      NamenodeRegistration registration) {
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    boolean regAllowed = !it.hasNext();
    NamenodeRegistration backupNode = null;
    ArrayList<EditLogOutputStream> errorStreams = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
          backupNode.isRole(registration.getRole())) {
        regAllowed = true; // same node re-registers
        break;
      }
      if(!eStream.isAlive()) {
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        regAllowed = true; // previous backup node failed
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    processIOError(errorStreams, true);
    return regAllowed;
  }
  
  static BytesWritable toBytesWritable(Options.Rename... options) {
    byte[] bytes = new byte[options.length];
    for (int i = 0; i < options.length; i++) {
      bytes[i] = options[i].value();
    }
    return new BytesWritable(bytes);
  }
}
