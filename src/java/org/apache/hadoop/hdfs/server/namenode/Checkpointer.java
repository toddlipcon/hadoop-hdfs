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
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.util.Daemon;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;

/**
 * The Checkpointer is responsible for supporting periodic checkpoints 
 * of the HDFS metadata.
 *
 * The Checkpointer is a daemon that periodically wakes up
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * 
 * The start of a checkpoint is triggered by one of the two factors:
 * (1) time or (2) the size of the edits file.
 */
class Checkpointer extends Daemon {
  public static final Log LOG = 
    LogFactory.getLog(Checkpointer.class.getName());

  private BackupNode backupNode;
  volatile boolean shouldRun;
  private long checkpointPeriod;    // in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log

  /**
   * Number of checkpoints successfully made
   */
  private volatile int countSuccessfulCheckpoints;

  private String infoBindAddress;


  private BackupStorage getFSImage() {
    return (BackupStorage)backupNode.getFSImage();
  }

  private NamenodeProtocol getNamenode(){
    return backupNode.namenode;
  }

  /**
   * Create a connection to the primary namenode.
   */
  Checkpointer(Configuration conf, BackupNode bnNode)  throws IOException {
    this.backupNode = bnNode;
    try {
      initialize(conf);
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Initialize checkpoint.
   */
  private void initialize(Configuration conf) throws IOException {
    // Create connection to the namenode.
    shouldRun = true;

    // Initialize other scheduling parameters from the configuration
    checkpointPeriod = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_KEY, 
                                  DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_DEFAULT);

    // Pull out exact http address for posting url to avoid ip aliasing issues
    String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
                                   DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));
    
    HttpServer httpServer = backupNode.httpServer;
    httpServer.setAttribute("name.system.image", getFSImage());
    httpServer.setAttribute("name.conf", conf);
    httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);

    LOG.info("Checkpoint Period : " + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.info("Log Size Trigger  : " + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
  }

  /**
   * Shut down the checkpointer.
   */
  void shutdown() {
    shouldRun = false;
    backupNode.stop();
  }

  //
  // The main work loop
  //
  public void run() {
    // Check the size of the edit log once every 5 minutes.
    long periodMSec = 5 * 60;   // 5 minutes
    if(checkpointPeriod < periodMSec) {
      periodMSec = checkpointPeriod;
    }
    periodMSec *= 1000;

    long lastCheckpointTime = 0;
    if(!backupNode.shouldCheckpointAtStartup())
      lastCheckpointTime = now();
    while(shouldRun) {
      try {
        long now = now();
        boolean shouldCheckpoint = false;
        if(now >= lastCheckpointTime + periodMSec) {
          shouldCheckpoint = true;
        } else {
          long size = getJournalSize();
          if(size >= checkpointSize)
            shouldCheckpoint = true;
        }
        if(shouldCheckpoint) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch(IOException e) {
        LOG.error("Exception in doCheckpoint: ", e);
      } catch(Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ", e);
        shutdown();
        break;
      }
      try {
        Thread.sleep(periodMSec);
      } catch(InterruptedException ie) {
        // do nothing
      }
    }
  }

  private long getJournalSize() throws IOException {
    /*
    // If BACKUP node has been loaded
    // get edits size from the local file. ACTIVE has the same.
    if(backupNode.isRole(NamenodeRole.BACKUP)
        && getFSImage().getEditLog().isOpen())
      return backupNode.journalSize();
    // Go to the ACTIVE node for its size
    return getNamenode().journalSize(backupNode.getRegistration());
    TODO
    */
    LOG.warn("getJournalSize TODO");
    return 0;
  }

  /**
   * Download <code>fsimage</code> file for a given checkpoint.
   */
  private void downloadCheckpoint(CheckpointSignature sig) throws IOException {
    BackupStorage bnImage = getFSImage();

    LOG.info("Downloading checkpoint data for signature: " + sig);
    bnImage.fetchImageIfNecessary(sig.newestImageIndex);
  }

  /**
   * Copy the new image into remote name-node.
   */
  private void uploadCheckpoint(CheckpointSignature sig) throws IOException {
    // Use the exact http addr as specified in config to deal with ip aliasing
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    String fsName = backupNode.nnHttpAddress;
    int httpPort = httpSocAddr.getPort();

    String fileid = "putimage=" +
      (sig.newestFinalizedEditLogIndex + 1) +
      "&port=" + httpPort +
      "&machine=" + infoBindAddress +
      "&token=" + sig.toString();
      LOG.info("Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, (File[])null);
  }

  /**
   * Create a new checkpoint
   */
  synchronized void doCheckpoint() throws IOException {
    long startTime = now();
    BackupStorage bnImage = getFSImage();
    bnImage.stopReplicationOnNextRoll();
    
    // Now roll the edit logs on the server
    // - this will trigger us to start logging immediately to the
    // next edits_inprogress in the new checkpoint dir, but we won't
    // apply them.
    NamenodeCommand cmd = 
      getNamenode().startCheckpoint(backupNode.getRegistration());

    CheckpointCommand cpCmd = null;
    switch(cmd.getAction()) {
      case NamenodeProtocol.ACT_SHUTDOWN:
        shutdown();
        throw new IOException("Name-node " + backupNode.nnRpcAddress
                                           + " requested shutdown.");
      case NamenodeProtocol.ACT_CHECKPOINT:
        cpCmd = (CheckpointCommand)cmd;
        break;
      default:
        throw new IOException("Unsupported NamenodeCommand: "+cmd.getAction());
    }
    // Make sure we've truly stopped replication - in the current design
    // the replication is synchronous, but now is where we need to make sure
    // we've stopped accepting edits to the FSN
    bnImage.waitForReplicationToStop();
    
    CheckpointSignature sig = cpCmd.getSignature();
    assert FSConstants.LAYOUT_VERSION == sig.getLayoutVersion() :
      "Signature should have current layout version. Expected: "
      + FSConstants.LAYOUT_VERSION + " actual "+ sig.getLayoutVersion();

    syncWithCheckpointSignature(sig);
    sig.validateStorageInfo(bnImage);
    bnImage.saveCheckpoint();

    if(cpCmd.needToReturnImage())
      uploadCheckpoint(sig);

    getNamenode().endCheckpoint(backupNode.getRegistration(), sig);

    bnImage.catchupSynchronization();
    backupNode.setRegistration(); // keep registration up to date
    countSuccessfulCheckpoints++;
    LOG.info("Checkpoint completed in "
	     + (now() - startTime)/1000 + " seconds.");
    // TODO        +	" New Image Size: " + bnImage.getFsImageName().length());
  }
  
  void syncWithCheckpointSignature(CheckpointSignature sig) throws IOException {
    BackupStorage bnImage = getFSImage();
    if (bnImage.needsBootstrapping(sig)) {
      LOG.info("Backup image needs bootstrapping - " +
          "resetting and downloading checkpoint");
      backupNode.resetNamespace();
      downloadCheckpoint(sig);
      bnImage.loadFSImage(
          bnImage.getFirstReadableFsImageFile(sig.newestImageIndex));
      bnImage.namesystemReflectsLogsThrough = sig.newestImageIndex - 1;
    }
    assert bnImage.namesystemReflectsLogsThrough >= sig.newestImageIndex - 1;
    
    for (int logIndex = bnImage.namesystemReflectsLogsThrough + 1;
         logIndex <= sig.newestFinalizedEditLogIndex;
         logIndex++) {
      LOG.info("Fetching and applying log index #"+ logIndex);
      bnImage.fetchLogIfNecessary(logIndex);
      bnImage.applyLog(logIndex);
    }
    assert bnImage.namesystemReflectsLogsThrough == sig.newestFinalizedEditLogIndex;
  }

  public int getNumSuccessfulCheckpoints() {
    return countSuccessfulCheckpoints;
  }
}
