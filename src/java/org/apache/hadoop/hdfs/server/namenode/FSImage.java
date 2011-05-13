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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.LoadPlan;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NNStorageListener;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements NNStorageListener, Closeable {
  protected static final Log LOG = LogFactory.getLog(FSImage.class.getName());

  // checkpoint states
  enum CheckpointStates{START, ROLLED_EDITS, UPLOAD_START, UPLOAD_DONE; }

  protected FSNamesystem namesystem = null;
  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;
  protected MD5Hash newImageDigest = null;

  protected NNStorage storage = null;

  /**
   * URIs for importing an image from a checkpoint. In the default case,
   * URIs will represent directories. 
   */
  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;

  private Configuration conf;

  /**
   * Can fs-image be rolled?
   */
  volatile protected CheckpointStates ckptState = FSImage.CheckpointStates.START; 

  /**
   */
  FSImage() {
    this((FSNamesystem)null);
  }

  /**
   * Constructor
   * @param conf Configuration
   */
  FSImage(Configuration conf) throws IOException {
    this();
    this.conf = conf; // TODO we have too many constructors, this is a mess

    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, 
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      NameNode.LOG.info("set FSImage.restoreFailedStorage");
      storage.setRestoreFailedStorage(true);
    }
    setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
        FSImage.getCheckpointEditsDirs(conf, null));
  }

  private FSImage(FSNamesystem ns) {
    this.conf = new Configuration();
    
    storage = new NNStorage(conf);
    if (ns != null) {
      storage.setUpgradeManager(ns.upgradeManager);
    }
    storage.registerListener(this);

    this.editLog = new FSEditLog(storage);
    setFSNamesystem(ns);
  }

  /**
   * @throws IOException 
   */
  FSImage(Collection<URI> fsDirs, Collection<URI> fsEditsDirs) 
      throws IOException {
    this();
    storage.setStorageDirectories(fsDirs, fsEditsDirs);
  }

  public FSImage(StorageInfo storageInfo, String bpid) {
    storage = new NNStorage(storageInfo, bpid);
  }

  /**
   * Represents an Image (image and edit file).
   * @throws IOException 
   */
  FSImage(URI imageDir) throws IOException {
    this();
    ArrayList<URI> dirs = new ArrayList<URI>(1);
    ArrayList<URI> editsDirs = new ArrayList<URI>(1);
    dirs.add(imageDir);
    editsDirs.add(imageDir);
    storage.setStorageDirectories(dirs, editsDirs);
  }
  
  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  void setFSNamesystem(FSNamesystem ns) {
    namesystem = ns;
    if (ns != null) {
      storage.setUpgradeManager(ns.upgradeManager);
    }
  }
 
  void setCheckpointDirectories(Collection<URI> dirs,
                                Collection<URI> editsDirs) {
    checkpointDirs = dirs;
    checkpointEditsDirs = editsDirs;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param dataDirs
   * @param startOpt startup option
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  boolean recoverTransitionRead(Collection<URI> dataDirs,
                                Collection<URI> editsDirs,
                                StartupOption startOpt)
      throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    // none of the data dirs exist
    if((dataDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    
    if(startOpt == StartupOption.IMPORT 
        && (checkpointDirs == null || checkpointDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );

    if(startOpt == StartupOption.IMPORT 
        && (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    
    storage.setStorageDirectories(dataDirs, editsDirs);
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();

    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      LOG.debug("Data dir states:\n  " +
          Joiner.on("\n  ").withKeyValueSeparator(": ")
          .join(dataDirStates));
      throw new IOException("NameNode is not formatted.");      
    }


    if (storage.getLayoutVersion() < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && storage.getLayoutVersion() < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && storage.getLayoutVersion() != FSConstants.LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + FSConstants.LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with -upgrade option.");
    }
    
    // Upgrade to federation requires -upgrade -clusterid <clusterID> option
    if (startOpt == StartupOption.UPGRADE
        && storage.getLayoutVersion() > Storage.LAST_PRE_FEDERATION_LAYOUT_VERSION) {
      if (startOpt.getClusterId() == null) {
        throw new IOException(
            "\nFile system image contains an old layout version "
                + storage.getLayoutVersion() + ".\nAn upgrade to version "
                + FSConstants.LAYOUT_VERSION
                + " is required.\nPlease restart NameNode with "
                + "-upgrade -clusterid <clusterID> option.");
      }
      storage.setClusterID(startOpt.getClusterId());
      
      // Create new block pool Id
      storage.setBlockPoolID(storage.newBlockPoolID());
    }
    
    // check whether distributed upgrade is reguired and/or should be continued
    storage.verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED:
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        LOG.info("Formatting ...");
        sd.clearDirectory(); // create empty currrent dir
        break;
      default:
        break;
      }
    }

    // 3. Do transitions
    switch(startOpt) {
    case UPGRADE:
      doUpgrade();
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint();
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback();
      break;
    case REGULAR:
      // just load the image
    }
    
    return loadFSImage();
  }
  
  /**
   * For each storage directory, performs recovery of incomplete transitions
   * (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
   * state into the dataDirStates map.
   * @param dataDirStates output of storage directory states
   * @return true if there is at least one valid formatted storage directory
   */
  private boolean recoverStorageDirs(StartupOption startOpt,
      Map<StorageDirectory, StorageState> dataDirStates) throws IOException {
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);      
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          sd.read(); // read and verify consistency with other directories
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    return isFormatted;
  }

  private void doUpgrade() throws IOException {
    if(storage.getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      storage.initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            "previous fs state should not exist during upgrade. "
            + "Finalize or rollback first.");
    }

    // load the latest image
    this.loadFSImage();

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Starting upgrade of image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      try {
        File curDir = sd.getCurrentDir();
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        assert curDir.exists() : "Current directory must exist.";
        assert !prevDir.exists() : "prvious directory must not exist.";
        assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
        assert !editLog.isOpen() : "Edits log must not be open.";

        // rename current to tmp
        NNStorage.rename(curDir, tmpDir);
        
        if (!curDir.mkdir()) {
          throw new IOException("Cannot create directory " + curDir);
        }
      } catch (Exception e) {
        LOG.error("Failed upgrade of image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    storage.reportErrorsOnDirectories(errorSDs);
    errorSDs.clear();

    saveFSImageInAllDirs(editLog.getLastWrittenTxId());

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        sd.write();
        
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        // rename tmp to previous
        NNStorage.rename(tmpDir, prevDir);
      } catch (IOException ioe) {
        LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
        errorSDs.add(sd);
        continue;
      }
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    isUpgradeFinalized = false;
    storage.reportErrorsOnDirectories(errorSDs);
    storage.initializeDistributedUpgrade();
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(getFSNamesystem());
    prevState.getStorage().layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev 
        = prevState.getStorage().new StorageDirectory(sd.getRoot());

      // read and verify consistency of the prev dir
      sdPrev.read(sdPrev.getPreviousVersionFile());
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. None of the storage "
                            + "directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.getStorage().getLayoutVersion()
               + "; new CTime = " + prevState.getStorage().getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      NNStorage.rename(curDir, tmpDir);
      // rename previous to current
      NNStorage.rename(prevDir, curDir);

      // delete tmp dir
      NNStorage.deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    storage.verifyDistributedUpgradeProgress(StartupOption.REGULAR);
  }

  private void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.getRoot() + "."
             + (storage.getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + storage.getLayoutVersion()
                   + "; cur CTime = " + storage.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    NNStorage.rename(prevDir, tmpDir);
    NNStorage.deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
  }

  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @throws IOException
   */
  void doImportCheckpoint() throws IOException {
    FSNamesystem fsNamesys = getFSNamesystem();
    FSImage ckptImage = new FSImage(fsNamesys);
    // replace real image with the checkpoint image
    FSImage realImage = fsNamesys.getFSImage();
    assert realImage == this;
    fsNamesys.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(checkpointDirs, checkpointEditsDirs,
                                              StartupOption.REGULAR);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.getStorage().setStorageInfo(ckptImage.getStorage());
    realImage.getEditLog().setNextTxId(ckptImage.getEditLog().getLastWrittenTxId()+1);

    fsNamesys.dir.fsImage = realImage;
    realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());
    // and save it but keep the same checkpointTime
    saveNamespace();
    getStorage().writeAll();
  }

  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      doFinalize(sd);
    }
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  public FSEditLog getEditLog() {
    return editLog;
  }

  public void openEditLog() throws IOException {
    assert editLog != null : "editLog must be initialized";
    Preconditions.checkState(!editLog.isOpen(),
        "edit log should not yet be open");
    editLog.open();
    storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
  };

  private FSImageStorageInspector inspectStorageDirs() throws IOException {
    int minLayoutVersion = Integer.MAX_VALUE; // the newest
    int maxLayoutVersion = Integer.MIN_VALUE; // the oldest
    
    // First determine what range of layout versions we're going to inspect
    for (Iterator<StorageDirectory> it = storage.dirIterator();
         it.hasNext();) {
      StorageDirectory sd = it.next();
      if (!sd.getVersionFile().exists()) {
        LOG.warn("Storage directory " + sd + " contains no VERSION file. Skipping...");
        continue;
      }
      sd.read(); // sets layoutVersion
      minLayoutVersion = Math.min(minLayoutVersion, storage.getLayoutVersion());
      maxLayoutVersion = Math.max(maxLayoutVersion, storage.getLayoutVersion());
    }
    
    if (minLayoutVersion > maxLayoutVersion) {
      throw new IOException("No storage directories contained VERSION information");
    }
    assert minLayoutVersion <= maxLayoutVersion;
    
    // If we have any storage directories with the new layout version
    // (ie edits_<txnid>) then use the new inspector, which will ignore
    // the old format dirs.
    FSImageStorageInspector inspector;
    if (minLayoutVersion <= FSConstants.FIRST_TXNID_BASED_LAYOUT_VERSION) {
      inspector = new FSImageTransactionalStorageInspector();
      if (maxLayoutVersion > FSConstants.FIRST_TXNID_BASED_LAYOUT_VERSION) {
        LOG.warn("Ignoring one or more storage directories with old layouts");
      }
    } else {
      inspector = new FSImageOldStorageInspector();
    }

    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      inspector.inspectDirectory(sd);
    }

    return inspector;
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * Saving and loading fsimage should never trigger symlink resolution. 
   * The paths that are persisted do not have *intermediate* symlinks 
   * because intermediate symlinks are resolved at the time files, 
   * directories, and symlinks are created. All paths accessed while 
   * loading or saving fsimage should therefore only see symlinks as 
   * the final path component, and the functions called below do not
   * resolve symlinks that are the final path component.
   *
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage() throws IOException {
    FSImageStorageInspector inspector = inspectStorageDirs();
    
    isUpgradeFinalized = inspector.isUpgradeFinalized();
    
    boolean needToSave = inspector.needToSave();
    
    // Plan our load. This will throw if it's impossible to load from the
    // data that's available.
    LoadPlan loadPlan = inspector.createLoadPlan();    
    LOG.debug("Planning to load image using following plan:\n" + loadPlan);

    
    // Recover from previous interrupted checkpoint, if any
    needToSave |= loadPlan.doRecovery();

    //
    // Load in bits
    //
    StorageDirectory sdForProperties =
      loadPlan.getStorageDirectoryForProperties();
    // TODO need to discuss what the correct logic is for determing which
    // storage directory to read properties from
    sdForProperties.read();
    File imageFile = loadPlan.getImageFile();
    MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);

    try {
      loadFSImage(imageFile, expectedMD5);
    } catch (IOException ioe) {
      throw new IOException("Failed to load image from " + loadPlan.getImageFile(), ioe);
    }

    needToSave |= loadEdits(loadPlan.getEditsFiles());

    /* TODO(todd) Need to discuss whether we should force a re-save
     * of the image if one of the edits or images has an old format
     * version. We used to do:
     *
     * needToSave |= (editsVersion != FSConstants.LAYOUT_VERSION 
                    || imageVersion != FSConstants.LAYOUT_VERSION); */
    return needToSave;
  }

  /**
   * Load the specified list of edit files into the image.
   * @return true if the image should be re-saved
   */
  protected boolean loadEdits(List<File> editLogs) throws IOException {
    LOG.debug("About to load edits:\n  " + Joiner.on("\n  ").join(editLogs));
      
    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
    long startingTxId = storage.getCheckpointTxId() + 1;
    int numLoaded = 0;
    // Load latest edits
    for (File edits : editLogs) {
      LOG.debug("Reading " + edits + " expecting start txid #" + startingTxId);
      EditLogFileInputStream editIn = new EditLogFileInputStream(edits);
      int thisNumLoaded = loader.loadFSEdits(editIn, startingTxId);
      startingTxId += thisNumLoaded;
      numLoaded += thisNumLoaded;
      editIn.close();
    }

    // update the counts
    getFSNamesystem().dir.updateCountForINodeWithQuota();    
    
    // update the txid for the edit log
    editLog.setNextTxId(storage.getCheckpointTxId() + numLoaded + 1);

    // If we loaded any edits, need to save.
    return numLoaded > 0;
  }


  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  void loadFSImage(File curFile, MD5Hash expectedMd5) throws IOException {
    FSImageFormat.Loader loader = new FSImageFormat.Loader(
        conf, getFSNamesystem());
    loader.load(curFile);
    namesystem.setBlockPoolId(this.getBlockPoolID());

    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (expectedMd5 != null &&
        !expectedMd5.equals(readImageMd5)) {
      throw new IOException("Image file " + curFile +
          " is corrupt with MD5 checksum of " + readImageMd5 +
          " but expecting " + expectedMd5);
    }
    
    storage.setImageDigest(readImageMd5); // set this fsimage's checksum

    long txId = loader.getLoadedImageTxId();
    storage.setCheckpointTxId(txId);
    editLog.setNextTxId(txId + 1);
  }


  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(StorageDirectory sd, long txid) throws IOException {
    File newFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File dstFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    
    FSImageFormat.Saver saver = new FSImageFormat.Saver();
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    saver.save(newFile, getFSNamesystem(), compression);
    
    MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());
    storage.setImageDigest(saver.getSavedDigest());
    storage.setCheckpointTxId(editLog.getLastWrittenTxId());
  }

  /**
   * FSImageSaver is being run in a separate thread when saving
   * FSImage. There is one thread per each copy of the image.
   *
   * FSImageSaver assumes that it was launched from a thread that holds
   * FSNamesystem lock and waits for the execution of FSImageSaver thread
   * to finish.
   * This way we are guraranteed that the namespace is not being updated
   * while multiple instances of FSImageSaver are traversing it
   * and writing it out.
   */
  private class FSImageSaver implements Runnable {
    private StorageDirectory sd;
    private List<StorageDirectory> errorSDs;
    private final long txid;
    
    FSImageSaver(StorageDirectory sd, List<StorageDirectory> errorSDs, long txid) {
      this.sd = sd;
      this.errorSDs = errorSDs;
      this.txid = txid;
    }
    
    public void run() {
      try {
        saveFSImage(sd, txid);
      } catch (Throwable t) {
        LOG.error("Unable to save image for " + sd.getRoot(), t);
        errorSDs.add(sd);
      }
    }
    
    public String toString() {
      return "FSImageSaver for " + sd.getRoot() +
             " of type " + sd.getStorageDirType();
    }
  }
  
  private void waitForThreads(List<Thread> threads) {
    for (Thread thread : threads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException iex) {
          LOG.error("Caught exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }
  /**
   * Save the contents of the FS image to a new image file in each of the
   * current storage directories.
   */
  void saveNamespace() throws IOException {
    assert editLog != null : "editLog must be initialized";
    storage.attemptRestoreRemovedStorage();

    boolean editLogWasOpen = editLog.isOpen();
    
    if (editLogWasOpen) {
      editLog.endCurrentLogSegment();
    }
    long imageTxId = editLog.getLastWrittenTxId();
    try {
      saveFSImageInAllDirs(imageTxId);
      storage.writeAll(); // TODO is this a good spot for this?
      
    } finally {
      if (editLogWasOpen) {
        editLog.startLogSegment(imageTxId + 1);
        // Take this opportunity to note the current transaction
        storage.writeTransactionIdFileToStorage(imageTxId + 1);
      }
    }
    
  }
  
  protected void saveFSImageInAllDirs(long txid) throws IOException {
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());

    List<Thread> saveThreads = new ArrayList<Thread>();
    // save images into current
    for (Iterator<StorageDirectory> it
           = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      FSImageSaver saver = new FSImageSaver(sd, errorSDs, txid);
      Thread saveThread = new Thread(saver, saver.toString());
      saveThreads.add(saveThread);
      saveThread.start();
    }
    waitForThreads(saveThreads);
    saveThreads.clear();
    storage.reportErrorsOnDirectories(errorSDs);

    renameCheckpoint(txid);    
  }

  /**
   * Renames new image
   */
  private void renameCheckpoint(long txid) throws IOException {
    ArrayList<StorageDirectory> al = null;

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      try {
        renameCheckpointInDir(sd, txid);
      } catch (IOException ioe) {
        LOG.warn("Unable to rename checkpoint in " + sd, ioe);
        if (al == null) {
          al = Lists.newArrayList();
        }
        al.add(sd);
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
  }

  private void renameCheckpointInDir(StorageDirectory sd, long txid)
      throws IOException {
    File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    // renameTo fails on Windows if the destination file 
    // already exists.
    if(LOG.isDebugEnabled()) {
      LOG.debug("renaming  " + ckpt.getAbsolutePath() 
                + " to " + curFile.getAbsolutePath());
    }
    if (!ckpt.renameTo(curFile)) {
      if (!curFile.delete() || !ckpt.renameTo(curFile)) {
        throw new IOException("renaming  " + ckpt.getAbsolutePath() + " to "  + 
            curFile.getAbsolutePath() + " FAILED");
      }
    }    
  }

  CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog();
    // Record this log segment ID in all of the storage directories, so
    // we won't miss this log segment on a restart if the edits directories
    // go missing.
    storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
    return new CheckpointSignature(this);
  }

  /**
   * This is called just before a new checkpoint is uploaded to the
   * namenode.
   */
  void validateCheckpointUpload(CheckpointSignature sig) throws IOException { 
    // verify token
    long expectedTxId = getEditLog().getLastWrittenTxId();
    if (sig.curSegmentTxId != expectedTxId) {
      throw new IOException("Namenode has an edit log corresponding to txid " +
          expectedTxId + " but new checkpoint was created using editlog " +
          "ending at txid " + sig.curSegmentTxId + ". Checkpoint Aborted.");
    }

    sig.validateStorageInfo(this);
  }

  /**
   * Start checkpoint.
   * <p>
   * If backup storage contains image that is newer than or incompatible with 
   * what the active name-node has, then the backup node should shutdown.<br>
   * If the backup image is older than the active one then it should 
   * be discarded and downloaded from the active node.<br>
   * If the images are the same then the backup image will be used as current.
   * 
   * @param bnReg the backup node registration.
   * @param nnReg this (active) name-node registration.
   * @return {@link NamenodeCommand} if backup node should shutdown or
   * {@link CheckpointCommand} prescribing what backup node should 
   *         do with its image.
   * @throws IOException
   */
  NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                  NamenodeRegistration nnReg) // active name-node
  throws IOException {
    String msg = null;
    // Verify that checkpoint is allowed
    if(bnReg.getNamespaceID() != storage.getNamespaceID())
      msg = "Name node " + bnReg.getAddress()
            + " has incompatible namespace id: " + bnReg.getNamespaceID()
            + " expected: " + storage.getNamespaceID();
    else if(bnReg.isRole(NamenodeRole.ACTIVE))
      msg = "Name node " + bnReg.getAddress()
            + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
    else if(bnReg.getLayoutVersion() < storage.getLayoutVersion()
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() > storage.getCTime())
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() == storage.getCTime()
            && bnReg.getCheckpointTxId() > storage.getCheckpointTxId()))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
            + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
            + " cTime = " + bnReg.getCTime()
            + " checkpointTxId = " + bnReg.getCheckpointTxId()
            + ". Current version: LV = " + storage.getLayoutVersion()
            + " cTime = " + storage.getCTime()
            + " checkpointTxId = " + storage.getCheckpointTxId();
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean isImgObsolete = true;
    if(bnReg.getLayoutVersion() == storage.getLayoutVersion()
        && bnReg.getCTime() == storage.getCTime()
        && bnReg.getCheckpointTxId() == storage.getCheckpointTxId())
      isImgObsolete = false;
    boolean needToReturnImg = true;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
    getEditLog().logJSpoolStart(bnReg, nnReg);
    return new CheckpointCommand(sig, isImgObsolete, needToReturnImg);
  }

  /**
   * End checkpoint.
   * <p>
   * Rename uploaded checkpoint to the new image;
   * purge old edits file;
   * rename edits.new to edits;
   * redirect edit log streams to the new edits;
   * update checkpoint time if the remote node is a checkpoint only node.
   * 
   * @param sig
   * @param remoteNNRole
   * @throws IOException
   */
  void endCheckpoint(CheckpointSignature sig,
                     NamenodeRole remoteNNRole) throws IOException {
    sig.validateStorageInfo(this);
  }

  /**
   * This is called when a checkpoint upload finishes successfully.
   * @throws IOException 
   */
  synchronized void checkpointUploadDone(long txid, MD5Hash digest)
  throws IOException {
    renameCheckpoint(txid);
    List<StorageDirectory> badSds = Lists.newArrayList();
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File imageFile = new File(sd.getCurrentDir(),
          NNStorage.getImageFileName(txid)); // TODO make function
      try {
        MD5FileUtils.saveMD5File(imageFile, digest);
      } catch (IOException ioe) {
        badSds.add(sd);
      }
    }
    storage.reportErrorsOnDirectories(badSds);
    
    // So long as this is the newest image available,
    // advertise it as such to other checkpointers
    // from now on
    if (txid > storage.getCheckpointTxId()) {
      storage.setCheckpointTxId(txid);
      storage.setImageDigest(digest);
    }
  }

  synchronized public void close() throws IOException {
    getEditLog().close();
    storage.close();
  }


  /**
   * Retrieve checkpoint dirs from configuration.
   *
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * dfs.namenode.checkpoint.dir configuration property
   */
  static Collection<URI> getCheckpointDirs(Configuration conf,
      String defaultValue) {
    Collection<String> dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    if (dirNames.size() == 0 && defaultValue != null) {
      dirNames.add(defaultValue);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  static Collection<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = 
      conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public NNStorage getStorage() {
    return storage;
  }

  @Override // NNStorageListener
  public void errorOccurred(StorageDirectory sd) throws IOException {
    // do nothing,
  }

  
  @Override // NNStorageListener
  public void formatOccurred(StorageDirectory sd) throws IOException {
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      sd.lock();
      try {
        // TODO what happens if you add a new storage dir to an already existing
        // namespace?
        saveFSImage(sd, 0);
        renameCheckpointInDir(sd, 0);
      } finally {
        sd.unlock();
      }
      LOG.info("Storage directory " + sd.getRoot()
               + " has been successfully formatted.");
    }
  }

  public int getLayoutVersion() {
    return storage.getLayoutVersion();
  }
  
  public int getNamespaceID() {
    return storage.getNamespaceID();
  }
  
  public String getClusterID() {
    return storage.getClusterID();
  }
  
  public String getBlockPoolID() {
    return storage.getBlockPoolID();
  }
}
