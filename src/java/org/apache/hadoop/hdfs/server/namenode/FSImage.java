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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage extends Storage {

  private static final SimpleDateFormat DATE_FORM =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  //
  // The filenames used for storing the images
  //
  enum NameNodeFile {
    IMAGE     ("fsimage"),
    EDITS     ("edits"),
    EDITS_INPROGRESS ("edits_inprogress"),
    IMAGE_NEW ("fsimage.ckpt");
  
    private String fileName = null;
    private NameNodeFile(String name) {this.fileName = name;}
    String getName() {return fileName;}
  }

  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which 
   * stores both fsimage and edits.
   */
  static enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;
    
    public StorageDirType getStorageDirType() {
      return this;
    }
    
    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
  }

  protected FSNamesystem namesystem = null;

  /**
   * The index of the first edit log not included in this FSImage.
   * For example, if an image is created by merging fsimage_0 plus
   * edits_0, edits_1, and edits_2, this value will be set to
   * 3
   */
  protected int mostRecentSavedImageIndex = 0;


  /**
   * The last log index that this namesystem reflects edits from.
   * 
   * In the case of a primary NN, this number should generally be
   * equivalent to the index of the log that's currently being written.
   * During startup, it is initialized based on the fsimage_N that's loaded
   * and as logs are loaded it is incremented.
   * 
   * In the case of a backup NN, it depends on the current state of synchronization.
   * @see BackupStorage.journalState
   */
  protected volatile int namesystemReflectsLogsThrough = -1;


  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;

  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;

  /**
   * list of failed (and thus removed) storages
   */
  protected List<StorageDirectory> removedStorageDirs = new ArrayList<StorageDirectory>();
    
  /**
   * URIs for importing an image from a checkpoint. In the default case,
   * URIs will represent directories. 
   */
  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;
  
  /**
   * Used for saving the image to disk
   */
  static private final FsPermission FILE_PERM = new FsPermission((short)0);
  static private final byte[] PATH_SEPARATOR = DFSUtil.string2Bytes(Path.SEPARATOR);
  
  /**
   */
  FSImage() {
    this((FSNamesystem)null);
  }
  
  FSImage(FSNamesystem ns) {
    super(NodeType.NAME_NODE);
    setFSNamesystem(ns);
  }
  
  /**
   * @throws IOException 
   */
  FSImage(Collection<URI> fsDirs, Collection<URI> fsEditsDirs) 
      throws IOException {
    this();
    setStorageDirectories(fsDirs, fsEditsDirs);
  }

  public FSImage(StorageInfo storageInfo) {
    super(NodeType.NAME_NODE, storageInfo);
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
    setStorageDirectories(dirs, editsDirs);
  }

  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  void setFSNamesystem(FSNamesystem ns) {
    namesystem = ns;
  }

  public void setRestoreFailedStorage(boolean val) {
    LOG.info("set restore failed storage to " + val);
    restoreFailedStorage=val;
  }

  public boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }

  void setStorageDirectories(Collection<URI> fsNameDirs,
                             Collection<URI> fsEditsDirs) throws IOException {
    this.storageDirs = new ArrayList<StorageDirectory>();
    this.removedStorageDirs = new ArrayList<StorageDirectory>();
    
   // Add all name dirs with appropriate NameNodeDirType 
    for (URI dirName : fsNameDirs) {
      checkSchemeConsistency(dirName);
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      // Add to the list of storage directories, only if the 
      // URI is of type file://
      if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) 
          == 0){
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
            dirType));
      }
    }
    
    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      checkSchemeConsistency(dirName);
      // Add to the list of storage directories, only if the 
      // URI is of type file://
      if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase())
          == 0)
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
                    NameNodeDirType.EDITS));
    }
  }
  
  /* 
   * Checks the consistency of a URI, in particular if the scheme 
   * is specified and is supported by a concrete implementation 
   */
  static void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null)
      throw new IOException("Undefined scheme for " + u);
    else {
      try {
        // the scheme should be enumerated as JournalType
        JournalType.valueOf(scheme.toUpperCase());
      } catch (IllegalArgumentException iae){
        throw new IOException("Unknown scheme " + scheme + 
            ". It should correspond to a JournalType enumeration value");
      }
    }
  };
  
  void setCheckpointDirectories(Collection<URI> dirs,
                                Collection<URI> editsDirs) {
    checkpointDirs = dirs;
    checkpointEditsDirs = editsDirs;
  }
  
  static String getImageFileName(NameNodeFile type, int index) {
    assert index >= 0;
    return type.getName() + "_" + index;
  }
  
  static File getImageFile(StorageDirectory sd, NameNodeFile type, int index) {
    return new File(sd.getCurrentDir(), getImageFileName(type, index));
  }
  
  /**
   * Return the first readable image file with the given index from one of the
   * storage directories.
   * 
   * @throws IOException if there are no readable images with this index.
   */
  File getFirstReadableFsImageFile(int index) throws IOException {
    return getFirstReadableFile(NameNodeDirType.IMAGE,
                                NameNodeFile.IMAGE,
                                index);
  }
  
  /**
   * Return the first readable finalized edit file with the given index from one
   * of the storage directories.
   * 
   * @throws IOException if there are no readable logs with this index.
   */
  File getFirstReadableEditsFile(int index) throws IOException {
    return getFirstReadableFile(NameNodeDirType.EDITS,
                                NameNodeFile.EDITS,
                                index);
  }
  
  /**
   * Get the destination paths for a checkpoint upload of the given
   * index. This returns an fsimage.ckpt_N in each of the directories.
   */
  public File[] getImageCheckpointFiles(int index) throws IOException {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
           dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getImageFile(it.next(), NameNodeFile.IMAGE_NEW, index));
    }
    return list.toArray(new File[list.size()]);
  }
  
  /**
   * Return the first readable file of the given type with the given index.
   * @throws IOException if no such files are found, or there was an error.
   */
  protected File getFirstReadableFile(NameNodeDirType dirType, NameNodeFile fileType,
      int index) throws IOException
  {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = dirIterator(dirType); it.hasNext();) {
      sd = it.next();
      if(sd.getRoot().canRead())
        return getImageFile(sd, fileType, index);
    }
    throw new FileNotFoundException("No " + fileType + " found in any of the "
        + dirType + " dirs with index " + index);
  }
  
  List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }
  
  static File getFinalizedEditsFile(StorageDirectory sd, int index) {
    return getImageFile(sd, NameNodeFile.EDITS, index);
  }
  
  static File getInProgressEditsFile(StorageDirectory sd, int index) {
    return getImageFile(sd, NameNodeFile.EDITS_INPROGRESS, index);
  }
  
  static boolean existsFinalizedEditsFile(StorageDirectory sd, int index) {
    return getFinalizedEditsFile(sd, index).exists(); 
  }

  static boolean existsInProgressEditsFile(StorageDirectory sd, int index) {
    return getInProgressEditsFile(sd, index).exists(); 
  }
  
  Collection<File> getFiles(NameNodeFile type, NameNodeDirType dirType, int index) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(getImageFile(it.next(), type, index));
    }
    return list;
  }
  
  Collection<URI> getDirectories(NameNodeDirType dirType) 
      throws IOException {
    ArrayList<URI> list = new ArrayList<URI>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      StorageDirectory sd = it.next();
      try {
        list.add(Util.fileAsURI(sd.getRoot()));
      } catch (IOException e) {
        throw new IOException("Exception while processing " +
            "StorageDirectory " + sd.getRoot(), e);
      }
    }
    return list;
  }
  
  /**
   * Retrieve current directories of type IMAGE
   * @return Collection of URI representing image directories 
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }
  
  /**
   * Retrieve current directories of type EDITS
   * @return Collection of URI representing edits directories 
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
  }
  
  /**
   * Return number of storage directories of the given type.
   * @param dirType directory type
   * @return number of storage directories of type dirType
   */
  int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null)
      return getNumStorageDirs();
    Iterator<StorageDirectory> it = dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next())
      numDirs++;
    return numDirs;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * Note: this does <b>not</b> open the logs for write.
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
    
    setStorageDirectories(dataDirs, editsDirs);
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      dirIterator(); it.hasNext();) {
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
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT)
      throw new IOException("NameNode is not formatted.");
    if (layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      checkVersionUpgradable(layoutVersion);
    }
    if (startOpt != StartupOption.UPGRADE
          && layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION
          && layoutVersion != FSConstants.LAYOUT_VERSION)
        throw new IOException(
           "\nFile system image contains an old layout version " + layoutVersion
         + ".\nAn upgrade to version " + FSConstants.LAYOUT_VERSION
         + " is required.\nPlease restart NameNode with -upgrade option.");
    // check whether distributed upgrade is reguired and/or should be continued
    verifyDistributedUpgradeProgress(startOpt);
  
    // 2. Format unformatted dirs.
    for (Iterator<StorageDirectory> it = 
                     dirIterator(); it.hasNext();) {
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
    
    boolean needToSave = loadFSImage();
    return needToSave;
  }

  private void doUpgrade() throws IOException {
    if(getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
                                               "previous fs state should not exist during upgrade. "
                                               + "Finalize or rollback first.");
    }
  
    this.loadFSImage();
  
    // Do upgrade for each directory
    long oldCTime = this.getCTime();
    this.cTime = now();  // generate new cTime for the state
    int oldLV = this.getLayoutVersion();
    this.layoutVersion = FSConstants.LAYOUT_VERSION;

    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Upgrading image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + this.getLayoutVersion()
               + "; new CTime = " + this.getCTime());
      File curDir = sd.getCurrentDir();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      assert curDir.exists() : "Current directory must exist.";
      assert !prevDir.exists() : "prvious directory must not exist.";
      assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
      assert editLog == null || !editLog.isOpen() : "Edits log must not be open.";
      // rename current to tmp
      rename(curDir, tmpDir);
      // save new image
      if (!curDir.mkdir())
        throw new IOException("Cannot create directory " + curDir);
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE, 0));

      // write version and time files
      sd.write();
      // rename tmp to previous
      rename(tmpDir, prevDir);
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    initializeDistributedUpgrade();

    // reset the logs id
    namesystemReflectsLogsThrough = -1;
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(getFSNamesystem());
    prevState.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = 
                       dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = prevState.new StorageDirectory(sd.getRoot());
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = 
                          dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;
  
      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.getLayoutVersion()
               + "; new CTime = " + prevState.getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      rename(curDir, tmpDir);
      // rename previous to current
      rename(prevDir, curDir);
  
      // delete tmp dir
      deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    verifyDistributedUpgradeProgress(StartupOption.REGULAR);
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
             + (getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + this.getLayoutVersion()
                   + "; cur CTime = " + this.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    rename(prevDir, tmpDir);
    deleteDir(tmpDir);
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
    realImage.setStorageInfo(ckptImage);
    // -1 here because fsimage_7 reflects only up through edits_6
    namesystemReflectsLogsThrough = ckptImage.mostRecentSavedImageIndex - 1;
    fsNamesys.dir.fsImage = realImage;
    saveFSImage(ckptImage.mostRecentSavedImageIndex);
  }
  
  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = 
                          dirIterator(); it.hasNext();) {
      doFinalize(it.next());
    }
  }
  
  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    if (layoutVersion == 0)
      throw new IOException("NameNode directory " 
                            + sd.getRoot() + " is not formatted.");
    String sDUS, sDUV;
    sDUS = props.getProperty("distributedUpgradeState"); 
    sDUV = props.getProperty("distributedUpgradeVersion");
    setDistributedUpgradeState(
        sDUS == null? false : Boolean.parseBoolean(sDUS),
        sDUV == null? getLayoutVersion() : Integer.parseInt(sDUV));
  }
  /**
   * Write last checkpoint time and version file into the storage directory.
   * 
   * The version file should always be written last.
   * Missing or corrupted version file indicates that 
   * the checkpoint is not valid.
   * 
   * @param sd storage directory
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion", Integer.toString(uVersion)); 
    }
  }
  
  
  /**
   * @param sds - array of SDs to process
   * @param propagate - flag, if set - then call corresponding EditLog stream's 
   * processIOError function.
   */
  void processIOError(ArrayList<StorageDirectory> sds, boolean propagate) {
    ArrayList<EditLogOutputStream> al = null;
    for(StorageDirectory sd:sds) {
      // if has a stream assosiated with it - remove it too..
      if (propagate && sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        EditLogOutputStream eStream = editLog.getEditsStream(sd);
        if(al == null) al = new ArrayList<EditLogOutputStream>(1);
        al.add(eStream);
      }
      
      for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
        StorageDirectory sd1 = it.next();
        if (sd.equals(sd1)) {
          //add storage to the removed list
          LOG.warn("FSImage:processIOError: removing storage: "
              + sd.getRoot().getPath());
          try {
            sd1.unlock(); //unlock before removing (in case it will be restored)
          } catch (Exception e) {
            // nothing
          }
          removedStorageDirs.add(sd1);
          it.remove();
          break;
        }
      }
    }
    // if there are some edit log streams to remove    
    if(propagate && al != null) 
      editLog.processIOError(al, false);
    
    // TODO need to do any rolling stuff here? need to thoroughly test this
  }
  
  public FSEditLog getEditLog() {
    return editLog;
  }
  
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      if(sd.getVersionFile().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            oldImageDir + " does not exist.");
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int odlVersion = oldFile.readInt();
      if (odlVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldFile.close();
    }
    return true;
  }
  

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   *
   * Also initializes the edit logs
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
    FSImageStorageInspector inspector = inspectStorage();
    isUpgradeFinalized = inspector.isUpgradeFinalized();
    int lastImageIndex = inspector.getLatestImageIndex();
    int lastLogIndex = inspector.getLatestEditsIndex(true /* include inprogress */);
  
    // Recover any logs that may have been in progress during
    // the shutdown
    LOG.info("Recovering inprogress logs between index " +
             lastImageIndex + " and " + lastLogIndex);
    FSEditLog.recoverInProgressLogs(this, lastImageIndex, lastLogIndex);
  
    // Pick an image to load
    File imageToLoad = getFirstReadableFsImageFile(lastImageIndex);
    assert imageToLoad.exists();
  
    long startTime = now();
    long imageSize = imageToLoad.length();
    
    //
    // Load in bits
    //
  
    boolean needToSave = inspector.needToSave;
  
    // Set our storage info from this particular storage directory.
    // The image file is in storagedir/current/fsimage, so go up
    // two levels to find storagedir.
    new StorageDirectory(imageToLoad.getParentFile().getParentFile()).read();
  
    needToSave |= loadFSImage(imageToLoad);
    mostRecentSavedImageIndex = lastImageIndex;
    
    LOG.info("Image file of size " + imageSize + " loaded in " 
        + (now() - startTime)/1000 + " seconds.");    
  
    // Load all of the edits between the loaded image and the latest.
    List<File> editsToLoad = new ArrayList<File>();
    for (int i = lastImageIndex; i <= lastLogIndex; i++) {
      editsToLoad.add(getFirstReadableEditsFile(i));     
    }
    needToSave |= (loadFSEdits(editsToLoad) > 0);
    
    // We now reflect the logs all the way through the last log we loaded.
    namesystemReflectsLogsThrough = lastLogIndex;
    return needToSave;
  }
  
  /**
   * Open the logs up for writing.
   * This should only be called once, after the image has been fully
   * loaded.
   * @throws IOException if the logs are already open, or the logs cannot
   *   be opened.
   */
  synchronized void openLogs() throws IOException {
    if (editLog != null) {
      throw new IOException("Logs already open!");
    }
    // Since this is called after loading, we should have finalized
    // the file for index namesystemReflectsLogsThrough, so open
    // the next one.
    int toOpen = namesystemReflectsLogsThrough + 1;
    LOG.info("Opening log #" + toOpen);
    editLog = new FSEditLog(this, toOpen);
    editLog.openNewLogs(toOpen);
    // Only increment this variable once we've successfully opened.
    // TODO write test for what happens with startup when one storage dir is bad. 
    namesystemReflectsLogsThrough = toOpen;
  }
  
  /**
   * Run an FSImageStorageInspector across all the storage in this image.
   * This determines which image and edits should be loaded.
   */
  FSImageStorageInspector inspectStorage() throws IOException {
    FSImageStorageInspector inspector = new FSImageStorageInspector();
  
    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      inspector.inspectDirectory(sd);
    }
    
    // TODO test startup with a bad storage dir
    return inspector;
  }

  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  boolean loadFSImage(File curFile) throws IOException {
    assert this.getLayoutVersion() < 0 : "Negative layout version is expected.";
    assert curFile != null : "curFile is null";

    FSNamesystem fsNamesys = getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;

    //
    // Load in bits
    //
    boolean needToSave = true;
    DataInputStream in = new DataInputStream(new BufferedInputStream(
                              new FileInputStream(curFile)));
    try {
      /*
       * Note: Remove any checks for version earlier than 
       * Storage.LAST_UPGRADABLE_LAYOUT_VERSION since we should never get 
       * to here with older images.
       */
      
      /*
       * TODO we need to change format of the image file
       * it should not contain version and namespace fields
       */
      // read image version: first appeared in version -1
      int imgVersion = in.readInt();
      // read namespaceID: first appeared in version -2
      this.namespaceID = in.readInt();

      // read number of files
      long numFiles;
      if (imgVersion <= -16) {
        numFiles = in.readLong();
      } else {
        numFiles = in.readInt();
      }

      this.layoutVersion = imgVersion;
      // read in the last generation stamp.
      if (imgVersion <= -12) {
        long genstamp = in.readLong();
        fsNamesys.setGenerationStamp(genstamp); 
      }

      needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

      // read file info
      short replication = fsNamesys.getDefaultReplication();

      LOG.info("Number of files = " + numFiles);

      byte[][] pathComponents;
      byte[][] parentPath = {{}};
      INodeDirectory parentINode = fsDir.rootDir;
      for (long i = 0; i < numFiles; i++) {
        long modificationTime = 0;
        long atime = 0;
        long blockSize = 0;
        pathComponents = readPathComponents(in);
        replication = in.readShort();
        replication = fsNamesys.adjustReplication(replication);
        modificationTime = in.readLong();
        if (imgVersion <= -17) {
          atime = in.readLong();
        }
        if (imgVersion <= -8) {
          blockSize = in.readLong();
        }
        int numBlocks = in.readInt();
        Block blocks[] = null;

        // for older versions, a blocklist of size 0
        // indicates a directory.
        if ((-9 <= imgVersion && numBlocks > 0) ||
            (imgVersion < -9 && numBlocks >= 0)) {
          blocks = new Block[numBlocks];
          for (int j = 0; j < numBlocks; j++) {
            blocks[j] = new Block();
            if (-14 < imgVersion) {
              blocks[j].set(in.readLong(), in.readLong(), 
                            GenerationStamp.GRANDFATHER_GENERATION_STAMP);
            } else {
              blocks[j].readFields(in);
            }
          }
        }
        // Older versions of HDFS does not store the block size in inode.
        // If the file has more than one block, use the size of the 
        // first block as the blocksize. Otherwise use the default block size.
        //
        if (-8 <= imgVersion && blockSize == 0) {
          if (numBlocks > 1) {
            blockSize = blocks[0].getNumBytes();
          } else {
            long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
            blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
          }
        }
        
        // get quota only when the node is a directory
        long nsQuota = -1L;
        if (imgVersion <= -16 && blocks == null  && numBlocks == -1) {
          nsQuota = in.readLong();
        }
        long dsQuota = -1L;
        if (imgVersion <= -18 && blocks == null && numBlocks == -1) {
          dsQuota = in.readLong();
        }

        // Read the symlink only when the node is a symlink
        String symlink = "";
        if (imgVersion <= -23 && numBlocks == -2) {
          symlink = Text.readString(in);
        }
        
        PermissionStatus permissions = fsNamesys.getUpgradePermission();
        if (imgVersion <= -11) {
          permissions = PermissionStatus.read(in);
        }
        
        if (isRoot(pathComponents)) { // it is the root
          // update the root's attributes
          if (nsQuota != -1 || dsQuota != -1) {
            fsDir.rootDir.setQuota(nsQuota, dsQuota);
          }
          fsDir.rootDir.setModificationTime(modificationTime);
          fsDir.rootDir.setPermissionStatus(permissions);
          continue;
        }
        // check if the new inode belongs to the same parent
        if(!isParent(pathComponents, parentPath)) {
          parentINode = null;
          parentPath = getParent(pathComponents);
        }
        // add new inode
        // without propagating modification time to parent
        parentINode = fsDir.addToParent(pathComponents, parentINode, permissions,
                                        blocks, symlink, replication, modificationTime, 
                                        atime, nsQuota, dsQuota, blockSize, false);
      }
      
      // load datanode info
      this.loadDatanodes(imgVersion, in);

      // load Files Under Construction
      this.loadFilesUnderConstruction(imgVersion, in, fsNamesys);
      
      this.loadSecretManagerState(imgVersion, in, fsNamesys);
      
    } finally {
      in.close();
    }
    
    return needToSave;
  }

  short adjustReplication(short replication) {
    FSNamesystem fsNamesys = getFSNamesystem();
    short minReplication = fsNamesys.getMinReplication();
    if (replication<minReplication) {
      replication = minReplication;
    }
    short maxReplication = fsNamesys.getMaxReplication();
    if (replication>maxReplication) {
      replication = maxReplication;
    }
    return replication;
  }

  /**
   * Return string representing the parent of the given path.
   */
  String getParent(String path) {
    return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
  }
  
  byte[][] getParent(byte[][] path) {
    byte[][] result = new byte[path.length - 1][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new byte[path[i].length];
      System.arraycopy(path[i], 0, result[i], 0, path[i].length);
    }
    return result;
  }

  private boolean isRoot(byte[][] path) {
    return path.length == 1 &&
      path[0] == null;    
  }

  private boolean isParent(byte[][] path, byte[][] parent) {
    if (path == null || parent == null)
      return false;
    if (parent.length == 0 || path.length != parent.length + 1)
      return false;
    boolean isParent = true;
    for (int i = 0; i < parent.length; i++) {
      isParent = isParent && Arrays.equals(path[i], parent[i]); 
    }
    return isParent;
  }


  /**
   * Load edits from a set of files.
   * 
   * @param editsFiles the list of files to load. 
   * @return number of edits loaded
   * 
   * TODO would be nice to move this function to EditLogLoader
   */
  int loadFSEdits(List<File> editsFiles) throws IOException {

    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
    int numEdits = 0;
    
    for (File f : editsFiles) {
      EditLogFileInputStream edits = new EditLogFileInputStream(f);
      numEdits += loader.loadFSEdits(edits);
      edits.close();
    }

    // update the counts.
    getFSNamesystem().dir.updateCountForINodeWithQuota();    

    return numEdits;
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile) throws IOException {
    FSNamesystem fsNamesys = getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    long startTime = now();
    //
    // Write out data
    //
    FileOutputStream fos = new FileOutputStream(newFile);
    DataOutputStream out = new DataOutputStream(
      new BufferedOutputStream(fos));
    try {
      out.writeInt(FSConstants.LAYOUT_VERSION);
      out.writeInt(namespaceID);
      out.writeLong(fsDir.rootDir.numItemsInTree());
      out.writeLong(fsNamesys.getGenerationStamp());
      byte[] byteStore = new byte[4*FSConstants.MAX_PATH_LENGTH];
      ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
      // save the root
      saveINode2Image(strbuf, fsDir.rootDir, out);
      // save the rest of the nodes
      saveImage(strbuf, 0, fsDir.rootDir, out);
      fsNamesys.saveFilesUnderConstruction(out);
      fsNamesys.saveSecretManagerState(out);
      strbuf = null;

      out.flush();
      fos.getChannel().force(true);
    } finally {
      out.close();
    }

    LOG.info("Image file " + newFile + " of size " +
             newFile.length() + " saved in " +
             (now() - startTime)/1000 + " seconds.");
  }
  
  /**
   * Save the current namesystem to the image file in each of the
   * storage directories.
   */
  public void saveFSImage() throws IOException {
    // fsimage_N means we have data from all the logs up to but not including
    // N
    int imageIndexToSave = namesystemReflectsLogsThrough + 1;

    LOG.info("Rolling edits log in saveFSImage to " + imageIndexToSave);
    editLog.rollEditLog(imageIndexToSave);
    
    saveFSImage(imageIndexToSave);
    namesystemReflectsLogsThrough = imageIndexToSave;
  }
  
  
  void saveFSImage(int imageIndex) throws IOException {
    assert imageIndex == namesystemReflectsLogsThrough + 1:
      "Trying to save fsimage_" + imageIndex + " but NS " +
      "reflects logs up to " + namesystemReflectsLogsThrough;
    
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      NameNodeDirType dirType = (NameNodeDirType)sd.getStorageDirType();

      if (dirType.isOfType(NameNodeDirType.IMAGE))
        saveFSImage(getImageFile(sd, NameNodeFile.IMAGE_NEW, imageIndex));
    }
    rollFSImage(imageIndex);
    mostRecentSavedImageIndex = imageIndex;
    writeVersionFiles();
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed(now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.lock();
    try {
      NameNodeDirType dirType = (NameNodeDirType)sd.getStorageDirType();
      if (dirType.isOfType(NameNodeDirType.IMAGE))
        saveFSImage(getImageFile(sd, NameNodeFile.IMAGE, 0));
      if (dirType.isOfType(NameNodeDirType.EDITS))
        FSEditLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS, 0));
      sd.write();
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  public void format() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;

    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
  }

  /*
   * Save one inode's attributes to the image.
   */
  private static void saveINode2Image(ByteBuffer name,
                                      INode node,
                                      DataOutputStream out) throws IOException {
    int nameLen = name.position();
    out.writeShort(nameLen);
    out.write(name.array(), name.arrayOffset(), nameLen);
    if (node.isDirectory()) {
      out.writeShort(0);  // replication
      out.writeLong(node.getModificationTime());
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-1);   // # of blocks
      out.writeLong(node.getNsQuota());
      out.writeLong(node.getDsQuota());
      FILE_PERM.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             FILE_PERM);
    } else if (node.isLink()) {
      out.writeShort(0);  // replication
      out.writeLong(0);   // modification time
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-2);   // # of blocks
      Text.writeString(out, ((INodeSymlink)node).getLinkValue());
      FILE_PERM.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             FILE_PERM);      
    } else {
      INodeFile fileINode = (INodeFile)node;
      out.writeShort(fileINode.getReplication());
      out.writeLong(fileINode.getModificationTime());
      out.writeLong(fileINode.getAccessTime());
      out.writeLong(fileINode.getPreferredBlockSize());
      Block[] blocks = fileINode.getBlocks();
      out.writeInt(blocks.length);
      for (Block blk : blocks)
        blk.write(out);
      FILE_PERM.fromShort(fileINode.getFsPermissionShort());
      PermissionStatus.write(out, fileINode.getUserName(),
                             fileINode.getGroupName(),
                             FILE_PERM);
    }
  }
  
  /**
   * Save file tree image starting from the given root.
   * This is a recursive procedure, which first saves all children of
   * a current directory and then moves inside the sub-directories.
   */
  private static void saveImage(ByteBuffer parentPrefix,
                                int prefixLength,
                                INodeDirectory current,
                                DataOutputStream out) throws IOException {
    int newPrefixLength = prefixLength;
    if (current.getChildrenRaw() == null)
      return;
    for(INode child : current.getChildren()) {
      // print all children first
      parentPrefix.position(prefixLength);
      parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
      saveINode2Image(parentPrefix, child, out);
    }
    for(INode child : current.getChildren()) {
      if(!child.isDirectory())
        continue;
      parentPrefix.position(prefixLength);
      parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
      newPrefixLength = parentPrefix.position();
      saveImage(parentPrefix, newPrefixLength, (INodeDirectory)child, out);
    }
    parentPrefix.position(prefixLength);
  }

  void loadDatanodes(int version, DataInputStream in) throws IOException {
    if (version > -3) // pre datanode image version
      return;
    if (version <= -12) {
      return; // new versions do not store the datanodes any more.
    }
    int size = in.readInt();
    for(int i = 0; i < size; i++) {
      DatanodeImage nodeImage = new DatanodeImage();
      nodeImage.readFields(in);
      // We don't need to add these descriptors any more.
    }
  }

  private void loadFilesUnderConstruction(int version, DataInputStream in, 
      FSNamesystem fs) throws IOException {
    FSDirectory fsDir = fs.dir;
    if (version > -13) // pre lease image version
      return;
    int size = in.readInt();

    LOG.info("Number of files under construction = " + size);

    for (int i = 0; i < size; i++) {
      INodeFileUnderConstruction cons = readINodeUnderConstruction(in);

      // verify that file exists in namespace
      String path = cons.getLocalName();
      INode old = fsDir.getFileINode(path);
      if (old == null) {
        throw new IOException("Found lease for non-existent file " + path);
      }
      if (old.isDirectory()) {
        throw new IOException("Found lease for directory " + path);
      }
      INodeFile oldnode = (INodeFile) old;
      fsDir.replaceNode(path, oldnode, cons);
      fs.leaseManager.addLease(cons.getClientName(), path); 
    }
  }

  private void loadSecretManagerState(int version,  DataInputStream in, 
      FSNamesystem fs) throws IOException {
    if (version > -23) {
      //SecretManagerState is not available.
      //This must not happen if security is turned on.
      return; 
    }
    fs.loadSecretManagerState(in);
  }
  
  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFileUnderConstruction readINodeUnderConstruction(
                            DataInputStream in) throws IOException {
    byte[] name = readBytes(in);
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    int i = 0;
    for (; i < numBlocks-1; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfo(blk, blockReplication);
    }
    // last block is UNDER_CONSTRUCTION
    if(numBlocks > 0) {
      blk.readFields(in);
      blocks[i] = new BlockInfoUnderConstruction(
        blk, blockReplication, BlockUCState.UNDER_CONSTRUCTION, null);
    }
    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = readString(in);
    String clientMachine = readString(in);

    // These locations are not used at all
    int numLocs = in.readInt();
    DatanodeDescriptor[] locations = new DatanodeDescriptor[numLocs];
    for (i = 0; i < numLocs; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFields(in);
    }

    return new INodeFileUnderConstruction(name, 
                                          blockReplication, 
                                          modificationTime,
                                          preferredBlockSize,
                                          blocks,
                                          perm,
                                          clientName,
                                          clientMachine,
                                          null);
  }

  // Helper function that writes an INodeUnderConstruction
  // into the input stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out,
                                           INodeFileUnderConstruction cons,
                                           String path) 
                                           throws IOException {
    writeString(path, out);
    out.writeShort(cons.getReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());
    int nrBlocks = cons.getBlocks().length;
    out.writeInt(nrBlocks);
    for (int i = 0; i < nrBlocks; i++) {
      cons.getBlocks()[i].write(out);
    }
    cons.getPermissionStatus().write(out);
    writeString(cons.getClientName(), out);
    writeString(cons.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }

  /**
   * Moves fsimage_N.ckpt to fsimage_N
   */
  synchronized void rollFSImage(int indexToRoll) throws IOException {
    // First check that all of the checkpoint uploads are in place
    for (Iterator<StorageDirectory> it = 
           dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW, indexToRoll);
      File dst =  getImageFile(sd, NameNodeFile.IMAGE, indexToRoll);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
      if (dst.exists()) {
        throw new IOException("Checkpoint file " + dst +
                              " already exists");
      }
    }

    // Rename all of them
    ArrayList<StorageDirectory> badDirs = new ArrayList<StorageDirectory>();
    for (Iterator<StorageDirectory> it = 
           dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();

      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW, indexToRoll);
      File dst = getImageFile(sd, NameNodeFile.IMAGE, indexToRoll);
      try {
        rename(ckpt, dst);
      } catch (IOException ioe) {
        badDirs.add(sd);
      }
    }
    if (!badDirs.isEmpty()) processIOError(badDirs, true);

    mostRecentSavedImageIndex = indexToRoll;
  }

  /**
   * Updates version and fstime files in all directories (fsimage and edits).
   */
  void writeVersionFiles() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;

    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        sd.write();
      } catch (IOException e) {
        LOG.error("Cannot write file " + sd.getRoot(), e);

        if (al == null)
          al = new ArrayList<StorageDirectory>(1);
        al.add(sd);
      }
    }
    if (al != null)
      processIOError(al, true);
  }

  synchronized CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog(++namesystemReflectsLogsThrough);
    return new CheckpointSignature(this);
  }

  /**
   * This is called just before a new checkpoint is uploaded to the
   * namenode, and verifies that the checkpoint upload belongs to this
   * same filesystem. (eg that we don't have a 2NN from another HDFS
   * cluster trying to overwrite our image)
   */
  void validateCheckpointUpload(CheckpointSignature sig) throws IOException {
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
    if(bnReg.getNamespaceID() != this.getNamespaceID())
      msg = "Name node " + bnReg.getAddress()
            + " has incompatible namespace id: " + bnReg.getNamespaceID()
            + " expected: " + getNamespaceID();
    else if(bnReg.isRole(NamenodeRole.ACTIVE))
      msg = "Name node " + bnReg.getAddress()
            + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
    else if(bnReg.getLayoutVersion() < this.getLayoutVersion()
        || (bnReg.getLayoutVersion() == this.getLayoutVersion()
            && bnReg.getCTime() > this.getCTime())
        || (bnReg.getLayoutVersion() == this.getLayoutVersion()
            && bnReg.getCTime() == this.getCTime()
            && bnReg.getNewestImageIndex() > this.mostRecentSavedImageIndex))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
            + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
            + " cTime = " + bnReg.getCTime()
            + " newestImageIndex = " + bnReg.getNewestImageIndex()
            + ". Current version: LV = " + getLayoutVersion()
            + " cTime = " + getCTime()
            + " mostRecentSavedImageIndex = " + mostRecentSavedImageIndex;
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean isImgObsolete = true;
    if(bnReg.getLayoutVersion() == this.getLayoutVersion()
        && bnReg.getCTime() == this.getCTime()
        && bnReg.getNewestImageIndex() == this.mostRecentSavedImageIndex)
      isImgObsolete = false;
    boolean needToReturnImg = true;
    if(getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
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
    rollFSImage(sig.newestFinalizedEditLogIndex + 1);
  }

  /**
   * Return the index of the newest fsimage_N file.
   */
  int getNewestImageIndex() {
    return mostRecentSavedImageIndex;
  }

  /**
   * Return the
   * inprogress log!
   */
  int getNewestFinalizedLogIndex() {

    int ret = namesystemReflectsLogsThrough;
    // If we're currently writing a log, then the last finalized
    // log index is one less than what we're currently writing to.
    if (editLog != null && editLog.isOpen()) {
      ret--;
    }
    
    
    
    return ret;
  }

  synchronized void close() throws IOException {
    LOG.info("FSImage closing...");
    if (editLog != null) {
      getEditLog().close();
    }
    unlockAll();
  }

  /**
   * See if any of removed storages iw "writable" again, and can be returned 
   * into service
   */
  synchronized void attemptRestoreRemovedStorage(int editLogIndex) {   
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0) 
      return; //nothing to restore
    
    LOG.info("FSImage.attemptRestoreRemovedStorage: check removed(failed) " +
        "storage. removedStorages size = " + removedStorageDirs.size());
    for(Iterator<StorageDirectory> it = this.removedStorageDirs.iterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File root = sd.getRoot();
      LOG.info("currently disabled dir " + root.getAbsolutePath() + 
          "; type="+sd.getStorageDirType() + ";canwrite="+root.canWrite());
      try {
        
        if(root.exists() && root.canWrite()) { 
          format(sd);
          LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
          if(sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
            editLog.createAndAddEditLogStream(sd, editLogIndex);
          }
          this.addStorageDir(sd); // restore
          it.remove();
        }
      } catch(IOException e) {
        LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(), e);
      }
    }    
  }

  public long getTotalEditLogSize() throws IOException {
    return getEditLog().getTotalEditLogSize(mostRecentSavedImageIndex);
  }


  /**
   * DatanodeImage is used to store persistent information
   * about datanodes into the fsImage.
   */
  static class DatanodeImage implements Writable {
    DatanodeDescriptor node = new DatanodeDescriptor();

    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     * Public method that serializes the information about a
     * Datanode to be stored in the fsImage.
     */
    public void write(DataOutput out) throws IOException {
      new DatanodeID(node).write(out);
      out.writeLong(node.getCapacity());
      out.writeLong(node.getRemaining());
      out.writeLong(node.getLastUpdate());
      out.writeInt(node.getXceiverCount());
    }

    /**
     * Public method that reads a serialized Datanode
     * from the fsImage.
     */
    public void readFields(DataInput in) throws IOException {
      DatanodeID id = new DatanodeID();
      id.readFields(in);
      long capacity = in.readLong();
      long remaining = in.readLong();
      long lastUpdate = in.readLong();
      int xceiverCount = in.readInt();

      // update the DatanodeDescriptor with the data we read in
      node.updateRegInfo(id);
      node.setStorageID(id.getStorageID());
      node.setCapacity(capacity);
      node.setRemaining(remaining);
      node.setLastUpdate(lastUpdate);
      node.setXceiverCount(xceiverCount);
    }
  }

  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldImageDir = new File(rootDir, "image");
    if (!oldImageDir.exists())
      if (!oldImageDir.mkdir())
        throw new IOException("Cannot create directory " + oldImageDir);
    File oldImage = new File(oldImageDir, "fsimage");
    if (!oldImage.exists())
      // recreate old image file to let pre-upgrade versions fail
      if (!oldImage.createNewFile())
        throw new IOException("Cannot create file " + oldImage);
    RandomAccessFile oldFile = new RandomAccessFile(oldImage, "rws");
    // write new version into old image file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }

  private boolean getDistributedUpgradeState() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? false : ns.getDistributedUpgradeState();
  }

  private int getDistributedUpgradeVersion() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? 0 : ns.getDistributedUpgradeVersion();
  }

  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    getFSNamesystem().upgradeManager.setUpgradeState(uState, uVersion);
  }

  private void verifyDistributedUpgradeProgress(StartupOption startOpt
                                                ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;
    UpgradeManager um = getFSNamesystem().upgradeManager;
    assert um != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(um.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(um.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
          + um.getUpgradeVersion() + " to current LV " + FSConstants.LAYOUT_VERSION
          + " is required.\n   Please restart NameNode with -upgrade option.");
    }
  }

  private void initializeDistributedUpgrade() throws IOException {
    UpgradeManagerNamenode um = getFSNamesystem().upgradeManager;
    if(! um.initializeUpgrade())
      return;
    // write new upgrade state into disk
    writeAll();
    NameNode.LOG.info("\n   Distributed upgrade for NameNode version " 
        + um.getUpgradeVersion() + " to current LV " 
        + FSConstants.LAYOUT_VERSION + " is initialized.");
  }

  /**
   * Retrieve checkpoint dirs from configuration.
   *  
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * fs.checkpoint.dir configuration property
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

  static private final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  @SuppressWarnings("deprecation")
  public static String readString(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    return U_STR.toString();
  }

  static String readString_EmptyAsNull(DataInputStream in) throws IOException {
    final String s = readString(in);
    return s.isEmpty()? null: s;
  }
  
  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   * @throws IOException
   */
  public static byte[][] readPathComponents(DataInputStream in)
      throws IOException {
      U_STR.readFields(in);
      return DFSUtil.bytes2byteArray(U_STR.getBytes(),
        U_STR.getLength(), (byte) Path.SEPARATOR_CHAR);
    
  }

  // Same comments apply for this method as for readString()
  @SuppressWarnings("deprecation")
  public static byte[] readBytes(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    int len = U_STR.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(U_STR.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  @SuppressWarnings("deprecation")
  static void writeString(String str, DataOutputStream out) throws IOException {
    U_STR.set(str);
    U_STR.write(out);
  }
}
