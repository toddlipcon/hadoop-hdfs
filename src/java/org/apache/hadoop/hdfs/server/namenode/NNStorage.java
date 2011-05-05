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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Properties;
import java.io.RandomAccessFile;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;

import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * NNStorage is responsible for management of the StorageDirectories used by
 * the NameNode.
 */
@InterfaceAudience.Private
public class NNStorage extends Storage implements Closeable {
  private static final Log LOG = LogFactory.getLog(NNStorage.class.getName());

  static final String MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";
  static final String CHECKPOINT_TXID_PROPERTY = "checkpointTxId";

  //
  // The filenames used for storing the images
  //
  enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"),
    SEEN_TXID ("seen_txid"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new"), // from "old" pre-HDFS-1073 format
    EDITS_INPROGRESS ("edits_inprogress");

    private String fileName = null;
    private NameNodeFile(String name) { this.fileName = name; }
    String getName() { return fileName; }
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

  /**
   * Interface to be implemented by classes which make use of storage
   * directories. They are  notified when a StorageDirectory is causing errors,
   * becoming available or being formatted.
   *
   * This allows the implementors of the interface take their own specific
   * action on the StorageDirectory when this occurs.
   */
  interface NNStorageListener {
    /**
     * An error has occurred with a StorageDirectory.
     * @param sd The storage directory causing the error.
     * @throws IOException
     */
    void errorOccurred(StorageDirectory sd) throws IOException;

    /**
     * A storage directory has been formatted.
     * @param sd The storage directory being formatted.
     * @throws IOException
     */
    void formatOccurred(StorageDirectory sd) throws IOException;

    /**
     * A storage directory is now available use.
     * @param sd The storage directory which has become available.
     * @throws IOException
     */
    void directoryAvailable(StorageDirectory sd) throws IOException;
  }

  final private List<NNStorageListener> listeners;
  private UpgradeManager upgradeManager = null;
  protected MD5Hash imageDigest = null;

  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;
  private Object restorationLock = new Object();
  private boolean disablePreUpgradableLayoutCheck = false;


  /**
   * TxId of the last transaction that was included in the most
   * recent fsimage file. This does not include any transactions
   * that have since been written to the edit log.
   */
  protected long checkpointTxId = -1;

  /**
   * list of failed (and thus removed) storages
   */
  final protected List<StorageDirectory> removedStorageDirs
    = new CopyOnWriteArrayList<StorageDirectory>();

  /**
   * Construct the NNStorage.
   * @param conf Namenode configuration.
   */
  public NNStorage(Configuration conf) {
    super(NodeType.NAME_NODE);

    storageDirs = new CopyOnWriteArrayList<StorageDirectory>();
    this.listeners = new CopyOnWriteArrayList<NNStorageListener>();
  }

  @Override // Storage
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    if (disablePreUpgradableLayoutCheck) {
      return false;
    }

    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
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

  @Override // Closeable
  public void close() throws IOException {
    listeners.clear();
    unlockAll();
    storageDirs.clear();
  }

  /**
   * Set flag whether an attempt should be made to restore failed storage
   * directories at the next available oppurtuinity.
   *
   * @param val Whether restoration attempt should be made.
   */
  void setRestoreFailedStorage(boolean val) {
    LOG.warn("set restore failed storage to " + val);
    restoreFailedStorage=val;
  }

  /**
   * @return Whether failed storage directories are to be restored.
   */
  boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }

  /**
   * See if any of removed storages is "writable" again, and can be returned
   * into service. If saveNamespace is set, then this method is being
   * called from saveNamespace.
   *
   * @param saveNamespace Whether method is being called from saveNamespace()
   */
  void attemptRestoreRemovedStorage() {
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0)
      return; //nothing to restore

    /* We don't want more than one thread trying to restore at a time */
    synchronized (this.restorationLock) {
      LOG.info("NNStorage.attemptRestoreRemovedStorage: check removed(failed) "+
               "storarge. removedStorages size = " + removedStorageDirs.size());
      for(Iterator<StorageDirectory> it
            = this.removedStorageDirs.iterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        File root = sd.getRoot();
        LOG.info("currently disabled dir " + root.getAbsolutePath() +
                 "; type="+sd.getStorageDirType() 
                 + ";canwrite="+root.canWrite());
        try {
          
          if(root.exists() && root.canWrite()) {
            // when we try to restore we just need to remove all the data
            // without saving current in-memory state (which could've changed).
            sd.clearDirectory();
            
            LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
            for (NNStorageListener listener : listeners) {
              listener.directoryAvailable(sd);
            }
            
            this.addStorageDir(sd); // restore
            this.removedStorageDirs.remove(sd);
          }
        } catch(IOException e) {
          LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(), e);
        }
      }
    }
  }

  /**
   * @return A list of storage directories which are in the errored state.
   */
  List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }

  /**
   * Set the storage directories which will be used. NNStorage.close() should
   * be called before this to ensure any previous storage directories have been
   * freed.
   *
   * Synchronized due to initialization of storageDirs and removedStorageDirs.
   *
   * @param fsNameDirs Locations to store images.
   * @param fsEditsDirs Locations to store edit logs.
   * @throws IOException
   */
  synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs)
      throws IOException {
    this.storageDirs.clear();
    this.removedStorageDirs.clear();

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

  /**
   * Checks the consistency of a URI, in particular if the scheme
   * is specified and is supported by a concrete implementation
   * @param u URI whose consistency is being checked.
   */
  private static void checkSchemeConsistency(URI u) throws IOException {
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
   * Return the list of locations being used for a specific purpose.
   * i.e. Image or edit log storage.
   *
   * @param dirType Purpose of locations requested.
   * @throws IOException
   */
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
   * Determine the checkpoint time of the specified StorageDirectory
   *
   * @param sd StorageDirectory to check
   * @return If file exists and can be read, last recorded txid. If not, 0L.
   * @throws IOException On errors processing file pointed to by sd
   */
  static long readTransactionIdFile(StorageDirectory sd) throws IOException {
    File txidFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    long txid = 0L;
    if (txidFile.exists() && txidFile.canRead()) {
      BufferedReader br = new BufferedReader(new FileReader(txidFile));
      try {
        txid = Long.valueOf(br.readLine());
      } finally {
        IOUtils.cleanup(LOG, br);
      }
    }
    return txid;
  }
  
  /**
   * Write last checkpoint time into a separate file.
   *
   * @param sd
   * @throws IOException
   */
  void writeTransactionIdFile(StorageDirectory sd, long txid) throws IOException {
    Preconditions.checkArgument(txid >= 0, "bad txid: " + txid);
    
    File txIdFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
    if (txIdFile.exists() && ! txIdFile.delete()) {
        LOG.error("Cannot delete checkpoint time file: "
                  + txIdFile.getCanonicalPath());
    }
    LOG.info("===> writing txid " + txid + " to " + txIdFile);
    FileOutputStream fos = new FileOutputStream(txIdFile);
    try {
      fos.write(String.valueOf(txid).getBytes());
      fos.write('\n');
      fos.flush();
      fos.getChannel().force(true);
    } finally {
      IOUtils.cleanup(LOG, fos);
    }
  }

  /**
   * Set the transaction ID of the last checkpoint
   */
  void setCheckpointTxId(long checkpointTxId) {
    this.checkpointTxId = checkpointTxId;
  }

  /**
   * Return the transaction ID of the last checkpoint.
   */
  long getCheckpointTxId() {
    return checkpointTxId;
  }

  /**
   * Write a small file in all available storage directories that
   * indicates that the namespace has reached some given transaction ID.
   * 
   * This is used when the image is loaded to avoid accidental rollbacks
   * in the case where an edit log is fully deleted but there is no
   * checkpoint. See {@link TestNameEditsConfigs#testNameEditsConfigsFailure()}
   * @param newCpT the txid that has been reached
   */
  public void writeTransactionIdFileToStorage(long txid) {
    // Write txid marker in all storage directories
    for (StorageDirectory sd : storageDirs) {
      try {
        writeTransactionIdFile(sd, txid);
      } catch(IOException e) {
        // Close any edits stream associated with this dir and remove directory
        LOG.warn("writeTransactionIdToStorage failed on " + sd,
            e);
      }
    }
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing
   *
   * @return List of filenames to save checkpoints to.
   */
  public File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it =
                 dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getStorageFile(it.next(), NameNodeFile.IMAGE_NEW));
    }
    return list.toArray(new File[list.size()]);
  }

  /**
   * Return the name of the image file.
   * @return The name of the first image file.
   */
  public File getFsImageName() {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it =
      dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      sd = it.next();
      File fsImage = getStorageFile(sd, NameNodeFile.IMAGE);
      if(sd.getRoot().canRead() && fsImage.exists())
        return fsImage;
    }
    return null;
  }

  /**
   * @return The name of the first editlog file.
   */
  public File getFsEditName() throws IOException {
    for (Iterator<StorageDirectory> it
           = dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      if(sd.getRoot().canRead())
        return getEditFile(sd);
    }
    return null;
  }

  /**
   * @return The name of the first time file.
   */
  public File getFsTimeName() {
    StorageDirectory sd = null;
    // NameNodeFile.TIME shoul be same on all directories
    for (Iterator<StorageDirectory> it =
             dirIterator(); it.hasNext();)
      sd = it.next();
    return getStorageFile(sd, NameNodeFile.TIME);
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  private void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir

    for (NNStorageListener listener : listeners) {
      listener.formatOccurred(sd);
    }
    sd.write();
    writeTransactionIdFile(sd, 0);

    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  /**
   * Format all available storage directories.
   */
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

  /**
   * Move {@code current} to {@code lastcheckpoint.tmp} and
   * recreate empty {@code current}.
   * {@code current} is moved only if it is well formatted,
   * that is contains VERSION file.
   *
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getLastCheckpointTmp()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getPreviousCheckpoint()
   */
  protected void moveCurrent(StorageDirectory sd)
    throws IOException {
    File curDir = sd.getCurrentDir();
    File tmpCkptDir = sd.getLastCheckpointTmp();
    // mv current -> lastcheckpoint.tmp
    // only if current is formatted - has VERSION file
    if(sd.getVersionFile().exists()) {
      assert curDir.exists() : curDir + " directory must exist.";
      assert !tmpCkptDir.exists() : tmpCkptDir + " directory must not exist.";
      rename(curDir, tmpCkptDir);
    }
    // recreate current
    if(!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
  }

  /**
   * Move {@code lastcheckpoint.tmp} to {@code previous.checkpoint}
   *
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getPreviousCheckpoint()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getLastCheckpointTmp()
   */
  protected void moveLastCheckpoint(StorageDirectory sd)
    throws IOException {
    File tmpCkptDir = sd.getLastCheckpointTmp();
    File prevCkptDir = sd.getPreviousCheckpoint();
    // remove previous.checkpoint
    if (prevCkptDir.exists())
      deleteDir(prevCkptDir);
    // mv lastcheckpoint.tmp -> previous.checkpoint
    if(tmpCkptDir.exists())
      rename(tmpCkptDir, prevCkptDir);
  }

  @Override // Storage
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

    String sMd5 = props.getProperty(MESSAGE_DIGEST_PROPERTY);
    if (layoutVersion <= -26) {
      if (sMd5 == null) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "file " + STORAGE_FILE_VERSION
            + " does not have MD5 image digest.");
      }
      this.imageDigest = new MD5Hash(sMd5);
    } else if (sMd5 != null) {
      throw new InconsistentFSStateException(sd.getRoot(),
          "file " + STORAGE_FILE_VERSION +
          " has image MD5 digest when version is " + layoutVersion);
    }

    String sCheckpointId = props.getProperty(CHECKPOINT_TXID_PROPERTY);
    if (layoutVersion <= -28) {
      if (sCheckpointId == null) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "file " + STORAGE_FILE_VERSION
            + " does not have the checkpoint transaction id set.");
      }
      this.checkpointTxId = Long.valueOf(sCheckpointId);
    } else if (sCheckpointId != null) {
      throw new InconsistentFSStateException(sd.getRoot(),
          "file " + STORAGE_FILE_VERSION +
          " has checkpoint transaction id when version is " 
          + layoutVersion);
    }
  }

  /**
   * Write version file into the storage directory.
   *
   * The version file should always be written last.
   * Missing or corrupted version file indicates that
   * the checkpoint is not valid.
   *
   * @param sd storage directory
   * @throws IOException
   */
  @Override // Storage
  protected void setFields(Properties props,
                           StorageDirectory sd
                           ) throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion",
                        Integer.toString(uVersion));
    }
    if (imageDigest == null) {
      imageDigest = MD5Hash.digest(
          new FileInputStream(getStorageFile(sd, NameNodeFile.IMAGE)));
    }

    props.setProperty(MESSAGE_DIGEST_PROPERTY, imageDigest.toString());
    props.setProperty(CHECKPOINT_TXID_PROPERTY, String.valueOf(checkpointTxId));
  }

  /**
   * @return A File of 'type' in storage directory 'sd'.
   */
  static File getStorageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }

  /**
   * @return A editlog File in storage directory 'sd'.
   */
  static File getEditFile(StorageDirectory sd) {
    return getStorageFile(sd, NameNodeFile.EDITS);
  }

  /**
   * @return A temporary editlog File in storage directory 'sd'.
   */
  static File getEditNewFile(StorageDirectory sd) {
    return getStorageFile(sd, NameNodeFile.EDITS_NEW);
  }

  /**
   * @return A list of all Files of 'type' in available storage directories.
   */
  Collection<File> getFiles(NameNodeFile type, NameNodeDirType dirType) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it =
      (dirType == null) ? dirIterator() : dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(getStorageFile(it.next(), type));
    }
    return list;
  }

  /**
   * Set the upgrade manager for use in a distributed upgrade.
   * @param um The upgrade manager
   */
  void setUpgradeManager(UpgradeManager um) {
    upgradeManager = um;
  }

  /**
   * @return The current distribued upgrade state.
   */
  boolean getDistributedUpgradeState() {
    return upgradeManager == null ? false : upgradeManager.getUpgradeState();
  }

  /**
   * @return The current upgrade version.
   */
  int getDistributedUpgradeVersion() {
    return upgradeManager == null ? 0 : upgradeManager.getUpgradeVersion();
  }

  /**
   * Set the upgrade state and version.
   * @param uState the new state.
   * @param uVersion the new version.
   */
  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    upgradeManager.setUpgradeState(uState, uVersion);
  }

  /**
   * Verify that the distributed upgrade state is valid.
   * @param startOpt the option the namenode was started with.
   */
  void verifyDistributedUpgradeProgress(StartupOption startOpt
                                        ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;

    assert upgradeManager != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(upgradeManager.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(upgradeManager.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version "
                              + upgradeManager.getUpgradeVersion()
                              + " to current LV " + FSConstants.LAYOUT_VERSION
                              + " is required.\n   Please restart NameNode"
                              + " with -upgrade option.");
    }
  }

  /**
   * Initialize a distributed upgrade.
   */
  void initializeDistributedUpgrade() throws IOException {
    if(! upgradeManager.initializeUpgrade())
      return;
    // write new upgrade state into disk
    writeAll();
    LOG.info("\n   Distributed upgrade for NameNode version "
             + upgradeManager.getUpgradeVersion() + " to current LV "
             + FSConstants.LAYOUT_VERSION + " is initialized.");
  }

  /**
   * Set the digest for the latest image stored by NNStorage.
   * @param digest The digest for the image.
   */
  void setImageDigest(MD5Hash digest) {
    this.imageDigest = digest;
  }

  /**
   * Get the digest for the latest image storage by NNStorage.
   * @return The digest for the latest image.
   */
  MD5Hash getImageDigest() {
    return imageDigest;
  }

  /**
   * Register a listener. The listener will be notified of changes to the list
   * of available storage directories.
   *
   * @see NNStorageListener
   * @param sel A storage listener.
   */
  void registerListener(NNStorageListener sel) {
    listeners.add(sel);
  }

  /**
   * Disable the check for pre-upgradable layouts. Needed for BackupImage.
   * @param val Whether to disable the preupgradeable layout check.
   */
  void setDisablePreUpgradableLayoutCheck(boolean val) {
    disablePreUpgradableLayoutCheck = val;
  }

  /**
   * Marks a list of directories as having experienced an error.
   *
   * @param sds A list of storage directories to mark as errored.
   * @throws IOException
   */
  void reportErrorsOnDirectories(List<StorageDirectory> sds) throws IOException {
    for (StorageDirectory sd : sds) {
      reportErrorsOnDirectory(sd);
    }
  }

  /**
   * Reports that a directory has experienced an error.
   * Notifies listeners that the directory is no longer
   * available.
   *
   * @param sd A storage directory to mark as errored.
   * @throws IOException
   */
  void reportErrorsOnDirectory(StorageDirectory sd)
      throws IOException {
    LOG.warn("Error reported on storage directory " + sd);

    String lsd = listStorageDirectories();
    LOG.debug("current list of storage dirs:" + lsd);

    for (NNStorageListener listener : listeners) {
      listener.errorOccurred(sd);
    }

    LOG.info("About to remove corresponding storage: "
             + sd.getRoot().getAbsolutePath());
    try {
      sd.unlock();
    } catch (Exception e) {
      LOG.info("Unable to unlock bad storage directory: "
               +  sd.getRoot().getPath(), e);
    }

    if (this.storageDirs.remove(sd)) {
      this.removedStorageDirs.add(sd);
    }
    
    lsd = listStorageDirectories();
    LOG.debug("at the end current list of storage dirs:" + lsd);
  }
}
