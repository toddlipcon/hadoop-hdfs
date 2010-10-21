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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.commons.logging.Log;

/**
 * Responsible for loading the 'fsimage' file that describes the
 * state of a namesystem. This class deserializes the on-disk
 * format and writes the entries into the namesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageLoader {
  private final static Log LOG = FSImage.LOG;

  private final FSImage targetImage;

  /** Set once load has been called once - this is a one-shot class */
  private boolean loadCalled = false;

  FSImageLoader(FSImage targetImage) {
    this.targetImage = targetImage;
  }

  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   *
   * Before this is called, the target image must already have
   * the namespaceID and layoutVersion set. If the data for these
   * fields stored in the image file do not match the fields of
   * the target image, an IOException will be thrown.
   */
  boolean load(File imageFile) throws IOException {
    assert targetImage.getLayoutVersion() < 0 : "Negative layout version is expected.";
    assert imageFile != null : "imageFile is null";
    assert ! loadCalled : "load() may only be called once!";
    loadCalled = true;

    FSNamesystem targetNamesystem = targetImage.getFSNamesystem();
    FSDirectory fsDir = targetNamesystem.dir;

    //
    // Load in bits
    //
    boolean needToSave = true;
    DataInputStream in = new DataInputStream(new BufferedInputStream(
                              new FileInputStream(imageFile)));
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

      // Verify this matches the namespace expected by the target image
      if (imgVersion != targetImage.getLayoutVersion()) {
        throw new IOException(
          "Target image layout version is " + targetImage.getLayoutVersion() + 
          " but file '" + imageFile + "' to be loaded contains data for " +
          "layout version " + imgVersion);
      }

      // read namespaceID: first appeared in version -2
      int namespaceID = in.readInt();

      // Verify this matches the namespace expected by the target image
      if (namespaceID != targetImage.getNamespaceID()) {
        throw new IOException(
          "Target image namespace ID is " + targetImage.getNamespaceID() + 
          " but file '" + imageFile + "' to be loaded contains data for " +
          "namespace ID " + namespaceID);
      }

      // read number of files
      long numFiles;
      if (imgVersion <= -16) {
        numFiles = in.readLong();
      } else {
        numFiles = in.readInt();
      }

      // read in the last generation stamp.
      if (imgVersion <= -12) {
        long genstamp = in.readLong();
        targetNamesystem.setGenerationStamp(genstamp); 
      }

      needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

      // read file info
      short replication = targetNamesystem.getDefaultReplication();

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
        replication = targetNamesystem.adjustReplication(replication);
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
            blockSize = Math.max(targetNamesystem.getDefaultBlockSize(), first);
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
        
        PermissionStatus permissions = targetNamesystem.getUpgradePermission();
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
      this.loadFilesUnderConstruction(imgVersion, in, targetNamesystem);
      
      this.loadSecretManagerState(imgVersion, in, targetNamesystem);
      
    } finally {
      in.close();
    }
    
    return needToSave;
  }

  /**
   * Return string representing the parent of the given path.
   */
  private static String getParent(String path) {
    return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
  }

  private static byte[][] getParent(byte[][] path) {
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


  static private final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
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
  public static byte[] readBytes(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    int len = U_STR.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(U_STR.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  /**
   * DatanodeImage is used to store persistent information
   * about datanodes into the fsImage. This is only used
   * by somewhat ancient versions of Hadoop.
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

}