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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.commons.logging.Log;

/**
 * Responsible for serializing the current state of an FSImage to disk.
 */
class FSImageWriter {
  private final static Log LOG = FSImage.LOG;

  private final FSImage sourceImage;

  /**
   * Used for saving the image to disk
   */
  static private final FsPermission FILE_PERM = new FsPermission((short)0);
  static private final byte[] PATH_SEPARATOR = DFSUtil.string2Bytes(Path.SEPARATOR);


  FSImageWriter(FSImage sourceImage) {
    this.sourceImage = sourceImage;
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile) throws IOException {
    FSNamesystem sourceNamesystem = sourceImage.getFSNamesystem();
    FSDirectory fsDir = sourceNamesystem.dir;
    long startTime = now();
    //
    // Write out data
    //
    FileOutputStream fos = new FileOutputStream(newFile);
    DataOutputStream out = new DataOutputStream(
      new BufferedOutputStream(fos));
    try {
      out.writeInt(FSConstants.LAYOUT_VERSION);
      out.writeInt(sourceImage.getNamespaceID());
      out.writeLong(fsDir.rootDir.numItemsInTree());
      out.writeLong(sourceNamesystem.getGenerationStamp());
      byte[] byteStore = new byte[4*FSConstants.MAX_PATH_LENGTH];
      ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
      // save the root
      saveINode2Image(strbuf, fsDir.rootDir, out);
      // save the rest of the nodes
      saveImage(strbuf, 0, fsDir.rootDir, out);
      sourceNamesystem.saveFilesUnderConstruction(out);
      sourceNamesystem.saveSecretManagerState(out);
      strbuf = null;

      out.flush();
      fos.getChannel().force(true);
    } finally {
      out.close();
    }

    LOG.info("Image file of size " + newFile.length() + " saved in " 
        + (now() - startTime)/1000 + " seconds.");
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


  static private final DeprecatedUTF8 U_STR = new DeprecatedUTF8();
  static void writeString(String str, DataOutputStream out) throws IOException {
    U_STR.set(str);
    U_STR.write(out);
  }

}