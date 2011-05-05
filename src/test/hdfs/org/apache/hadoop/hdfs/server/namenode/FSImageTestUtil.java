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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundFSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.MD5Hash;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Utility functions for testing fsimage storage.
 */
public abstract class FSImageTestUtil {
  
  
  /**
   * This function returns a md5 hash of a file.
   * 
   * @param file input file
   * @return The md5 string
   */
  public static String getFileMD5(File file) throws IOException {
    return MD5FileUtils.computeMd5ForFile(file).toString();
  }
  
  public static StorageDirectory mockStorageDirectory(
      File currentDir, NameNodeDirType type) {
    // Mock the StorageDirectory interface to just point to this file
    StorageDirectory sd = Mockito.mock(StorageDirectory.class);
    Mockito.doReturn(type)
      .when(sd).getStorageDirType();
    Mockito.doReturn(currentDir).when(sd).getCurrentDir();
    
    Mockito.doReturn(mockFile(true)).when(sd).getVersionFile();
    Mockito.doReturn(mockFile(false)).when(sd).getPreviousDir();
    return sd;
  }
  
  static File mockFile(boolean exists) {
    File mockFile = mock(File.class);
    doReturn(exists).when(mockFile).exists();
    return mockFile;
  }
  
  public static FSImageTransactionalStorageInspector inspectStorageDirectory(
      File dir, NameNodeDirType dirType) throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(mockStorageDirectory(dir, dirType));
    return inspector;
  }

  /**
   * Assert that all of the given directories have the same newest filename
   * for fsimage that they hold the same data.
   */
  public static void assertSameNewestImage(List<File> dirs) throws Exception {
    if (dirs.size() < 2) return;
    
    long imageTxId = -1;
    
    List<File> imageFiles = new ArrayList<File>();
    for (File dir : dirs) {
      FSImageTransactionalStorageInspector inspector =
        inspectStorageDirectory(dir, NameNodeDirType.IMAGE);
      FoundFSImage latestImage = inspector.getLatestImage();
      assertNotNull("No image in " + dir, latestImage);      
      long thisTxId = latestImage.getTxId();
      if (imageTxId != -1 && thisTxId != imageTxId) {
        fail("Storage directory " + dir + " does not have the same " +
            "last image index " + imageTxId + " as another");
      }
      imageTxId = thisTxId;
      imageFiles.add(inspector.getLatestImage().getFile());
    }
    
    assertFileContentsSame(imageFiles.toArray(new File[0]));
  }
  
  /**
   * Given a list of directories, assert that any files that are named
   * the same thing have the same contents. For example, if a file
   * named "fsimage_1" shows up in more than one directory, then it must
   * be the same.
   * @throws Exception 
   */
  public static void assertParallelFilesAreIdentical(List<File> dirs,
      Set<String> ignoredFileNames) throws Exception {
    HashMap<String, List<File>> groupedByName = new HashMap<String, List<File>>();
    for (File dir : dirs) {
      for (File f : dir.listFiles()) {
        if (ignoredFileNames.contains(f.getName())) {
          continue;
        }
        
        List<File> fileList = groupedByName.get(f.getName());
        if (fileList == null) {
          fileList = new ArrayList<File>();
          groupedByName.put(f.getName(), fileList);
        }
        fileList.add(f);
      }
    }
    
    for (List<File> sameNameList : groupedByName.values()) {
      assertFileContentsSame(sameNameList.toArray(new File[0]));
    }  
  }
  
  /**
   * Assert that all of the given paths have the exact same
   * contents 
   */
  public static void assertFileContentsSame(File... files) throws Exception {
    if (files.length < 2) return;
    
    Map<File, String> md5s = getFileMD5s(files);
    if (Sets.newHashSet(md5s.values()).size() > 1) {
      fail("File contents differed:\n  " +
          Joiner.on("\n  ")
            .withKeyValueSeparator("=")
            .join(md5s));
    }
  }
  
  /**
   * Assert that the given files are not all the same, and in fact that
   * they have <code>expectedUniqueHashes</code> unique contents.
   */
  public static void assertFileContentsDifferent(
      int expectedUniqueHashes,
      File... files) throws Exception
  {
    Map<File, String> md5s = getFileMD5s(files);
    if (Sets.newHashSet(md5s.values()).size() != expectedUniqueHashes) {
      fail("Expected " + expectedUniqueHashes + " different hashes, got:\n  " +
          Joiner.on("\n  ")
            .withKeyValueSeparator("=")
            .join(md5s));
    }
  }
  
  public static Map<File, String> getFileMD5s(File... files) throws Exception {
    Map<File, String> ret = Maps.newHashMap();
    for (File f : files) {
      assertTrue("Must exist: " + f, f.exists());
      ret.put(f, getFileMD5(f));
    }
    return ret;
  }
}
