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
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;

import static org.junit.Assert.*;

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
  public static String getFileMD5(File file) throws Exception {
    String res = new String();
    MessageDigest mD = MessageDigest.getInstance("MD5");
    DataInputStream dis = new DataInputStream(new FileInputStream(file));

    try {
      while(true) {
        mD.update(dis.readByte());
      }
    } catch (EOFException eof) {}

    BigInteger bigInt = new BigInteger(1, mD.digest());
    res = bigInt.toString(16);
    dis.close();

    return res;
  }

  /**
   * Assert that all of the given directories have the same newest fsimage_N
   * index, and that they hold the same data.
   */
  public static void assertSameNewestImage(File... dirs) throws Exception {
	if (dirs.length < 2) return;
	
	int imageIndex = -1;
	
	List<File> imageFiles = new ArrayList<File>();
	for (File dir : dirs) {
	  FSImageStorageInspector inspector = new FSImageStorageInspector();
	  inspector.inspectImageDir(dir);
	  
	  try {
		int thisIndex = inspector.getLatestImageIndex();
		if (imageIndex != -1 && thisIndex != imageIndex) {
		  fail("Storage directory " + dir + " does not have the same " +
			  "last image index " + imageIndex + " as another");
		}
		imageIndex = thisIndex;
		imageFiles.add(
			new File(dir,
				FSImage.getImageFileName(NameNodeFile.IMAGE, imageIndex)));
	  } catch (FileNotFoundException fnfe) {
		fail("Storage directory " + dir + " holds no image files");
	  }
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
  public static void assertParallelFilesAreIdentical(File... dirs) throws Exception {
	HashMap<String, List<File>> groupedByName = new HashMap<String, List<File>>();
	for (File dir : dirs) {
	  for (File f : dir.listFiles()) {
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
	
	for (File f : files) {
	  assertTrue("File " + f + " should exist", f.exists());
	}
	String expectedMd5 = null;
	for (File f : files) {
	  String md5 = getFileMD5(f);
	  if (expectedMd5 != null) {
		assertEquals("File " + f + " does not match expected md5",
			expectedMd5, md5);
	  }
	  expectedMd5 = md5;
	}	
  }
}
