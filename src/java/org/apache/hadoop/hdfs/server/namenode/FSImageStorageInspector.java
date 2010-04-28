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

import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageStorageInspector.class.getName());

  boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  int latestImageIndex = -1;
  int latestEditsIndex = -1;
  int latestInProgressEditsIndex = -1;

  private static final Pattern IMAGE_REGEX = Pattern.compile(
    NameNodeFile.IMAGE.getName() + "_(\\d+)");
  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");


  FSImageStorageInspector() {
  }

  void inspectDirectory(StorageDirectory sd) throws IOException {
    // Was the file just formatted?
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    File currentDir = sd.getCurrentDir();

    // Determine if sd is image, edits or both
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      inspectImageDir(currentDir);
    }
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      inspectEditsDir(currentDir);
    }

    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }


  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  int getLatestImageIndex() throws FileNotFoundException {
    if (latestImageIndex == -1) {
      throw new FileNotFoundException("No image files found");
    }
    return latestImageIndex;
  }
  
  int getLatestEditsIndex(boolean includeInProgress)
    throws FileNotFoundException {
    int ret;
    if (includeInProgress) {
      ret = Math.max(latestInProgressEditsIndex,
          latestEditsIndex);
    } else {
      ret = latestEditsIndex;
    }
    if (ret == -1) {
      throw new FileNotFoundException("No edits found");
    }
    return ret;
  }

  void inspectEditsDir(File dir) throws IOException {
    latestEditsIndex = Math.max(latestEditsIndex,
        findLatestCandidate(dir, EDITS_REGEX));
    latestInProgressEditsIndex = Math.max(latestInProgressEditsIndex,
        findLatestCandidate(dir, EDITS_INPROGRESS_REGEX));
  }
  
  void inspectImageDir(File dir) throws IOException {
    latestImageIndex = Math.max(latestImageIndex,
        findLatestCandidate(dir, IMAGE_REGEX));
  }
  
  

  private int findLatestCandidate(File currentDir, Pattern regex)
    throws IOException {
    int max = -1;
    for (File f : currentDir.listFiles()) {
      Matcher m = regex.matcher(f.getName());
      if (m.matches()) {
        int fileIndex = Integer.parseInt(m.group(1));
        max = Math.max(max, fileIndex);
      }
    }
    return max;
  }
}