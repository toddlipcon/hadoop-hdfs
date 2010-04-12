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

  TreeSet<CandidateFile> imageCandidates = new TreeSet<CandidateFile>(ROLL_IDX_COMPARATOR);
  TreeSet<CandidateFile> editsCandidates = new TreeSet<CandidateFile>(ROLL_IDX_COMPARATOR);

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
  
  int getLatestImageIndex() {
    CandidateFile lastImage = imageCandidates.last();
    return lastImage.rollIndex; // TODO check null
  }

  File getLatestImageFile() {
    return imageCandidates.last().file;
  }
  
  int getLatestEditsIndex() {
    if (editsCandidates.isEmpty()) {
      return getLatestImageIndex() - 1;
    }
    CandidateFile lastLog = editsCandidates.last();
    return lastLog.rollIndex;
  }

  File getLatestEditsFile() {
    return editsCandidates.last().file;
  }

  
  void inspectEditsDir(File dir) throws IOException {
    findCandidates(dir, EDITS_REGEX, editsCandidates);
    findCandidates(dir, EDITS_INPROGRESS_REGEX, editsCandidates);
  }
  
  void inspectImageDir(File dir) throws IOException {
    findCandidates(dir, IMAGE_REGEX, imageCandidates);
  }
  
  

  private void findCandidates(
    File currentDir, Pattern regex, Collection<CandidateFile> candidates)
    throws IOException
  {
    for (File f : currentDir.listFiles()) {
      Matcher m = regex.matcher(f.getName());
      if (m.matches()) {
        int fileIndex = Integer.parseInt(m.group(1));
        candidates.add(new CandidateFile(f, fileIndex));
      }
    }
  }

  /**
   * A particular metadata file within a storage directory
   */
  private static class CandidateFile {
    File file;
    int rollIndex;

    public CandidateFile(File file, int rollIndex) {
      this.file = file;
      this.rollIndex = rollIndex;
    }

    public String toString() {
      return "CandidateFile(file=" + file + ", index=" + rollIndex + ")";
    }
  }

  private static final Comparator<CandidateFile> ROLL_IDX_COMPARATOR =
    new CompareByRollIndex();

  private static class CompareByRollIndex implements Comparator<CandidateFile> {
    public int compare(CandidateFile a, CandidateFile b) {
      if (a.rollIndex < b.rollIndex)
        return -1;
      else if (a.rollIndex == b.rollIndex)
        return 0;
      else
        return 1;
    }
  }
}