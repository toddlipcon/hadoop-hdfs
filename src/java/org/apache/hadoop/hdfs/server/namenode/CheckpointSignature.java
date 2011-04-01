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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
public class CheckpointSignature extends StorageInfo 
                      implements WritableComparable<CheckpointSignature> {
  private static final String FIELD_SEPARATOR = ":";
  MD5Hash imageDigest = null;
  
  long lastCheckpointTxId;
  long curSegmentTxId;

  public CheckpointSignature() {}

  CheckpointSignature(FSImage fsImage) {
    super(fsImage.getStorage());
    
    lastCheckpointTxId = fsImage.getStorage().getCheckpointTxId();
    curSegmentTxId = fsImage.getEditLog().getCurSegmentTxId();
    imageDigest = fsImage.getStorage().getImageDigest();
  }

  CheckpointSignature(String str) {
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == 6 : "Must be 6 fields in CheckpointSignature";
    layoutVersion = Integer.valueOf(fields[0]);
    namespaceID = Integer.valueOf(fields[1]);
    cTime = Long.valueOf(fields[2]);
    lastCheckpointTxId  = Long.valueOf(fields[3]);
    curSegmentTxId  = Long.valueOf(fields[4]);
    imageDigest = new MD5Hash(fields[5]);
  }

  /**
   * Get the MD5 image digest
   * @return the MD5 image digest
   */
  MD5Hash getImageDigest() {
    return imageDigest;
  }

  public String toString() {
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + String.valueOf(lastCheckpointTxId) + FIELD_SEPARATOR
         + String.valueOf(curSegmentTxId) + FIELD_SEPARATOR
         +  imageDigest.toString();
  }

  void validateStorageInfo(FSImage si) throws IOException {
    if(layoutVersion != si.getStorage().layoutVersion
       || namespaceID != si.getStorage().namespaceID 
       || cTime != si.getStorage().cTime
       || !imageDigest.equals(si.getStorage().getImageDigest())) {
      // checkpointTime can change when the image is saved - do not compare
      throw new IOException("Inconsistent checkpoint fields.\n"
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime
          + " ; imageDigest = " + imageDigest
          + ".\nExpecting respectively: "
          + si.getStorage().layoutVersion + "; " 
          + si.getStorage().namespaceID + "; " + si.getStorage().cTime
          + "; " + si.getStorage().getImageDigest());
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(CheckpointSignature o) {
    return ComparisonChain.start()
      .compare(layoutVersion, o.layoutVersion)
      .compare(namespaceID, o.namespaceID)
      .compare(cTime, o.cTime)
      .compare(lastCheckpointTxId, o.lastCheckpointTxId)
      .compare(curSegmentTxId, o.curSegmentTxId)
      .compare(imageDigest, o.imageDigest)
      .result();
  }

  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    return compareTo((CheckpointSignature)o) == 0;
  }

  public int hashCode() {
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ lastCheckpointTxId ^ curSegmentTxId) ^
            imageDigest.hashCode();
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(lastCheckpointTxId);
    out.writeLong(curSegmentTxId);
    imageDigest.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    lastCheckpointTxId = in.readLong();
    curSegmentTxId = in.readLong();
    imageDigest = new MD5Hash();
    imageDigest.readFields(in);
  }
}
