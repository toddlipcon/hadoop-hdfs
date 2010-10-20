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
import org.apache.hadoop.io.WritableComparable;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
public class CheckpointSignature extends StorageInfo 
                      implements WritableComparable<CheckpointSignature> {
  private static final String FIELD_SEPARATOR = ":";

  // TODO rename these fields, add some more safeguards
  /** Actually the edits roll index to read up to */
  int newestFinalizedEditLogIndex = -1;

  /** Actually the image index to pull */
  int newestImageIndex = -1;

  public CheckpointSignature() {}

  CheckpointSignature(FSImage fsImage) {
    super(fsImage);
    newestFinalizedEditLogIndex = fsImage.getNewestFinalizedLogIndex();
    newestImageIndex = fsImage.getNewestImageIndex();
  }

  CheckpointSignature(String str) {
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == 5 : "Must be 5 fields in CheckpointSignature";
    layoutVersion = Integer.valueOf(fields[0]);
    namespaceID = Integer.valueOf(fields[1]);
    cTime = Long.valueOf(fields[2]);
    newestFinalizedEditLogIndex = Integer.valueOf(fields[3]);
    newestImageIndex = Integer.valueOf(fields[4]);
  }

  public String toString() {
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + String.valueOf(newestFinalizedEditLogIndex) + FIELD_SEPARATOR
         + String.valueOf(newestImageIndex);
  }

  void validateStorageInfo(FSImage si) throws IOException {
    if(layoutVersion != si.layoutVersion
        || namespaceID != si.namespaceID || cTime != si.cTime) {
      // checkpointTime can change when the image is saved - do not compare
      // TODO still true?
      throw new IOException("Inconsistent checkpoint fields.\n"
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime
          + ".\nExpecting respectively: "
          + si.layoutVersion + "; " + si.namespaceID + "; " + si.cTime);          
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(CheckpointSignature o) {
    return 
      (layoutVersion < o.layoutVersion) ? -1 : 
                  (layoutVersion > o.layoutVersion) ? 1 :
      (namespaceID < o.namespaceID) ? -1 : (namespaceID > o.namespaceID) ? 1 :
      (cTime < o.cTime) ? -1 : (cTime > o.cTime) ? 1 :
      (newestFinalizedEditLogIndex < o.newestFinalizedEditLogIndex) ? -1 : (newestFinalizedEditLogIndex > o.newestFinalizedEditLogIndex) ? 1 :
      (newestImageIndex < o.newestImageIndex) ? -1 : 
                  (newestImageIndex > o.newestImageIndex) ? 1 : 0;
  }

  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    return compareTo((CheckpointSignature)o) == 0;
  }

  public int hashCode() {
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ newestFinalizedEditLogIndex ^ newestImageIndex);
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(newestFinalizedEditLogIndex);
    out.writeInt(newestImageIndex);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    newestFinalizedEditLogIndex = in.readInt();
    newestImageIndex = in.readInt();
  }
}
