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

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public interface DataTransferOps {
  public static class OpReadBlock implements Writable {
    public long blockId;
    public long blockGs;
    public long blockOffset;
    public long blockLen;
    public String clientName;
    public BlockAccessToken accessToken;

    public OpReadBlock() {}

    public OpReadBlock(
      long blockId, long blockGs, long blockOffset, long blockLen,
      String clientName, BlockAccessToken accessToken) {

      this.blockId = blockId;
      this.blockGs = blockGs;
      this.blockOffset = blockOffset;
      this.blockLen = blockLen;
      this.clientName = clientName;
      this.accessToken = accessToken;
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(blockId);
      out.writeLong(blockGs);
      out.writeLong(blockOffset);
      out.writeLong(blockLen);
      Text.writeString(out, clientName);
      accessToken.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      blockId = in.readLong();
      blockGs = in.readLong();
      blockOffset = in.readLong();
      blockLen = in.readLong();
      clientName = Text.readString(in);
      accessToken = DataTransferProtocol.Receiver.readAccessToken(in);
    }
  }


  public static class OpWriteBlock implements Writable {
    public long blockId;
    public long blockGs;
    public int pipelineSize;
    public BlockConstructionStage stage;
    public long newGs;
    public long minBytesRcvd;
    public long maxBytesRcvd;
    public String client;
    public DatanodeInfo srcDataNode;
    public DatanodeInfo[] targets;
    public BlockAccessToken accessToken;

    public OpWriteBlock() {}

    public OpWriteBlock(
      long blockId, long blockGs, int pipelineSize, 
      BlockConstructionStage stage, long newGs, long minBytesRcvd,
      long maxBytesRcvd, String client, DatanodeInfo srcDataNode, 
      DatanodeInfo[] targets, BlockAccessToken accessToken) {

      this.blockId = blockId;
      this.blockGs = blockGs;
      this.pipelineSize = pipelineSize;
      this.stage = stage;
      this.newGs = newGs;
      this.minBytesRcvd = minBytesRcvd;
      this.maxBytesRcvd = maxBytesRcvd;
      this.client = client;
      this.srcDataNode = srcDataNode;
      this.targets = targets;
      this.accessToken = accessToken;
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(blockId);
      out.writeLong(blockGs);
      out.writeInt(pipelineSize);
      stage.write(out);
      WritableUtils.writeVLong(out, newGs);
      WritableUtils.writeVLong(out, minBytesRcvd);
      WritableUtils.writeVLong(out, maxBytesRcvd);
      Text.writeString(out, client);

      out.writeBoolean(srcDataNode != null);
      if (srcDataNode != null) {
        srcDataNode.write(out);
      }
      out.writeInt(targets.length - 1);
      for (int i = 1; i < targets.length; i++) {
        targets[i].write(out);
      }

      accessToken.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      blockId = in.readLong();
      blockGs = in.readLong();
      pipelineSize = in.readInt(); // num of datanodes in entire pipeline
      stage = BlockConstructionStage.readFields(in);
      newGs = WritableUtils.readVLong(in);
      minBytesRcvd = WritableUtils.readVLong(in);
      maxBytesRcvd = WritableUtils.readVLong(in);
      client = Text.readString(in); // working on behalf of this client
      srcDataNode = in.readBoolean()? DatanodeInfo.read(in): null;

      int nTargets = in.readInt();
      if (nTargets < 0) {
        throw new IOException("Mislabelled incoming datastream.");
      }
      targets = new DatanodeInfo[nTargets];
      for (int i = 0; i < targets.length; i++) {
        targets[i] = DatanodeInfo.read(in);
      }
      accessToken = DataTransferProtocol.Receiver.readAccessToken(in);
    }
  }

  public static class OpReplaceBlock implements Writable {
    public long blockId;
    public long blockGs;
    public String storageId;
    public DatanodeInfo src;
    public BlockAccessToken accessToken;

    public OpReplaceBlock() {}

    public OpReplaceBlock(
      long blockId, long blockGs, String storageId, DatanodeInfo src,
      BlockAccessToken accessToken) {

      this.blockId = blockId;
      this.blockGs = blockGs;
      this.storageId = storageId;
      this.src = src;
      this.accessToken = accessToken;
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(blockId);
      out.writeLong(blockGs);
      Text.writeString(out, storageId);
      src.write(out);
      accessToken.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      blockId = in.readLong();
      blockGs = in.readLong();
      storageId = Text.readString(in); // read del hint
      src = DatanodeInfo.read(in); // read proxy source
      accessToken = DataTransferProtocol.Receiver.readAccessToken(in);
    }
  }

  public static class OpCopyBlock implements Writable {
    public long blockId;
    public long blockGs;
    public BlockAccessToken accessToken;

    public OpCopyBlock() {}

    public OpCopyBlock(long blockId, long blockGs, BlockAccessToken accessToken) {
      this.blockId = blockId;
      this.blockGs = blockGs;
      this.accessToken = accessToken;
    }

    public void readFields(DataInput in) throws IOException {
      blockId = in.readLong();
      blockGs = in.readLong();
      accessToken = DataTransferProtocol.Receiver.readAccessToken(in);
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(blockId);
      out.writeLong(blockGs);
      accessToken.write(out);
    }

  }
  public static class OpBlockChecksum implements Writable {
    public long blockId;
    public long blockGs;
    public BlockAccessToken accessToken;

    public OpBlockChecksum() {}
    public OpBlockChecksum(
      long blockId, long blockGs, BlockAccessToken accessToken) {

      this.blockId = blockId;
      this.blockGs = blockGs;
      this.accessToken = accessToken;
    }

    public void readFields(DataInput in) throws IOException {
      blockId = in.readLong();
      blockGs = in.readLong();
      accessToken = DataTransferProtocol.Receiver.readAccessToken(in);
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(blockId);
      out.writeLong(blockGs);
      accessToken.write(out);
    }
  }
}