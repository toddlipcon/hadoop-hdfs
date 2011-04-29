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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.junit.Test;

public class TestEditLogFileOutputStream {
  
  private final static int HEADER_LEN = 17;
  private final static int MKDIR_LEN = 59;

  @Test
  public void testPreallocation() throws IOException {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set("dfs.http.address", "127.0.0.1:0");
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);

    StorageDirectory sd = nn.getFSImage().getStorage().getStorageDir(0);
    File editLog = NNStorage.getInProgressEditsFile(sd, 1);

    assertEquals("Edit log should contain a header as valid length",
        HEADER_LEN, EditLogFileInputStream.getValidLength(editLog));
    assertEquals("Edit log should have 1MB of bytes allocated",
        1024*1024, editLog.length());
    

    nn.mkdirs("/tmp", new FsPermission((short)777), false);

    assertEquals("Edit log should have more valid data after writing a txn",
        MKDIR_LEN + HEADER_LEN,
        EditLogFileInputStream.getValidLength(editLog));

    assertEquals("Edit log should be 1MB long",
        1024 * 1024, editLog.length());
    // 256 blocks for the 1MB of preallocation space
    assertTrue("Edit log disk space used should be at least 257 blocks",
        256 * 4096 <= new DU(editLog, conf).getUsed());
  }

}
