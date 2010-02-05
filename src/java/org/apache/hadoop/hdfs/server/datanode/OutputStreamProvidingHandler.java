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
package org.apache.hadoop.hdfs.server.datanode;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OutputStreamProvidingHandler
  extends OutputStream {

  public static final Log LOG = LogFactory.getLog(
    OutputStreamProvidingHandler.class);

  Channel channel;
  long timeoutMillis = 0;


  public OutputStreamProvidingHandler(Channel channel) {
    this.channel = channel;
  }

  @Override
  public void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte)(b & 0xFF);
    write(buf, 0, 1);
  }

  @Override
  public void write(byte buf[], int off, int len) throws IOException {
    LOG.debug("Writing len=" + len);
    ChannelBuffer cbuf = ChannelBuffers.wrappedBuffer(buf, off, len);

    try {
      ChannelFuture future = channel.write(cbuf);
      LOG.debug("Awaiting future...");

      if (!future.await(timeoutMillis)) {
        throw new IOException("Write operation timed out after " + timeoutMillis
                              + "ms");
      }
      LOG.debug("The future is now!");

      if (future.isCancelled()) {
        throw new IOException("Write operation cancelled");
      } else if (!future.isSuccess()) {
        throw new IOException("Write operation failed", future.getCause());
      }
      assert future.isSuccess();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ChannelException ce) {
      throw new IOException(ce);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      ChannelFuture f = channel.close();
      if (!f.await(timeoutMillis)) {
        throw new IOException("Timed out closing channel " + channel
                              + " after " + timeoutMillis + "ms");
      }
      if (! f.isSuccess()) {
        throw new IOException("Failed to close channel " + channel,
                              f.getCause());
      }
    } catch (InterruptedException ie) {
      throw new IOException("Close Interrupted");
    }
  }

  public void setTimeout(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}