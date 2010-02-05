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

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.io.InputStream;
import java.io.EOFException;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.queue.BlockingReadHandler;
import org.jboss.netty.handler.queue.BlockingReadTimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InputStreamProvidingHandler
  extends InputStream
  implements ChannelUpstreamHandler,
  LifeCycleAwareChannelHandler
{
  public static final Log LOG = LogFactory.getLog(
    InputStreamProvidingHandler.class);

  private ChannelBuffer curBuf = null;
  private ChannelBufferInputStream cbis = null;
  private ChannelHandlerContext ctx;
  private BlockingReadHandler<ChannelBuffer> blockingReadHandler;

  private byte oneByteBuf[] = new byte[1];


  // 0 timeout is forever
  private long timeoutMillis = 0;

  public InputStreamProvidingHandler() {
    blockingReadHandler = new BlockingReadHandler<ChannelBuffer>();
  }


  ///////// Lifecycle Hooks /////////////
  @Override
  public void beforeAdd(ChannelHandlerContext ctx) {}
  @Override
  public void afterAdd(ChannelHandlerContext ctx) {
    this.ctx = ctx;
  }
  @Override
  public void beforeRemove(ChannelHandlerContext ctx) {}
  @Override
  public void afterRemove(ChannelHandlerContext ctx) {}


  ///////// Upstream Handler /////////////

  @Override
  public void handleUpstream(ChannelHandlerContext ctx,
                             ChannelEvent e) throws Exception {
    blockingReadHandler.handleUpstream(ctx, e);
  }

  public int available() throws IOException {
    if (curBuf == null) return 0;
    return cbis.available();
  }

  public void close() throws IOException {
    try {
      if (timeoutMillis > 0) {
        ctx.getChannel().close().await(timeoutMillis, TimeUnit.MILLISECONDS);
      } else {
        ctx.getChannel().close().await();
      }
    } catch (ChannelException ce) {
      throw new IOException(ce);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  public int read() throws IOException {
    int n = read(oneByteBuf, 0, 1);
    if (n == 0) return -1;
    return oneByteBuf[0];
  }

  public int read(byte buf[], int offset, int len) throws IOException {
    LOG.debug("Reading " + len + " bytes...");
    try {
      if (curBuf == null || cbis.available() == 0) {
        LOG.debug("Need a new buffer...");
        cbis = null;

        if (timeoutMillis > 0) {
          curBuf = blockingReadHandler.read(timeoutMillis, TimeUnit.MILLISECONDS);
        } else {
          curBuf = blockingReadHandler.read();
        }
        if (curBuf == null) {
          if (blockingReadHandler.isClosed()) {
            LOG.debug("Got EOF...");
            return -1;
          } else {
            LOG.debug("Got no data...");
            throw new IOException("No new buf, but also not closed.");
          }
        }
        cbis = new ChannelBufferInputStream(curBuf);
        LOG.debug("Got a new buffer with " + cbis.available() + " bytes...");
      }
      assert curBuf != null;
      assert cbis != null;

      return cbis.read(buf, offset, len);
    } catch (ChannelException ce) {
      throw new IOException(ce);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (BlockingReadTimeoutException te) {
      throw new IOException("Timed out in read after " +
                            timeoutMillis + "ms");
    }
  }

  public void setTimeout(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }
}