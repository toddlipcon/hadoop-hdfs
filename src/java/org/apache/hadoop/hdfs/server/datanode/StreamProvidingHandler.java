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
package org.apache.hadoop.hdfs;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.io.InputStream;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.queue.BlockingReadHandler;
import org.jboss.netty.handler.queue.BlockingReadTimeoutException;



public class StreamProvidingHandler
  extends InputStream
  implements ChannelUpstreamHandler,
  LifeCycleAwareChannelHandler
{

  private ChannelBuffer curBuf = null;
  private ChannelBufferInputStream cbis = null;
  private ChannelHandlerContext ctx;
  private BlockingReadHandler<ChannelBuffer> blockingReadHandler;

  private byte oneByteBuf[] = new byte[1];

  // Default to 1 year timeout (ie no timeout, really)
  private TimeUnit timeoutUnit = TimeUnit.DAYS;
  private long timeout = 365;

  public StreamProvidingHandler() {
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
      ctx.getChannel().close().await();
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
    try {
      if (curBuf == null || cbis.available() == 0) {
        curBuf = blockingReadHandler.read(timeout, timeoutUnit);
        if (curBuf == null) {
          return 0; // EOF (closed)
        }
        cbis = new ChannelBufferInputStream(curBuf);
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
                            timeout + " " + timeoutUnit);
    }
  }

  public void setTimeout(long timeout, TimeUnit unit) {
    this.timeout = timeout;
    this.timeoutUnit = unit;
  }
}