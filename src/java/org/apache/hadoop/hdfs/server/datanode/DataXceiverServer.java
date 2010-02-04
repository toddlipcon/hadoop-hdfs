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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;


import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or 
 * other DataNodes.  This small server does not use the 
 * Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;

  DataNode datanode;

  /** A manager to make sure that cluster balancing does not
   * take too much resources.
   * 
   * It limits the number of block moves for balancing and
   * the total amount of bandwidth they can use.
   */
  static class BlockBalanceThrottler extends BlockTransferThrottler {
   private int numThreads;
   
   /**Constructor
    * 
    * @param bandwidth Total amount of bandwidth can be used for balancing 
    */
   private BlockBalanceThrottler(long bandwidth) {
     super(bandwidth);
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
   }
   
   /** Check if the block move can start. 
    * 
    * Return true if the thread quota is not exceeded and 
    * the counter is incremented; False otherwise.
    */
   synchronized boolean acquire() {
     if (numThreads >= Balancer.MAX_NUM_CONCURRENT_MOVES) {
       return false;
     }
     numThreads++;
     return true;
   }
   
   /** Mark that the move is completed. The thread counter is decremented. */
   synchronized void release() {
     numThreads--;
   }
  }

  BlockBalanceThrottler balanceThrottler;
  
  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. For now we set it to be the default block size set
   * in the server side configuration, which is not ideal because the
   * default block size should be a client-size configuration. 
   * A better solution is to include in the header the estimated block size,
   * i.e. either the actual block size or the default block size.
   */
  long estimateBlockSize;


  /**
   * The parent (listening) channel for the xceiver server.
   */
  Channel serverChannel;

  /** TODO:
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
  */  

  CountDownLatch startLatch = new CountDownLatch(1);
  CountDownLatch stopLatch = new CountDownLatch(1);
  
  DataXceiverServer(InetSocketAddress bindAddress, Configuration conf, 
      DataNode datanode) throws IOException {
    
    this.datanode = datanode;
    this.estimateBlockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    
    //set up parameter for cluster balancing
    this.balanceThrottler = new BlockBalanceThrottler(
      conf.getLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY, 
                   DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT));

    try {
      ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool()));

      // Set up the pipeline factory.
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
          public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new XceiverServerHandler());
          }
        });

      serverChannel = bootstrap.bind(bindAddress);
    } catch (ChannelException ce) {
      throw new IOException(ce);
    }
  }

  InetSocketAddress getBoundAddress() {
    return (InetSocketAddress)serverChannel.getLocalAddress();
  }

  /**
   */
  public void run() {
    // Start accepting work.
    startLatch.countDown();

    while (datanode.shouldRun) {
      try {
        stopLatch.await();
      } catch (InterruptedException ie) {}
    }
    // TODO close the xceiver bootstrap
  }
  
  void kill() {
    assert datanode.shouldRun == false :
      "shoudRun should be set to false before killing";

    stopLatch.countDown();
  }

  public class XceiverServerHandler extends SimpleChannelUpstreamHandler {
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        e.getChannel().write(e.getMessage());
    }
  }

}
