/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;

/**
 * Simple wrapper on top of a TransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class OneForOneBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

  // 用于向服务端发送请求的TransportClient。
  private final TransportClient client;
  /**
   * 即OpenBlocks。
   * OpenBlocks将携带远端节点的：
   *  - appId（应用程序标识）
   *  - execId（Executor标识）
   *  - blockIds（BlockId的数组）
   * 这表示从远端的哪个实例获取哪些Block，并且知道是哪个远端Executor生成的Block。
   */
  private final OpenBlocks openMessage;
  // BlockId的数组。与openMessage的blockIds属性一致。
  private final String[] blockIds;
  // 将在获取Block成功或失败时被回调。
  private final BlockFetchingListener listener;
  // 获取块成功或失败时回调，配合BlockFetchingListener使用。
  private final ChunkReceivedCallback chunkCallback;

  /**
   * 客户端给服务端发送OpenBlocks消息后，
   * 服务端会在OneForOneStreamManager的streams缓存中缓存从存储体系中读取到的ManagedBuffer序列，
   * 并生成与ManagedBuffer序列对应的streamId，
   * 然后将streamId和ManagedBuffer序列的大小封装为StreamHandle消息返回给客户端，
   * 客户端的streamHandle属性将持有此StreamHandle消息。
   */
  private StreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
      TransportClient client,
      String appId,
      String execId,
      String[] blockIds,
      BlockFetchingListener listener) {
    this.client = client;
    this.openMessage = new OpenBlocks(appId, execId, blockIds);
    this.blockIds = blockIds;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block.
   * 用于拉取数据后的回调
   **/
  private class ChunkCallback implements ChunkReceivedCallback {

    // 拉取单个的块成功
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      // 会通知给监听器的方法，从这里可以得知，服务端返回的数据是按照BlockId的顺序进行排列的
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    // 拉取单个的块失败
    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      // 由failRemainingBlocks()方法通知给监听器
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  /**
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    // 参数检查
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }

    // 使用TransportClient的sendRpc()发送OpenBlocks消息
    client.sendRpc(openMessage.toByteBuffer(),
            // 并向客户端的outstandingRpcs缓存注册匿名的RpcResponseCallback实现
            new RpcResponseCallback() {
      // 获取成功
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          // 从响应中反序列化得到StreamHandle对象
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          // 遍历StreamHandle的numChunks
          for (int i = 0; i < streamHandle.numChunks; i++) {
            /**
             * 调用TransportClient的fetchChunk()方法逐个获取Block。
             * 注意，该方法在完成后会将结果通过chunkCallback相关回调函数返回
             * chunkCallback的回调函数会调用监听器，传递拉取数据和结果
             * 详见{@link ChunkCallback}
             */
            client.fetchChunk(streamHandle.streamId, i, chunkCallback);
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }
}
