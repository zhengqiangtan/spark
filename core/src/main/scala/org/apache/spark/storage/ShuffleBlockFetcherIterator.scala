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

package org.apache.spark.storage

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
  *
  * 用于获取多个Block的迭代器。
  * 如果Block在本地，那么从本地的BlockManager获取；
  * 如果Block在远端，那么通过ShuffleClient请求远端节点上的BlockTransferService获取。
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
  *                     用于从远端节点下载Block。
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
  *                        将要获取的Block与所在地址的关系。
  *                        从此属性可以看出，每个BlockManager中包含一到多个任务需要的Block。
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
  *                         一批请求的最大字节数。
  *                         总数不能超过maxBytesInFlight，而且每个请求的字节数不能超过maxBytesInFlight的1/5，以提高请求的并发度，保证至少向5个不同的节点发送请求获取数据，最大限度地利用各节点的资源。
  *                         可以通过参数spark.reducer.maxMbInFlight来控制大小（默认为48MB）。
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
  *                        单次最多请求数。
  *                        此参数可以通过spark.reducer.maxReqsInFlight属性配置，默认为Integer.MAX_VALUE。
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    maxBytesInFlight: Long,
    maxReqsInFlight: Int)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
    *
    * 一共要获取的Block数量
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
    *
    * 已经处理的Block数量
   */
  private[this] var numBlocksProcessed = 0

  // ShuffleBlockFetcherIterator的启动时间
  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks.
    * 缓存了本地BlockManager管理的Block的BlockId。
    **/
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks.
    * 缓存了远端BlockManager管理的Block的BlockId。
    **/
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
    *
    * 用于保存获取Block的结果信息（FetchResult）
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
    *
    * 当前正在处理的FetchResult。
   */
  @volatile private[this] var currentResult: FetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
    *
    * 获取Block的请求信息（FetchRequest）的队列。
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /** Current bytes in flight from our requests
    * 当前批次的请求的字节数。
    **/
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight
    * 当前批次的请求的数量。
    **/
  private[this] var reqsInFlight = 0

  // Shuffle的度量信息
  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
    *
    * ShuffleBlockFetcherIterator是否处于激活状态。
    * 如果isZombie为true，则ShuffleBlockFetcherIterator处于非激活状态。
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  // 进行初始化
  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    currentResult match {
      case SuccessFetchResult(_, _, _, buf, _) => buf.release()
      case _ =>
    }
    currentResult = null
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    // 将请求的所有Block的大小累加到bytesInFlight
    bytesInFlight += req.size
    // 将reqsInFlight累加一
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    val blockIds = req.blocks.map(_._1.toString)

    // 批量下载远端的Block
    val address = req.address
    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        // 下载成功后会调用该方法
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          // Only add the buffer to results queue if the iterator is not zombie,
          // i.e. cleanup() has not been called yet.
          ShuffleBlockFetcherIterator.this.synchronized {
            if (!isZombie) { // 结果封装为SuccessFetchResult放入results中
              // Increment the ref count because we need to pass this to a different thread.
              // This needs to be released after use.
              buf.retain()
              remainingBlocks -= blockId
              // 将结果封装为SuccessFetchResult放入到results中
              results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
                remainingBlocks.isEmpty))
              logDebug("remainingBlocks: " + remainingBlocks)
            }
          }
          logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          results.put(new FailureFetchResult(BlockId(blockId), address, e))
        }
      }
    )
  }

  // 划分从本地读取和需要远程读取的Block的请求
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    // 每个远程请求的最大尺寸，等于maxBytesInFlight的1/5或者1
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    // 缓存需要远程请求的FetchRequest对象
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    // 统计所有Block的总大小
    var totalBlocks = 0
    // 遍历已经在blocksByAddress中缓存的按照BlockManagerId分组的BlockId
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) { // BlockManagerId对应的Executor与当前Executor相同
        // Filter out zero-sized blocks
        // 将BlockManagerId对应的所有大小不为零的BlockId存入localBlock
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        // 将所有大小不为零的BlockId存入local Blocks
        numBlocksToFetch += localBlocks.size
      } else { // 远端的Block
        val iterator = blockInfos.iterator
        // 当前累加到curBlocks中的所有Block的大小，用于保证每个远程请求的尺寸不超过targetRequestSize的限制
        var curRequestSize = 0L
        // 远程获取的累加缓存，用于保存每个远程请求的尺寸不超过targetRequestSize的限制，
        // 实现批量发送请求，以提高系统性能
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            // 将所有大小大于零的BlockId和size累加到curBlocks
            curBlocks += ((blockId, size))
            // 将所有大小不为零的BlockId存入remoteBlocks
            remoteBlocks += blockId
            numBlocksToFetch += 1
            // 增加当前请求要获取的Block的总大小
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) { // 每当curRequestSize≥targetRequestSize
            // Add this FetchRequest
            // 新建FetchRequest放入remoteRequests
            remoteRequests += new FetchRequest(address, curBlocks)
            // 新建curBlocks，将curRequestSize置为0，为生成下一个FetchRequest做准备
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) { // 对剩余的Block新建FetchRequest放入remoteRequests
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
    *
    * 获取本地Block
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      // 获取BlockId
      val blockId = iter.next()
      try {
        // 获取Block数据,创建SuccessFetchResult对象，并添加到results中
        val buf = blockManager.getBlockData(blockId)
        // 维护Shuffle度量信息
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        // 创建SuccessFetchResult对象，添加到results中
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    // 给TaskContextImpl添加任务完成的监听器，便于任务执行完成后调用cleanup()方法进行一些清理工作
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    // 划分从本地读取和需要远程读取的Block的请求
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    // 将FetchRequest随机排序后存入fetchRequests
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 发送请求
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    // 获取本地Block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  // 取决于numBlocksProcessed是否小于numBlocksToFetch
  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()
    // 从results队列中取出一个FetchResult
    currentResult = results.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    // 根据FetchResult的类型匹配FailureFetchResult或SuccessFetchResult
    result match {
      case SuccessFetchResult(_, address, size, buf, isNetworkReqDone) =>
        // 维护度量信息
        if (address != blockManager.blockManagerId) {
          shuffleMetrics.incRemoteBytesRead(buf.size)
          shuffleMetrics.incRemoteBlocksFetched(1)
        }
        bytesInFlight -= size
        if (isNetworkReqDone) {
          reqsInFlight -= 1
          logDebug("Number of requests in flight " + reqsInFlight)
        }
      case _ =>
    }
    // Send fetch requests up to maxBytesInFlight
    fetchUpToMaxBytes()

    result match {
      case FailureFetchResult(blockId, address, e) =>
        throwFetchFailedException(blockId, address, e)

      case SuccessFetchResult(blockId, address, _, buf, _) =>
        try {
          // 返回BlockId与BufferReleasingInputStream的元组。
          (result.blockId, new BufferReleasingInputStream(buf.createInputStream(), this))
        } catch {
          case NonFatal(t) =>
            throwFetchFailedException(blockId, address, t)
        }
    }
  }

  // 用于向远端发送请求，以获取Block
  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight
    // 遍历fetchRequest中所有的FetchRequest
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 ||
        (reqsInFlight + 1 <= maxReqsInFlight &&
          bytesInFlight + fetchRequests.front.size <= maxBytesInFlight))) {
      // 发生FetchRequest远程请求，获取Block中间结果
      sendRequest(fetchRequests.dequeue())
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    // 返回FetchRequest要下载的所有Block的大小之和。
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
