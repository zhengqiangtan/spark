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

import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
  *
  * 由Driver上的SparkEnv负责创建和注册到Driver的RpcEnv中。
  * BlockManagerMasterEndpoint只存在于Driver的SparkEnv中，
  * Driver或Executor上的BlockManagerMaster的driverEndpoint属性
  * 将持有BlockManagerMasterEndpoint的RpcEndpointRef。
  *
  * 主要对各个节点上的BlockManager、BlockManager与Executor的映射关系
  * 及Block位置信息（即Block所在的BlockManager）等进行管理。
  *
  * BlockManagerMasterEndpoint接收Driver或Executor上BlockManagerMaster发送的消息，
  * 对所有的BlockManager统一管理。
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  // BlockManagerId与BlockManagerInfo之间映射关系的缓存。
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  // Executor ID与BlockManagerId之间映射关系的缓存。
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  // BlockId与存储了此BlockId对应Block的BlockManager的BlockManagerId之间的一对多关系缓存。
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  // 执行询问操作会使用到的线程池
  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  // 转为了隐式对象
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  // 拓扑映射处理类，用于处理集群所有节点的拓扑结构的映射关系
  private val topologyMapper = {
    /**
      * 从spark.storage.replication.topologyMapper参数获取处理拓扑映射的类的类名，
      * 默认为DefaultTopologyMapper类型的类名
      */
    val topologyMapperClassName = conf.get(
      "spark.storage.replication.topologyMapper", classOf[DefaultTopologyMapper].getName)
    // 反射创建拓扑映射类的实例
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    // 返回创建好的实例
    mapper
  }

  logInfo("BlockManagerMasterEndpoint up")

  // 用于接收BlockManager相关的消息
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      context.reply(register(blockManagerId, maxMemSize, slaveEndpoint))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      // 这里还需要删除其他Worker上的Block
      removeBlockFromWorkers(blockId)
      // 然后返回true
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    // 获取Block的位置
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      // 遍历所有位置
      locations.foreach { blockManagerId: BlockManagerId =>
        // 获取对应的BlockManagerInfo对象
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          // 获取其中的BlockManagerSlaveEndpoint发送RemoveBlock消息
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    // 这里用到了隐式参数askExecutionContext
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
    * BlockManagerMasterEndpoint在接收到RegisterBlockManager消息后，将调用该方法
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId,
      maxMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    /**
      * 根据BlockManager发送消息中的BlockManagerId的相关信息，构造新的BlockManagerId
      * 主要是使用topologyMapper生产拓扑信息
      */
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) { // 当前并未管理该BlockManagerId的BlockManagerInfo对象
      // 根据BlockManagerId中保存的Executor ID获取旧的BlockManagerId
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) => // 存在，则移除
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      // 更新blockManagerIdByExecutor字典
      blockManagerIdByExecutor(id.executorId) = id

      /**
        * 更新blockManagerInfo字典，添加新的BlockManagerInfo对象，
        * 最终将触发对所有SparkListener的onBlockManagerAdded方法的调用，进而达到监控的目的。
        */
      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }
    // 投递BlockManager被添加的事件到事件总线
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
    // 返回新的BlockManagerId
    id
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

/**
  * 用于封装Block的状态信息
  * @param storageLevel Block的StorageLevel。
  * @param memSize Block占用的内存大小。
  * @param diskSize Block占用的磁盘大小。
  */
@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  // 是否存储到存储体系中，即memSize与diskSize的大小之和是否大于0。
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId, // 对应BlockManager的BlockManagerId
    timeMs: Long, // 创建时间
    val maxMem: Long, // BlockManager中剩余可用内存的大小
    val slaveEndpoint: RpcEndpointRef) // 对应的BlockManager所在节点的BlockManagerSlaveEndpointRef
  extends Logging {

  // 记录最后一次访问当前BlockManagerInfo的时间
  private var _lastSeenMs: Long = timeMs
  // 记录当前BlockManagerInfo对应的BlockManager管理的所有数据块的剩余的可用内存大小
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  // 记录当前BlockManagerInfo对应的BlockManager管理的所有数据块的BlockStatus状态对象
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  // 记录当前BlockManagerInfo对应的BlockManager管理的数据块，但不包括Broadcast数据块
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  // 获取指定数据块的BlockStatus
  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  // 更新指定数据块的BlockStatus信息
  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    // 更新时间最后一次访问当前BlockManagerInfo的时间
    updateLastSeenMs()

    // 判断当前BlockManagerInfo对应的BlockManager是否管理着指定的数据块
    if (_blocks.containsKey(blockId)) { // 存在，即管理着
      // The block exists on the slave already.
      // 获取数据块对应的BlockStatus
      val blockStatus: BlockStatus = _blocks.get(blockId)
      // 记录旧的值
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      // 检查原来的存储级别是否使用了内存存储
      if (originalLevel.useMemory) {
        /**
          * 因为原来的存储级别使用的内存，现在要对其进行修改，
          * 则先将旧的内存值累加到_remainingMem中，说明将该数据块原来使用的内存大小归还了，
          * 在后面的操作还会将新的内存值从_remainingMem中减去，说明又将特定的内存大小分配给该数据块了。
          */
        _remainingMem += originalMemSize
      }
    }

    // 检查设置的存储级别是否有效，即数据块使用了内存或磁盘存储级别，且副本数量大于1
    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      // 设置的存储级别有效
      var blockStatus: BlockStatus = null
      // Q: 疑问：为什么内存和存储只会使用一个？？？
      if (storageLevel.useMemory) { // 使用内存存储级别
        // 构建新的BlockStatus对象，磁盘存储为0
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        // 更新_blocks字典
        _blocks.put(blockId, blockStatus)
        // 将特定的内存大小分配给该数据块了，所以需要从总的剩余内存中减去
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) { // 使用磁盘存储级别
        // 构建新的BlockStatus对象，内存存储为0
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        // 更新_blocks字典
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }

      // 检查数据块是否不是Broadcast数据块，且已经存在缓存数据
      if (!blockId.isBroadcast && blockStatus.isCached) {
        // 如果是，将其记录到_cachedBlocks缓存集合
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) { // 存储级别无效，检查当前BlockManager是否管理该数据块
      // 说明此时是需要将该数据块进行移除的
      // If isValid is not true, drop the block.
      // 获取对应的BlockStatus
      val blockStatus: BlockStatus = _blocks.get(blockId)
      // 从_blocks中移除
      _blocks.remove(blockId)
      // 从_cachedBlocks中移除
      _cachedBlocks -= blockId
      // 打印日志
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }

  // 移除指定数据块
  def removeBlock(blockId: BlockId) {
    // 先判断是否存在该数据块
    if (_blocks.containsKey(blockId)) { // 存在
      // 归还数据块占用的内存给_remainingMem
      _remainingMem += _blocks.get(blockId).memSize
      // 从_blocks中移除
      _blocks.remove(blockId)
    }
    // 从_cachedBlocks中移除
    _cachedBlocks -= blockId
  }

  // 获取当前BlockManagerInfo对应的BlockManager管理的内存剩余的总大小
  def remainingMem: Long = _remainingMem

  // 获取当前BlockManagerinfo最后被访问的事件
  def lastSeenMs: Long = _lastSeenMs

  // 获取_block
  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  // 获取被缓存的数据块的BlockId集合
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  // 清除所有管理的数据块
  def clear() {
    // Q: 为什么清除所有数据块时不归还内存给_remainingMem
    _blocks.clear()
  }
}
