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

package org.apache.spark

import java.io._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)

/** RpcEndpoint class for MapOutputTrackerMaster
  * 用于接收获取map中间状态和停止对map中间状态进行跟踪的请求，
  * 实现了特质RpcEndpoint并重写了receiveAndReply方法
  **/
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) => // 获取Map中间状态的请求
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      // 会将消息投递到MapOutputTrackerMaster的mapOutputRequests队列，等待MessageLoop线程进行处理
      val mapOutputStatuses = tracker.post(new GetMapOutputMessage(shuffleId, context))

    case StopMapOutputTracker => // 停止跟踪Map中间状态的请求
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      // 直接返回true
      context.reply(true)
      // 停止了当前的RpcEndpoint
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and executor) use different HashMap to store its metadata.
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  /** Set to the MapOutputTrackerMasterEndpoint living on the driver.
    * 用于持有Driver上MapOutputTrackerMasterEndpoint的RpcEndpointRef。
    **/
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * This HashMap has different behavior for the driver and the executors.
   *
   * On the driver, it serves as the source of map outputs recorded from ShuffleMapTasks.
   * On the executors, it simply serves as a cache, in which a miss triggers a fetch from the
   * driver's corresponding HashMap.
   *
   * Note: because mapStatuses is accessed concurrently, subclasses should make sure it's a
   * thread-safe map.
   *
   * 用于维护各个Map任务的输出状态。
   * 其中键对应shuffleId，值存储各个Map任务对应的状态信息MapStatus。
   * 各个MapOutputTrackerWorker会向MapOutputTrackerMaster不断汇报Map任务的状态信息，
   * MapOutputTrackerMaster的mapStatuses中维护的信息是最新最全的。
   * MapOutputTrackerWorker的mapStatuses对于本节点Executor运行的Map任务状态是及时更新的，
   * 而对于其他节点上的Map任务状态则更像一个缓存，
   * 在mapStatuses不能命中时会向Driver上的MapOutputTrackerMaster获取最新的任务状态信息。
   */
  protected val mapStatuses: Map[Int, Array[MapStatus]]

  /**
   * Incremented every time a fetch fails so that client nodes know to clear
   * their cache of map output locations if this happens.
   *
   * 用于Executor故障转移的同步标记。
   * 每个Executor在运行的时候会更新epoch，潜在的附加动作将清空缓存。
   * 当Executor丢失后增加epoch。
   */
  protected var epoch: Long = 0
  // 用于保证epoch变量的线程安全性。
  protected val epochLock = new AnyRef

  /** Remembers which map output locations are currently being fetched on an executor.
   * shuffle获取集合，用来记录当前Executor正在从哪些Map输出的位置拉取数据。
   */
  private val fetching = new HashSet[Int]

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   *
   * 用于向MapOutputTrackerMasterEndpoint发送消息，并期望在超时时间之内得到回复。
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      // 将通过RpcEndpoint的askWithRetry()方法实现
      trackerEndpoint.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true.
   * 用于向MapOutputTrackerMasterEndpoint发送消息，
   * 并期望在超时时间之内获得的返回值为true，否则抛出异常
   */
  protected def sendTracker(message: Any) {
    // 使用askTracker()方法实现
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given reduce task.
   *
   * 通过shuffleId和reduceId获取存储了Reduce所需的Map中间输出结果的BlockManager的BlockManagerId，
   * 以及Map中间输出结果每个Block块的BlockId与大小
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    // 调用重载方法
    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1)
  }

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given range of map output partitions (startPartition is included but
   * endPartition is excluded from the range).
   *
   * 通过shuffleId和reduceId获取存储了Reduce所需的Map中间输出结果的BlockManager的BlockManagerId，
   * 以及Map中间输出结果每个Block块的BlockId与大小
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    // 获取shuffleId对应的Map任务状态
    val statuses = getStatuses(shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized { // 加锁
      // 使用MapOutputTracker的相关方法实现
      return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    }
  }

  /**
   * Return statistics about all of the outputs for a given shuffle.
   *
   * 用于获取shuffle依赖的各个Map输出Block大小的统计信息
   */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    // 先获取ShuffleID对应的MapStatus（即Map状态信息）的数组
    val statuses = getStatuses(dep.shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      // 获取ShuffleDependency经过分区器后的分区总数
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      // 遍历Map状态信息数组
      for (s <- statuses) {
        // 遍历所有分区
        for (i <- 0 until totalSizes.length) {
          /**
           * 从Map状态信息中获取指定Reduce任务需要拉取的Block的大小
           * getSizeForBlock()是MapStatus的方法
           * 从这里的操作可以看出，Reduce任务的个数就是ShuffleDependency中分区器的总分区数
           * 且Reduce任务的reduceId就是其通过ShuffleDependency中分区器得到的分区的ID
           */
          totalSizes(i) += s.getSizeForBlock(i)
        }
      }
      // 返回包含了统计信息的MapOutputStatistics样例类对象
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  /**
   * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
   * on this array when reading it, because on the driver, we may be changing it in place.
   *
   * (It would be nice to remove this restriction in the future.)
    *
    * 根据shuffleId获取MapStatus（即Map状态信息）的数组
   */
  private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    // 尝试从本地mapStatuses字典获取
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) { // 获取为空，可能需要远程获取
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized { // 加锁
        // Someone else is fetching it; wait for them to be done
        // 正在获取，就该获取操作等待结束
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        // 再次尝试从本地mapStatuses字典读取
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        // 仍未读取到，就将shuffleId放入fetching集合，标识要开始进行获取了
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          // 向MapOutputTrackerMasterEndpoint发送GetMapOutputStatuses消息，以获取Map任务的状态信息
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          // 接收到Map任务状态信息后，对其进行反序列化操作得到Array[MapStatus]类型数组赋值给fetchedStatuses
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          // 对刚获取到的Map任务状态进行本地缓存
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            // 从fetching中移除shuffleId，标识对其对应的Map任务状态信息的获取结束
            fetching -= shuffleId
            // 唤醒阻塞在fetching上的线程，注意这里是有多线程操作环境的
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      // 获取到了，直接返回
      if (fetchedStatuses != null) {
        return fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      return statuses
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   *
   * 当Executor运行出现故障时，Master会再分配其他Executor运行任务，
   * 此时会调用该方法更新年代信息，并且清空mapStatuses。
   */
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  /** Unregister shuffle data.
    * 用于ContextCleaner清除shuffleId对应MapStatus的信息
    **/
  def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
  }

  /** Stop the tracker. */
  def stop() { }
}

/**
 * MapOutputTracker for the driver.
 *
 * MapOutputTrackerWorker将Map任务的跟踪信息，
 * 通过MapOutputTrackerMasterEndpoint的RpcEndpointRef发送给MapOutputTrackerMaster，
 * 由MapOutputTrackerMaster负责整理和维护所有的map任务的输出跟踪信息。
 *
 * MapOutputTrackerMasterEndpoint位于MapOutputTrackerMaster内部，二者只存在于Driver上。
 */
private[spark] class MapOutputTrackerMaster(conf: SparkConf,
    broadcastManager: BroadcastManager, isLocal: Boolean)
  extends MapOutputTracker(conf) {

  /** Cache a serialized version of the output statuses for each shuffle to send them out faster
    * 对MapOutputTracker的epoch的缓存。
    **/
  private var cacheEpoch = epoch

  // The size at which we use Broadcast to send the map output statuses to the executors
  /**
    * 用于广播的最小大小。
    * 可以使用spark.shuffle.mapOutput.minSizeForBroadcast属性配置，默认为512KB。
    * minSizeForBroadcast必须小于maxRpcMessageSize。
    */
  private val minSizeForBroadcast =
    conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512k").toInt

  /** Whether to compute locality preferences for reduce tasks
    * 是否为reduce任务计算本地性的偏好。
    * 可以使用spark.shuffle.reduceLocality.enabled属性进行配置，默认为true。
    **/
  private val shuffleLocalityEnabled = conf.getBoolean("spark.shuffle.reduceLocality.enabled", true)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  // 没有指定偏好位置信息的Map任务的最大数量
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  // 没有指定偏好位置信息的Reduce任务的最大数量，当使用HighlyCompressedMapStatus时该值需小于2000
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  /**
   * Map任务输出的实际位置命中对应的偏好位置的占比，该值越大，数据本地化会越充分，
   * 但也有可能由于一些位置过于繁忙导致调度出现更多时延
   */
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // HashMaps for storing mapStatuses and cached serialized statuses in the driver.
  // Statuses are dropped only by explicit de-registering.
  /**
   * 用于维护各个Map任务的输出状态。
   * 其中键对应shuffleId，值存储各个Map任务对应的状态信息MapStatus。
   * MapOutputTrackerMaster的mapStatuses中维护的信息是最新最全的。
   */
  protected val mapStatuses = new ConcurrentHashMap[Int, Array[MapStatus]]().asScala

  /**
    * 用于存储shuffleId与序列化后的状态的映射关系。
    * 其中key对应shuffleId，value为对MapStatus序列化后的字节数组。
    */
  private val cachedSerializedStatuses = new ConcurrentHashMap[Int, Array[Byte]]().asScala

  /**
    * 最大的Rpc消息的大小。
    * 此属性可以通过spark.rpc.message.maxSize属性进行配置，默认为128MB。
    * minSizeForBroadcast必须小于maxRpcMessageSize。
    */
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // Kept in sync with cachedSerializedStatuses explicitly
  // This is required so that the Broadcast variable remains in scope until we remove
  // the shuffleId explicitly or implicitly.
  /**
    * 用于缓存序列化的广播变量，保持与cachedSerializedStatuses的同步。
    * 当需要移除shuffleId在cachedSerializedStatuses中的状态数据时，此缓存中的数据也会被移除。
    */
  private val cachedSerializedBroadcast = new HashMap[Int, Broadcast[Array[Byte]]]()

  // This is to prevent multiple serializations of the same shuffle - which happens when
  // there is a request storm when shuffle start.
  /**
    * 每个shuffleId对应的锁。
    * 当shuffle过程开始时，会有大量的关于同一个shuffle的请求，使用锁可以避免对同一shuffle的多次序列化。
    */
  private val shuffleIdLocks = new ConcurrentHashMap[Int, AnyRef]()

  // requests for map output statuses
  // 使用阻塞队列来缓存GetMapOutputMessage（获取map任务输出）的请求。
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  /**
    * 用于处理Map任务输出状态请求消息的固定大小的线程池。
    * 此线程池提交的线程都以后台线程运行，且线程名以map-output-dispatcher为前缀，
    * 线程池大小可以使用spark.shuffle.mapOutput.dispatcher.numThreads属性配置，默认大小为8。
    */
  private val threadpool: ThreadPoolExecutor = {
    // 获取线程数量
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    // 创建固定线程数量的线程池
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    // 向每个线程池中提交一个MessageLoop任务
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  // Broadcast的最小大小不可大于RPC消息的最大大小
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"spark.shuffle.mapOutput.minSizeForBroadcast ($minSizeForBroadcast bytes) must " +
      s"be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an rpc " +
      "message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  // 投递GetMapOutputMessage消息
  def post(message: GetMapOutputMessage): Unit = {
    // 将消息放入mapOutputRequests队列，等待MessageLoop线程处理
    mapOutputRequests.offer(message)
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            // 从mapOutputRequests中获取GetMapOutputMessage
            val data = mapOutputRequests.take()
             if (data == PoisonPill) { // 如果是毒药，就将其放回并结束当前MessageLoop任务
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            // 获取RpcCallContext
            val context = data.context
            // 获取shuffleId
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            // 获取对应shuffleId所对应的序列化Map任务状态信息
            val mapOutputStatuses = getSerializedMapOutputStatuses(shuffleId)
            // 将序列化的Map任务状态信息返回客户端
            context.reply(mapOutputStatuses)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new GetMapOutputMessage(-99, null)

  // Exposed for testing 缓存的经过序列化的Broadcast的数量
  private[spark] def getNumCachedSerializedBroadcast = cachedSerializedBroadcast.size

  // 注册shuffleId，第二个参数为Map任务数量
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    // 注册shuffleId的同时会检查是否已经存在，如果已经存在则抛出异常
    if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    // add in advance
    // 并为对应的shuffleId创建锁对象
    shuffleIdLocks.putIfAbsent(shuffleId, new Object())
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    // 获取shuffleId对应的Map状态信息数组
    val array = mapStatuses(shuffleId)
    array.synchronized {
      // 向指定索引添加状态信息
      array(mapId) = status
    }
  }

  /** Register multiple map output information for the given shuffle
    * 把ShuffleMapStage中每个ShuffleMapTask的MapStatus保存到shuffleId在mapStatuses中对应的数组中。
    * 第三个参数在一个Task任务完成，或者Executor丢失时会为true
    **/
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    // 直接将statuses放入mapStatuses字典
    mapStatuses.put(shuffleId, statuses.clone())
    if (changeEpoch) { // 如果需要改变年代信息，则自增年代信息
      incrementEpoch()
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager
   * 根据指定Shuffle ID、Map任务ID和BlockManagerId取消Map任务输出信息的注册
   **/
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    // 根据Shuffle ID获取对应的Map任务的状态信息数组
    val arrayOpt = mapStatuses.get(shuffleId)
    // 判断是否存在
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      // 获取Map任务的状态信息数组
      val array = arrayOpt.get
      array.synchronized {
        // 判断是否存在对应的Map任务ID，同时数据存储的位置要与bmAddress参数指定的一致
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          // 置空
          array(mapId) = null
        }
      }
      // 自增年代信息
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data */
  override def unregisterShuffle(shuffleId: Int) {
    // 移除对应的Map任务状态信息数组
    mapStatuses.remove(shuffleId)
    // 移除对应的序列化后的状态数据
    cachedSerializedStatuses.remove(shuffleId)
    // 移除对应的序列化的广播变量缓存，最终委托给了BroadcastManager处理
    cachedSerializedBroadcast.remove(shuffleId).foreach(v => removeBroadcast(v))
    // 移除对应的锁
    shuffleIdLocks.remove(shuffleId)
  }

  /** Check if the given shuffle is being tracked
    * 查找是否已经存在指定shuffleId对应的MapStatus
    **/
  def containsShuffle(shuffleId: Int): Boolean = {
    // 先查字节缓存，再查原数据
    cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
  }

  /**
   * Return the preferred hosts on which to run the given map output partition in a given shuffle,
   * i.e. the nodes that the most outputs for that partition are on.
   *
   * 根据ShuffleDependency和指定的Map任务输出的分区编号来获取偏好位置序列
   *
   * @param dep shuffle dependency object
   *            指定的ShuffleDependency
   * @param partitionId map output partition that we want to read
   *                    指定的Map输出的分区编号
   * @return a sequence of host names
   *         偏好位置序列
   */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
      : Seq[String] = {
    /**
     * 需要同时满足下面的三个条件：
     * 1. 判断是否开了为Reduce任务计算本地性的偏好的功能。
     * 2. ShuffleDependency依赖的RDD的分区数量小于SHUFFLE_PREF_MAP_THRESHOLD阈值。
     * 3. ShuffleDependency的分区器的总分区数量小于SHUFFLE_PREF_REDUCE_THRESHOLD阈值。
     */
    if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
        dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      // 找出存放指定Shuffle、指定reduceId所需要拉取的数据达到一定占比的BlockManager，占比阈值为0.2
      val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
        dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
      if (blockManagerIds.nonEmpty) { // 不为空
        // 获取BlockManager所在节点的地址并返回
        blockManagerIds.get.map(_.host)
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  /**
   * Return a list of locations that each have fraction of map output greater than the specified
   * threshold.
   *
   * 获取指定Shuffle、指定Reduce任务的Map任务输出的位置信息列表，
   * 返回值是一个BlockManagerId列表，该列表中BlockManagerId对应的BlockManager中存放的数据
   * 占总数据的占比需要大于指定的阈值fractionThreshold
   *
   * @param shuffleId id of the shuffle
   *                  指定Shuffle
   * @param reducerId id of the reduce task
   *                  指定的Reduce任务ID
   * @param numReducers total number of reducers in the shuffle
   *                     指定Shuffle过程中Reduce任务的总数
   * @param fractionThreshold fraction of total map output size that a location must have
   *                          for it to be considered large.
   *                          阈值
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {

    // 从mapStatuses字典获取对应Shuffle的Map任务状态信息数组
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses != null) { // 不为空才处理
      statuses.synchronized { // 加锁
        if (statuses.nonEmpty) { // 状态信息数组不为空
          // HashMap to add up sizes of all blocks at the same location
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          // 遍历状态信息数组
          while (mapIdx < statuses.length) {
            // 获取对应位置的Map任务状态信息
            val status = statuses(mapIdx)
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            if (status != null) { // 不为空
              // 获取指定reduceId的所需要拉取的数据块大小
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) { // 需要拉取的数据块大于0
                // 更新locs数组，键为BlockManagerId，值为对应BlockManager上存储的数据块的大小累计值
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                // 累计总输出大小
                totalOutputSize += blockSize
              }
            }
            // mapId自增
            mapIdx = mapIdx + 1
          }
          // 过滤出单个数据块大小与总数据大小比值大于fractionThreshold阈值的位置信息
          val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // Return if we have any locations which satisfy the required threshold
          if (topLocs.nonEmpty) {
            // 返回过滤得到的BlockManagerId的数组
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    // 为空则直接返回None
    None
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  // 移除指定的Broadcast
  private def removeBroadcast(bcast: Broadcast[_]): Unit = {
    if (null != bcast) {
      // 使用BroadcastManager进行管理
      broadcastManager.unbroadcast(bcast.id,
        removeFromDriver = true, blocking = false)
    }
  }

  // 清理缓存的经过序列化的Broadcast
  private def clearCachedBroadcast(): Unit = {
    // 遍历cachedSerializedBroadcast字典，使用removeBroadcast()方法移除
    for (cached <- cachedSerializedBroadcast) removeBroadcast(cached._2)
    // 清空cachedSerializedBroadcast字典
    cachedSerializedBroadcast.clear()
  }

  // 获取指定shuffleId的序列化后的Map任务状态信息
  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var retBytes: Array[Byte] = null
    var epochGotten: Long = -1

    // Check to see if we have a cached version, returns true if it does
    // and has side effect of setting retBytes.  If not returns false
    // with side effect of setting statuses
    // 用于检查缓存的状态的方法
    def checkCachedStatuses(): Boolean = {
      epochLock.synchronized {
        // 检查年代信息
        if (epoch > cacheEpoch) { // 如果年代信息发生变化
          // 清除cachedSerializedStatuses字典
          cachedSerializedStatuses.clear()
          clearCachedBroadcast()
          // 更新年代信息
          cacheEpoch = epoch
        }
        // 从缓存的状态信息字典中尝试获取
        cachedSerializedStatuses.get(shuffleId) match {
          case Some(bytes) => // 获取到
            // 赋值给retBytes记录
            retBytes = bytes
            // 标记已获取到
            true
          case None => // 否则尝试从mapStatuses中获取对应的状态数组
            logDebug("cached status not found for : " + shuffleId)
            statuses = mapStatuses.getOrElse(shuffleId, Array.empty[MapStatus])
            // 记录年代信息
            epochGotten = epoch
            false
        }
      }
    }

    // 如果checkCachedStatuses()方法能从cachedSerializedStatuses缓存中获取到则直接返回
    if (checkCachedStatuses()) return retBytes

    // 获取对应的锁，如果没有则新创建
    var shuffleIdLock = shuffleIdLocks.get(shuffleId)
    if (null == shuffleIdLock) {
      val newLock = new Object()
      // in general, this condition should be false - but good to be paranoid
      val prevLock = shuffleIdLocks.putIfAbsent(shuffleId, newLock)
      shuffleIdLock = if (null != prevLock) prevLock else newLock
    }
    // synchronize so we only serialize/broadcast it once since multiple threads call
    // in parallel
    shuffleIdLock.synchronized { // 加锁
      // double check to make sure someone else didn't serialize and cache the same
      // mapstatus while we were waiting on the synchronize
      // 再次尝试使用checkCachedStatuses()方法从cachedSerializedStatuses缓存中获取
      if (checkCachedStatuses()) return retBytes

      // If we got here, we failed to find the serialized locations in the cache, so we pulled
      // out a snapshot of the locations as "statuses"; let's serialize and return that
      // 将上面获取到的MapStatus数组序列化，该方法会对序列化MapStatus数组后产生的字节数组进行广播
      val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
        isLocal, minSizeForBroadcast)
      logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
      // Add them into the table only if the epoch hasn't changed while we were working
      epochLock.synchronized { // 年代锁
        if (epoch == epochGotten) { // 判断年代是否发生改变
          // 没有改变，先缓存到cachedSerializedStatuses字典中
          cachedSerializedStatuses(shuffleId) = bytes
          // 缓存Broadcast对象
          if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
        } else {
          logInfo("Epoch changed, not caching!")
          // 年代信息发生了变化，移除刚刚获取的Broadcast广播对象
          removeBroadcast(bcast)
        }
      }
      // 返回获取到的字节数组
      bytes
    }
  }

  override def stop() {
    // 投递"毒药消息"
    mapOutputRequests.offer(PoisonPill)
    // 关闭线程池
    threadpool.shutdown()
    // 向MapOutputTrackerMasterEndpoint发送StopMapOutputTracker消息
    sendTracker(StopMapOutputTracker)
    // 清空mapStatuses
    mapStatuses.clear()
    // 将MapOutputTrackerMasterEndpoint置为空
    trackerEndpoint = null
    // 清空cachedSerializedStatuses
    cachedSerializedStatuses.clear()
    clearCachedBroadcast()
    // 清空Shuffle Lock
    shuffleIdLocks.clear()
  }
}

/**
 * MapOutputTracker for the executors, which fetches map output information from the driver's
 * MapOutputTrackerMaster.
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  protected val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus], broadcastManager: BroadcastManager,
      isLocal: Boolean, minBroadcastSize: Int): (Array[Byte], Broadcast[Array[Byte]]) = {
    // 创建输出流
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    // 包装为Gzip压缩流
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized { // 加锁，并写入到流中
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    // 将流中的数据转换为字节数组
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) { // 检查大小
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      // 广播该数据
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      // 序列化广播后得到的Broadcast对象
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(new GZIPOutputStream(out))
      oos.writeObject(bcast)
      oos.close()
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    assert (bytes.length > 0)

    // 用于反序列化对象
    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        // 直接反序列化
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Array[MapStatus]]
      case BROADCAST => // Broadcast模式的反序列化
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        // 需要经过两次反序列化，第一次得到Broadcast对象，第二次从Broadcast得到value后再次反序列化
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        // 序列化Broadcast的value
        deserializeObject(bcast.value, 1, bcast.value.length - 1).asInstanceOf[Array[MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  private def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    // 遍历MapStatus集合
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) { // 状态为空，抛出异常
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        // 遍历每个对应的分区
        for (part <- startPartition until endPartition) {
          // 添加到splitsByAddress字典
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
      }
    }

    splitsByAddress.toSeq
  }
}
