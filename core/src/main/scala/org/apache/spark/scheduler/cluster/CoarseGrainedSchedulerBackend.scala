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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.spark.{ExecutorAllocationClient, SparkEnv, SparkException, TaskState}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

/**
 * A scheduler backend that waits for coarse-grained executors to connect.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 *
 * 等待CoarseGrainedExecutorBackend进行连接的SchedulerBackend实现。
 * 由CoarseGrainedSchedulerBackend建立的CoarseGrainedExecutorBackend进程将会一直存在，
 * 真正的Executor线程将在CoarseGrainedExecutorBackend进程中执行。
 *
 * CoarseGrainedSchedulerBackend实现了特质ExecutorAllocationClient。
 * 在创建并启动ExecutorAllocationManager的最后，会调用CoarseGrainedSchedulerBackend的requestTotalExecutors()方法
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging
{
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  // 用于统计当前集群中的CPU Core总数。
  protected val totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  // 当前注册到CoarseGrainedSchedulerBackend的Executor的总数。
  protected val totalRegisteredExecutors = new AtomicInteger(0)
  protected val conf = scheduler.sc.conf
  // RPC消息的最大大小。可通过spark.rpc.message.maxSize属性配置，默认为128MB。
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  // RPC请求的超时时间。可通过spark.rpc.askTimeout属性或spark.network.timeout属性配置，默认为120s。
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  /**
   * 已经注册的资源与期望得到的资源之间的最小比值，当比值大于等于_minRegisteredRatio时，才提交Task。
   * 可通过spark.scheduler.minRegistered-ResourcesRatio属性配置，默认为0。
   */
  private val _minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  /**
   * 在还未达到_minRegisteredRatio时，如果已经等待了超过maxRegisteredWaitingTimeMs指定的时间，那么提交Task。
   * 可通过spark.scheduler.maxRegisteredResourcesWaitingTime属性配置，默认为30s。
   */
  private val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")

  // CoarseGrainedSchedulerBackend的创建时间。
  private val createTime = System.currentTimeMillis()

  // Accessing `executorDataMap` in `DriverEndpoint.receive/receiveAndReply` doesn't need any
  // protection. But accessing `executorDataMap` out of `DriverEndpoint.receive/receiveAndReply`
  // must be protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should
  // only be modified in `DriverEndpoint.receive/receiveAndReply` with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  /**
   * Executor的ID与ExecutorData之间的映射关系缓存。
   * ExecutorData保存了Executor的RpcEndpointRef、RpcAddress、Host、
   * Executor的空闲内核数（free-Cores）、Executor的总内核数（totalCores）等信息。
   */
  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  // 从集群管理器请求的还未注册到CoarseGrainedSchedulerBackend的Executor的数量。
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure).
  /**
   * 请求集群管理器kill一个Executor时，Executor并不会立即被杀死，
   * 所以executorsPendingToRemove缓存那些请求kill的Executor的ID与是否真的需要被杀死之间的映射关系。
   * 值为true，表示该Executor需要被杀死，而当值为false时，表示仅仅是将该Executor移除后替换为新的Executor
   * 具体可见killExecutors()方法
   */
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val executorsPendingToRemove = new HashMap[String, Boolean]

  // A map to store hostname with its possible task number running on it
  // 缓存机器的Host和在机器本地运行的Task数量之间的映射关系。
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var hostToLocalTaskCount: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  // 用于统计有本地性需求的Task的数量。
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var localityAwareTasks = 0

  // The num of current max ExecutorId used to re-register appMaster
  // 用于保存注册到executorDataMap的最大的Executor的ID。
  @volatile protected var currentExecutorIdCounter = 0

  /**
    * DriverEndpoint是CoarseGrainedSchedulerBackend的内部类，
    * 无论CoarseGrainedSchedulerBackend还是StandaloneSchedulerBackend，
    * 都借助于DriverEndpoint和其他组件通信。
    *
    * @param sparkProperties 从SparkConf中获取的所有以spark.开头的属性信息。
    */
  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // Executors that have been lost, but for which we don't yet know the real exit reason.
    // 已经丢失的（但是还不知道真实的退出原因）的Executor的ID。
    protected val executorsPendingLossReason = new HashSet[String]

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    // SparkEnv的子组件closureSerializer的实例，用于对闭包序列化。
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    // 每个Executor的RpcEnv的地址与Executor的ID之间的映射关系。
    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    // 只有一个线程的ScheduledThreadPoolExecutor，用于唤起对延迟调度的Task进行资源分配与调度。
    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    // 将DriverEndpoint注册到RpcEnv的Dispatcher时，会触发对DriverEndpoint的onStart方法的调用
    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      // 定时任务的执行间隔时间，可通过spark.scheduler.revive.interval属性配置，默认为1s。
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      // 向reviveThread提交了一个向DriverEndpoint自己发送ReviveOffers消息的定时任务
      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          // 向自己发送ReviveOffers消息
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }

    // 接收消息并处理
    override def receive: PartialFunction[Any, Unit] = {
      /**
        * Task在运行的过程中，会向DriverEndpoint发送StatusUpdate消息，
        * 让Driver知道Task的当前状态，从而执行更新度量、将Task释放的资源分配给其他Task等操作。
        */
      case StatusUpdate(executorId, taskId, state, data) =>
        // 调用TaskSchedulerImpl的statusUpdate方法更新Task的状态
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) { // Task的状态为已完成
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              // 将Task释放的内核数增加到对应Executor的空闲内核数
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              // 给下一个要调度的Task分配资源并运行Task
              makeOffers(executorId)
            case None => // 对于未知的Executor，DriverEndpoint选择忽略
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      // 启动时DriverEndpoint会向自己发送ReviveOffers消息
      case ReviveOffers =>
        // 调用makeOffers()方法
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        // 获取对应的ExecutorData
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            // 向Executor发送KillTask消息
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      /**
        * 给Driver分配了Executor之后，需要向Driver注册Executor，
        * 以便于Driver与Executor之间直接通信，而不用通过Worker作为媒介，进而提升执行效率。
        */
      case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
        if (executorDataMap.contains(executorId)) { // 重复注册Executor
          // 通过向CoarseGrainedExecutorBackend发送RegisterExecutorFailed消息，表示重复注册了。
          executorRef.send(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
          context.reply(true)
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          // 获取CoarseGrainedExecutorBackend的RpcAddress
          val executorAddress = if (executorRef.address != null) {
              executorRef.address
            } else {
              context.senderAddress
            }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          // 将CoarseGrainedExecutorBackend的RpcAddress和executorId的对应关系放入addressToExecutorId缓存中。
          addressToExecutorId(executorAddress) = executorId
          // 增加Driver已经获得的内核总数
          totalCoreCount.addAndGet(cores)
          // 向Driver注册的所有Executor的总数
          totalRegisteredExecutors.addAndGet(1)
          // 创建ExecutorData对象
          val data = new ExecutorData(executorRef, executorRef.address, hostname,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            // 将executorId与ExecutorData的对应关系放入executorDataMap缓存中
            executorDataMap.put(executorId, data)
            // 更新currentExecutorIdCounter和numPendingExecutors
            if (currentExecutorIdCounter < executorId.toInt) {
              currentExecutorIdCounter = executorId.toInt
            }
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }

          // 向CoarseGrainedExecutorBackend发送RegisteredExecutor消息，
          // CoarseGrainedExecutorBackend接收到RegisteredExecutor消息后将创建Executor实例
          executorRef.send(RegisteredExecutor)
          // Note: some tests expect the reply to come after we put the executor in the map
          context.reply(true)
          // 向LiveListenerBus投递SparkListenerExecutorAdded事件
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          // 给Task分配资源并运行Task
          makeOffers()
        }

      // 处理停止Driver的消息
      case StopDriver =>
        // 回复true
        context.reply(true)
        // 关闭RpcEndpoint
        stop()

      // 处理停止Executor的消息
      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        // 遍历executorDataMap中的ExecutorData
        for ((_, executorData) <- executorDataMap) {
          // 向每个CoarseGrainedExecutorBackend进程发送StopExecutor消息，
          // 由CoarseGrainedExecutorBackend进程进行资源回收处理
          executorData.executorEndpoint.send(StopExecutor)
        }
        // 回复true
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)
        context.reply(true)

      /**
        * CoarseGrainedExecutorBackend进程在启动过程中会向DriverEndpoint发送RetrieveSparkAppConfig消息，
        * 从Driver获取Executor所需的Spark属性信息和密钥信息。
        */
      case RetrieveSparkAppConfig =>
        // 构造Spark属性信息
        val reply = SparkAppConfig(sparkProperties,
          SparkEnv.get.securityManager.getIOEncryptionKey())
        // 将信息进行回复
        context.reply(reply)
    }

    // Make fake resource offers on all executors
    private def makeOffers() {
      // Filter out executors under killing
      // 过滤出激活的Executor
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
      // 根据每个激活的Executor的配置，创建WorkerOffer
      val workOffers = activeExecutors.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toIndexedSeq
      // 调用TaskSchedulerImpl的resourceOffers()方法给Task分配资源，调用launchTasks()方法运行Task。
      launchTasks(scheduler.resourceOffers(workOffers))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      // 找出对应的Executor ID，进行移除操作
      addressToExecutorId
        .get(remoteAddress)
        .foreach(removeExecutor(_, SlaveLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")))
    }

    // Make fake resource offers on just one executor
    private def makeOffers(executorId: String) {
      // Filter out executors under killing
      // 先判断Executor是否是激活的
      if (executorIsAlive(executorId)) {
        // 获取对应的ExecutorData对象
        val executorData = executorDataMap(executorId)
        // 创建WorkerOffer样例类对象
        val workOffers = IndexedSeq(
          new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
        // 分配资源并运行Task
        launchTasks(scheduler.resourceOffers(workOffers))
      }
    }

    private def executorIsAlive(executorId: String): Boolean = synchronized {
      !executorsPendingToRemove.contains(executorId) &&
        !executorsPendingLossReason.contains(executorId)
    }

    // Launch tasks returned by a set of resource offers
    // 运行Task
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        // 对TaskDescription进行序列化
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= maxRpcMessageSize) { // 序列化后的大小超出了Rpc消息的限制
          // 从TaskSchedulerImpl的taskIdToTaskSetManager中找出Task对应的TaskSetManager
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                "spark.rpc.message.maxSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, maxRpcMessageSize)
              // 放弃对TaskSetManager的调度
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else { // 序列化后的TaskDescription的大小小于RPC消息大小的最大值maxRpcMessageSize
          val executorData = executorDataMap(task.executorId)
          // 减少Executor的空闲内核数freeCores
          executorData.freeCores -= scheduler.CPUS_PER_TASK

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")

          // 向CoarseGrainedExecutorBackend发送LaunchTask消息。CoarseGrainedExecutorBackend将在收到LaunchTask消息后运行Task。
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      logDebug(s"Asked to remove executor $executorId with reason $reason")
      // 获取对应的ExecutorData
      executorDataMap.get(executorId) match {
        case Some(executorInfo) => // 能获取到
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          // 维护各类缓存的记录
          val killed = CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            executorsPendingToRemove.remove(executorId).getOrElse(false)
          }
          // 维护总的CPU Core计数
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          // 维护注册的Executor计数
          totalRegisteredExecutors.addAndGet(-1)
          // 通知TaskScheduler有Executor移除了
          scheduler.executorLost(executorId, if (killed) ExecutorKilled else reason)
          // 向消息总线投递SparkListenerExecutorRemoved消息
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
        case None => // 没有获取到对应的ExecutorData对象
          // SPARK-15262: If an executor is still alive even after the scheduler has removed
          // its metadata, we may receive a heartbeat from that executor and tell its block
          // manager to reregister itself. If that happens, the block manager master will know
          // about the executor, but the scheduler will not. Therefore, we should remove the
          // executor from the block manager when we hit this case.
          // 通知BlockManager移除对应的Executor
          scheduler.sc.env.blockManager.master.removeExecutorAsync(executorId)
          logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

/**
 * Stop making resource offers for the given executor. The executor is marked as lost with
 * the loss reason still pending.
 *
 * @return Whether executor should be disabled
 */
protected def disableExecutor(executorId: String): Boolean = {
  val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
    if (executorIsAlive(executorId)) { // Executor还是激活状态的
      // 将其添加到executorsPendingLossReason中
      executorsPendingLossReason += executorId
      // 返回true
      true
    } else {
      // Returns true for explicitly killed executors, we also need to get pending loss reasons;
      // For others return false.
      // 如果Executor记录在将被移除的字典中，则返回true
      executorsPendingToRemove.contains(executorId)
    }
  }

  if (shouldDisable) { // 需要被disable
    logInfo(s"Disabling executor $executorId.")
    // 告诉TaskScheduler有Executor丢失了
    scheduler.executorLost(executorId, LossReasonPending)
  }

  shouldDisable
}

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  // DriverEndpoint的RpcEndpointRef
  var driverEndpoint: RpcEndpointRef = null

  protected def minRegisteredRatio: Double = _minRegisteredRatio

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        // 将以spark.开头的属性添加到数组缓冲properties中
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    // 创建DriverEndpoint
    driverEndpoint = createDriverEndpointRef(properties)
  }

  // 创建DriverEndpoint
  protected def createDriverEndpointRef(
      properties: ArrayBuffer[(String, String)]): RpcEndpointRef = {
    /**
      * 创建DriverEndpoint，
      * 并将DriverEndpoint注册到SparkContext的SparkEnv的RpcEnv中，
      * 注册时以常量ENDPOINT_NAME（值为CoarseGrainedScheduler）作为注册名。
      */
    rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties)
  }

  def stopExecutors() {
    try {
      // 向DriverEndpoint发送StopExecutors消息
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithRetry[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    // 停止Executor
    stopExecutors()
    try {
      // 向DriverEndpoint发送StopDriver消息
      if (driverEndpoint != null) {
        driverEndpoint.askWithRetry[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   *
   * 重置CoarseGrainedSchedulerBackend的为初始状态，
   * 此操作是YARN Client模式下在Application Master注册失败时被调用的
   **/
  protected def reset(): Unit = {
    // 获取当前CoarseGrainedSchedulerBackend中所有已申请到的Executor的ID
    val executors = synchronized {
      // 清空numPendingExecutors计数器
      numPendingExecutors = 0
      // 清空executorsPendingToRemove字典
      executorsPendingToRemove.clear()
      // 得到所有已申请到的Executor的ID
      Set() ++ executorDataMap.keys
    }

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    // 对每个已申请到的Executor进行移除操作
    executors.foreach { eid =>
      removeExecutor(eid, SlaveLost("Stale executor after cluster manager re-registered."))
    }
  }

  /**
    * TaskSchedulerImpl的submitTasks方法最后会
    * 调用TaskSchedulerImpl的resourceOffers()方法给Task分配资源并运行Task
    */
  override def reviveOffers() {
    // 向DriverEndpoint发送ReviveOffers消息
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  /**
   * Called by subclasses when notified of a lost worker. It just fires the message and returns
   * at once.
   */
  protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // Only log the failure since we don't care about the result.
    // 向DriverEndpoint发送RemoveExecutor消息
    driverEndpoint.ask[Boolean](RemoveExecutor(executorId, reason)).onFailure { case t =>
      logError(t.getMessage, t)
    }(ThreadUtils.sameThread)
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")
      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  private def numExistingExecutors: Int = executorDataMap.size

  override def getExecutorIds(): Seq[String] = {
    executorDataMap.keySet.toSeq
  }

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    // 检查参数
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")

    val response = synchronized {
      // 更新记录着正在申请的Executor的数量的计数器
      numPendingExecutors += numAdditionalExecutors
      logDebug(s"Number of pending executors is now $numPendingExecutors")

      // Account for executors pending to be added or removed
      /**
       * 这里重新计算了一些需要申请的Executor个数，使用doRequestTotalExecutors()方法进行申请
       * 即已申请到的Executor数量 + 正在申请的Executor数量 - 等待移除的Executor数量
       */
      doRequestTotalExecutors(
        numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
    }

    // 阻塞等待，附带超时机制
    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   *                     请求的Executor数量
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   *                           有本地性偏好的Task总数
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   *                             Host与想要在Host本地运行的Task数量之间的映射关系
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]
    ): Boolean = {

    // 检查参数
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }

    val response = synchronized {
      // 记录有本地性需求的Task
      this.localityAwareTasks = localityAwareTasks
      // 记录机器的Host和在机器本地运行的Task数量之间的映射关系
      this.hostToLocalTaskCount = hostToLocalTaskCount

      // 计算待申请的Executor数量
      numPendingExecutors =
        math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)

      // 调用doRequestTotalExecutors()方法
      doRequestTotalExecutors(numExecutors)
    }

    // 超时等待执行
    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] =
    Future.successful(false)

  /**
   * Request that the cluster manager kill the specified executors.
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final override def killExecutors(executorIds: Seq[String]): Seq[String] = {
    killExecutors(executorIds, replace = false, force = false)
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * When asking the executor to be replaced, the executor loss is considered a failure, and
   * killed tasks that are running on the executor will count towards the failure limits. If no
   * replacement is being requested, then the tasks will not count towards the limit.
   *
   * @param executorIds identifiers of executors to kill
   *                    需要被杀死的Executor
   * @param replace whether to replace the killed executors with new ones
   *                是否将杀死的Executor替换为新的
   * @param force whether to force kill busy executors
   *              是否强制杀死正处于繁忙状态的Executor
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final def killExecutors(
      executorIds: Seq[String],
      replace: Boolean,
      force: Boolean): Seq[String] = {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")

    val response = synchronized {
      // 将需要杀死的Executor分为已知的和未知的，已知的Executor在executorDataMap中是有注册的
      val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
      // 记录未知Executor相关的日志
      unknownExecutors.foreach { id =>
        logWarning(s"Executor to kill $id does not exist!")
      }

      // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
      // If this executor is busy, do not kill it unless we are told to force kill it (SPARK-9552)
      val executorsToKill = knownExecutors
        // 从已知Executor中过滤掉已经处于等待移除的Executor
        .filter { id => !executorsPendingToRemove.contains(id) }
        // 如果没有指定强制杀死繁忙的Executor，则过滤掉繁忙的Executor
        .filter { id => force || !scheduler.isExecutorBusy(id) }

      /**
       * 将过滤后的Executor的ID更新到executorsPendingToRemove字典中，
       * 对应的值指定是否需要替换为新的Executor。
       */
      executorsToKill.foreach { id => executorsPendingToRemove(id) = !replace }

      logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")

      // If we do not wish to replace the executors we kill, sync the target number of executors
      // with the cluster manager to avoid allocating new ones. When computing the new target,
      // take into account executors that are pending to be added or removed.
      // 调整整体的Executor数量，
      val adjustTotalExecutors =
        if (!replace) {
          // 不需要替换被杀死的Executor，重新计算并申请Executor；申请成功将返回true
          doRequestTotalExecutors(
            numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
        } else {
          // 需要替换杀死的Executor，则将需要杀死的Executor的数量添加到numPendingExecutors进行记录
          numPendingExecutors += knownExecutors.size
          // 直接返回true
          Future.successful(true)
        }

      // 定义杀死Executor的具体操作
      val killExecutors: Boolean => Future[Boolean] =
        if (!executorsToKill.isEmpty) {
          // 如果有需要杀死的Executor，则使用doKillExecutors()方法处理
          _ => doKillExecutors(executorsToKill)
        } else {
          // 否则直接返回false
          _ => Future.successful(false)
        }

      // 根据adjustTotalExecutors的结果来杀死具体的Executor
      val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)

      // 返回具体杀死的Executor ID的集合
      killResponse.flatMap(killSuccessful =>
        Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
      )(ThreadUtils.sameThread)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)
}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
