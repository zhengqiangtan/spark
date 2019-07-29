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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
    blockManagerId: BlockManagerId)

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
  *
  * HeartbeatReceiver运行在Driver上，用以接收各个Executor的心跳（HeartBeat）消息，对各个Executor的生死进行监控。
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  sc.addSparkListener(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  // TaskSchedulerImpl
  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  // 用于维护Executor的身份标识与HeartbeatReceiver最后一次收到Executor的心跳（HeartBeat）消息的时间戳之间的映射关系。
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  // Executor节点上的BlockManager的超时时间（单位为ms）。
  // 可通过spark.storage.blockManagerSlaveTimeoutMs属性配置，默认为120000。
  private val slaveTimeoutMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
  // Executor的超时时间（单位为ms）。可通过spark.network.timeout属性配置，默认为120000。
  private val executorTimeoutMs =
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  // 超时的间隔（单位为ms）。可通过spark.storage.blockManagerTimeoutIntervalMs属性配置，默认为60000。
  private val timeoutIntervalMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  // 检查超时的间隔（单位为ms）。可通过spark.network.timeoutInterval属性配置，默认采用timeoutIntervalMs的值。
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000

  // 向eventLoopThread提交执行超时检查的定时任务后返回的ScheduledFuture。提交的定时任务的执行间隔为checkTimeoutIntervalMs。
  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  // 用于执行心跳接收器的超时检查任务。
  // eventLoopThread只包含一个线程，此线程以heartbeat-receiver-event-loop-thread作为名称。
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  // 以Executors.newSingleThreadExecutor方式创建的ExecutorService，
  // 运行的单线程用于杀死Executor，此线程以kill-executor-thread作为名称
  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  /**
    * 当向Dispatcher注册HeartbeatReceiver时，
    * 与HeartbeatReceiver对应的Inbox将向自身的messages列表中放入OnStart消息，
    * 之后Dispatcher将处理OnStart消息，并调用HeartbeatReceiver的onStart方法。
    */
  override def onStart(): Unit = {
    // 创建定时调度任务timeoutCheckingTask
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      // 按照checkTimeoutIntervalMs指定的间隔，向HeartbeatReceiver自身发送ExpireDeadHosts消息。
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  // 接收消息并回复
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    case ExecutorRegistered(executorId) =>
      /**
        * 取出ExecutorRegistered消息携带的要注册的Executor的身份标识，
        * 并把此Executor的身份标识与最后一次见面时间放入executorLastSeen中，
        * 最后向HeartbeatReceiver自己回复true。
        */
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)

    case ExecutorRemoved(executorId) =>
      /**
        * 取出ExecutorRemoved消息携带的要移除的Executor的身份标识，
        * 并从executorLastSeen中移除此Executor的缓存，
        * 最后向HeartbeatReceiver自己回复true。
        */
      executorLastSeen.remove(executorId)
      context.reply(true)

    /**
      * 在创建了TaskScheduler后，SparkContext的_taskScheduler属性将持有了TaskScheduler的引用，
      * SparkContext会向HeartbeatReceiver发送TaskSchedulerIsSet消息。
      */
    case TaskSchedulerIsSet =>
      /**
        * 接收到TaskSchedulerIsSet消息后，将获取SparkContext的_taskScheduler属性持有的TaskScheduler的引用，
        * 并由自身的scheduler属性保存，最后向SparkContext回复true。
        */
      scheduler = sc.taskScheduler
      context.reply(true)

    case ExpireDeadHosts =>
      /**
        * 接收到ExpireDeadHosts消息后，将调用expireDeadHosts方法，
        * 然后向HeartbeatReceiver自己回复true。
        *
        * expireDeadHosts()方法用于检查超时的Executor。
        */
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
    /**
      * 每个活跃的Executor都会向Driver上的HeartbeatReceiver发送HeartBeat消息；
      * HeartbeatReceiver在接收到HeartBeat消息后，都将向Executor回复HeartbeatResponse消息。
      * HeartbeatResponse消息携带的reregisterBlockManager表示
      * 是否要求Executor重新向BlockManagerMaster注册BlockManager。
      */
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
      if (scheduler != null) { // TaskSchedulerImpl不为空
        if (executorLastSeen.contains(executorId)) { // executorLastSeen中包含HeartBeat消息携带的Executor的身份标识
          // 更新此Executor的最后一次见面时间
          executorLastSeen(executorId) = clock.getTimeMillis()
          // 提交用于调用TaskSchedulerImpl的executorHeartbeatReceived()方法的任务
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              /**
                * executorHeartbeatReceived()方法用于更新正在处理的Task的度量信息，
                * 并且让BlockManagerMaster知道BlockManager仍然存活。
                */
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              // 回复HeartbeatResponse消息
              context.reply(response)
            }
          })
        } else { // executorLastSeen中不包含HeartBeat消息携带的Executor的身份标识
          /**
            * 这种情况发生在刚刚从executorLastSeen中移除Executor及其最后见面时间后，
            * 却收到了Executor在被移除之前发送的HeartBeat消息。
            */
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          logDebug(s"Received heartbeat from unknown executor $executorId")
          // 向Executor发送HeartbeatResponse消息，此消息将会要求BlockManager重新注册
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else { // TaskSchedulerImpl为空
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        // 向Executor发送HeartbeatResponse消息，此消息将会要求BlockManager重新注册
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    // 向HeartbeatReceiver自己发送ExecutorRegistered消息
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
    *
    * 事件总线在接收到SparkListenerExecutorAdded消息后，
    * 将调用HeartbeatReceiver的onExecutorAdded()方法，这样HeartbeatReceiver将监听到Executor的添加。
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    // 从SparkListenerExecutorAdded获取Executor ID，调用addExecutor()方法
    addExecutor(executorAdded.executorId)
  }

  /**
   * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    // 向HeartbeatReceiver自己发送ExecutorRemoved消息
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
    *
    * 监听到Executor的移除
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    // 获取Executor ID，调用removeExecutor()方法
    removeExecutor(executorRemoved.executorId)
  }

  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    // 遍历executorLastSeen中的每个Executor的身份标识对应的lastSeenMs时间
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      // 判断当前时间（now）与Executor的lastSeenMs的差值是否大于executorTimeoutMs
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        // 移除丢失的Executor，并对Executor上正在运行的Task重新分配资源后进行调度
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
          // Asynchronously kill the executor to avoid blocking the current thread
        // 向单线程线程池killExecutorThread提交任务
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            // 杀死Executor
            sc.killAndReplaceExecutor(executorId)
          }
        })
        // 移除对Executor的超时检查
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
