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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * Interface allowing applications to speak with a Spark standalone cluster manager.
 *
 * Takes a master URL, an app description, and a listener for cluster events, and calls
 * back the listener when various events occur.
  *
  * StandaloneAppClient是在Standalone模式下，Application与集群管理器进行对话的客户端。
  * 最为核心的功能是向集群管理器请求或杀死Executor。
 *
  * @param rpcEnv
  * @param masterUrls Each url should look like spark://host:port.
  *                   用于缓存每个Master的spark：//host：port格式的URL的数组。
  * @param appDescription Application的描述信息（ApplicationDescription）。Application-Description中记录了如下信息：
  *                       - Application的名称（name）
  *                       - Application需要的最大内核数（maxCores）
  *                       - 每个Executor所需要的内存大小（memoryPerExecutorMB）
  *                       - 运行CoarseGrainedExecutorBackend进程的命令（command）
  *                       - Spark UI的URL（appUiUrl）、事件日志的路径（eventLogDir）
  *                       - 事件日志采用的压缩算法名（eventLogCodec）
  *                       - 每个Executor所需的内核数（coresPerExecutor）
  *                       - 提交Application的用户名（user）
  * @param listener 对集群事件的监听器。Standalone-AppClientListener有两个实现类，分别是StandaloneSchedulerBackend和AppClientCollector。
  *                 AppClientCollector只用于测试，StandaloneSchedulerBackend可用在local-cluster或Standalone模式下。
  *                 StandaloneSchedulerBackend在构造StandaloneAppClient时会将自身作为listener参数。
  * @param conf
  */
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: StandaloneAppClientListener,
    conf: SparkConf)
  extends Logging {

  // Master的RPC地址（RpcAddress）
  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))

  // 注册超时的秒数，固定为20。
  private val REGISTRATION_TIMEOUT_SECONDS = 20
  // 注册的重试次数，固定为3。
  private val REGISTRATION_RETRIES = 3

  // 用于持有ClientEndpoint的RpcEndpointRef，org.apache.spark.deploy.client.ClientEndpoint类型。
  private val endpoint = new AtomicReference[RpcEndpointRef]
  // 用于持有Application的ID
  private val appId = new AtomicReference[String]
  // 用于标识是否已经将Application注册到Master
  private val registered = new AtomicBoolean(false)

  /**
    * ClientEndpoint继承自ThreadSafeRpcEndpoint，也是StandaloneAppClient的内部类，
    * StandaloneAppClient依赖于ClientEndpoint与集群管理器进行通信。
    */
  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {

    // 处于激活状态的Master的RpcEndpointRef。
    private var master: Option[RpcEndpointRef] = None
    // To avoid calling listener.disconnected() multiple times
    // 是否已经与Master断开连接。此属性用于防止多次调用StandaloneAppClientListener的disconnected()方法。
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    // 表示ClientEndpoint是否已经死掉。此属性用于防止多次调用StandaloneAppClientListener的dead方法。
    private val alreadyDead = new AtomicBoolean(false)
    // 用于保存registerMasterThreadPool执行的向各个Master注册Application的任务返回的Future。
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    // 用于持有向registrationRetryThread提交关于注册的定时调度返回的ScheduledFuture。
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // A thread pool for registering with masters. Because registering with a master is a blocking
    // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
    // time so that we can register with all masters.
    /**
      * 用于向Master注册Application的ThreadPoolExecutor。
      * registerMasterThreadPool的线程池大小为外部类StandaloneAppClient的masterRpcAddresses数组的大小，
      * 启动的线程以appclient-register-master-threadpool为前缀。
      */
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length // Make sure we can register with all masters at the same time
    )

    // A scheduled executor for scheduling the registration actions
    // 用于对Application向Master进行注册的重试。
    // 由registrationRetryThread启动的线程名称为appclient-registration-retry-thread。
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    // 将ClientEndpoint注册到RpcEnv的Dispatcher时，会触发对ClientEndpoint的onStart方法的调用。
    override def onStart(): Unit = {
      try {
        // 向Master注册Application
        registerWithMaster(1)
      } catch {
        case e: Exception => // 出现异常
          logWarning("Failed to connect to master", e)
          // 将当前ClientEndpoint标记为和Master断开连接
          markDisconnected()
          // 停止StandaloneAppClient
          stop()
      }
    }

    /**
     *  Register with all masters asynchronously and returns an array `Future`s for cancellation.
      *  向所有的Master尝试注册Application，并将返回的Future
     */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      // 遍历所有的Master的RpcAddress
      for (masterAddress <- masterRpcAddresses) yield {
        // 向registerMasterThreadPool线程池提交一个任务
        registerMasterThreadPool.submit(new Runnable {
          // 具体的线程任务
          override def run(): Unit = try {
            // 如果已经注册则直接返回
            if (registered.get) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            // 获取Master对应的RpcEndpointRef
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            // 向Master发送RegisterApplication消息，
            // 此消息携带外部类StandaloneAppClient的appDescription属性和ClientEndpoint自身的RpcEndpointRef
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        })
      }
    }

    /**
     * Register with all masters asynchronously. It will call `registerWithMaster` every
     * REGISTRATION_TIMEOUT_SECONDS seconds until exceeding REGISTRATION_RETRIES times.
     * Once we connect to a master successfully, all scheduling work and Futures will be cancelled.
     *
     * nthRetry means this is the nth attempt to register with master.
      *
      * 用于向Master注册Application
     */
    private def registerWithMaster(nthRetry: Int) {
      // tryRegisterAllMasters()向所有的Master尝试注册Application，将返回的Future保存到registerMasterFutures
      registerMasterFutures.set(tryRegisterAllMasters())
      // 提交定时调度
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) { // 如果已经注册成功，那么取消向Master注册Application
            // 调用每一个Future的cancel()方法取消向Master注册Application
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) { // 重试次数超过限制，标记ClientEndpoint死亡
            markDead("All masters are unresponsive! Giving up.")
          } else { // 向Master注册Application的重试
            // 调用每一个Future的cancel()方法取消向Master注册Application
            registerMasterFutures.get.foreach(_.cancel(true))
            // 递归调用，增加了重试次数
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    /**
     * Send a message to the current master. If we have not yet registered successfully with any
     * master, the message will be dropped.
     */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(s"Drop $message because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    // 处理收到的RegisteredApplication、ApplicationRemoved、ExecutorAdded、ExecutorUpdated、MasterChanged等消息
    override def receive: PartialFunction[Any, Unit] = {
      // 当注册Application成功后，Master将向ClientEndpoint回复RegisteredApplication消息
      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        // 记录Application ID
        appId.set(appId_)
        // 标识已注册
        registered.set(true)
        // 记录Master的RpcEndpointRef
        master = Some(masterRef)
        // 调用监听器的方法
        listener.connected(appId.get)

      // 移除Application
      case ApplicationRemoved(message) =>
        // 将ClientEndpoint标记为DEAD
        markDead("Master removed our application: %s".format(message))
        // 停止RpcEndpoint
        stop()

      // Executor添加了
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        // 调用监听器的方法
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      // Executor状态更新
      case ExecutorUpdated(id, state, message, exitStatus, workerLost) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) { // 状态是KILLED、FAILED、LOST或EXITED
          // 调用监听器的方法
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerLost)
        }

      // Master发生更新
      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
        // 更新Master信息
        master = Some(masterRef)
        alreadyDisconnected = false
        // 向Master发送MasterChangeAcknowledged消息
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }

    // 处理收到的StopAppClient、RequestExecutors、KillExecutors三种消息
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // 处理停止App Client消息
      case StopAppClient =>
        // 标记此ClientEndpoint已经死掉
        markDead("Application has been stopped.")
        // 向Master发送UnregisterApplication消息
        sendToMaster(UnregisterApplication(appId.get))
        // 回复true
        context.reply(true)
        // 停止RpcEndpoint
        stop()

      // 处理请求Executor消息
      case r: RequestExecutors =>
        master match {
          // 将消息转发给Master
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      // 处理杀死Executor消息
      case k: KillExecutors =>
        master match {
          // 将消息转发给Master
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    private def askAndReplyAsync[T](
        endpointRef: RpcEndpointRef,
        context: RpcCallContext,
        msg: T): Unit = {
      // Ask a message and create a thread to reply with the result.  Allow thread to be
      // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(s"Could not connect to $address: $cause")
      }
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        // 调用监听器的方法
        listener.disconnected()
        // 标记已经断开连接
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String) {
      if (!alreadyDead.get) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }

  // 启动StandaloneAppClient
  def start() {
    // Just launch an rpcEndpoint; it will call back into the listener.
    /**
      * 向SparkContext的SparkEnv的RpcEnv注册ClientEndpoint，
      * 进而引起对ClientEndpoint的启动和向Master注册Application。
      */
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  // 停止StandaloneAppClient
  def stop() {
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        // 向ClientEndpoint发送了StopAppClient消息，会超时等待
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      // 将ClientEndpoint的RpcEndpointRef从endpoint中清除
      endpoint.set(null)
    }
  }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
    *
    * 用于向Master请求所需的所有Executor资源。
   *
   * @return whether the request is acknowledged.
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    // requestTotalExecutors()方法只能在Application注册成功之后调用。
    if (endpoint.get != null && appId.get != null) {
      // 向ClientEndpoint发送RequestExecutors消息，消息携带了Application ID和所需的Executor总数。
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
    *
    * 用于向Master请求杀死Executor。
    *
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    // killExecutors()方法只能在Application注册成功之后调用。
    if (endpoint.get != null && appId.get != null) {
      // 向ClientEndpoint发送KillExecutors消息，此消息携带Application ID和要杀死的Executor的ID。
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}
