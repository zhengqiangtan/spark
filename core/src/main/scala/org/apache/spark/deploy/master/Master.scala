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

package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
  * Master是local-cluster部署模式和Standalone部署模式中，整个Spark集群最为重要的组件之一，
  * 它的设计将直接决定整个集群的可扩展性、可用性和容错性。
  * Master的职责包括Worker的管理、Application的管理、Driver的管理等。
  * Master负责对整个集群中所有资源的统一管理和分配，它接收各个Worker的注册、更新状态、心跳等消息，
  * 也接收Driver和Application的注册。
  *
  * @param rpcEnv Master会向该RpcEnv注册自己
  * @param address RpcEnv的地址
  * @param webUiPort WebUI的端口
  * @param securityMgr 安全管理器
  * @param conf SparkConf
  */
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  /**
    * 包含一个线程的ScheduledThreadPoolExecutor，启动的线程以master-forward-message-thread作为名称。
    * forwardMessageThread主要用于运行checkForWorkerTimeOutTask和recoveryCompletionTask。
    */
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  // Hadoop的配置
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  // Worker的超时时间。可通过spark.worker.timeout属性配置，默认为60s。
  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
  /**
    * completedApps中最多可以保留的ApplicationInfo的数量的限制大小。
    * 当completedApps中的ApplicationInfo数量大于等于RETAINED_APPLICATIONS时，
    * 需要对completedApps中的部分ApplicationInfo进行清除。
    * 可通过spark.deploy.retainedApplications属性配置，默认为200。
    */
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  /**
    * completedDrivers中最多可以保留的DriverInfo的数量的限制大小。
    * 当completedDrivers中的DriverInfo数量大于等于RETAINED_DRIVERS时，
    * 需要对completedDrivers中的部分DriverInfo进行清除。
    * 可通过spark.deploy.retainedDrivers属性配置，默认为200。
    */
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  // 从workers中移除处于死亡（DEAD）状态的Worker所对应的WorkerInfo的权重。可通过spark.dead.worker.persistence属性配置，默认为15。
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  // 恢复模式。可通过spark.deploy.recoveryMode属性配置，默认为NONE。
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
  // Executor的最大重试数。可通过spark.deploy.max-ExecutorRetries属性配置，默认为10。
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)

  // 所有注册到Master的Worker信息（WorkerInfo）的集合。
  val workers = new HashSet[WorkerInfo]
  // Application ID与ApplicationInfo的映射关系。
  val idToApp = new HashMap[String, ApplicationInfo]
  // 正等待调度的Application所对应的ApplicationInfo的集合。
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  // 所有ApplicationInfo的集合。
  val apps = new HashSet[ApplicationInfo]

  // Worker ID与WorkerInfo的映射关系。
  private val idToWorker = new HashMap[String, WorkerInfo]
  // Worker的RpcEnv的地址（RpcAddress）与WorkerInfo的映射关系。
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  // RpcEndpointRef与ApplicationInfo的映射关系。
  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  // Application对应Driver的RpcEnv的地址（RpcAddress）与App-licationInfo的映射关系。
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  // 已经完成的ApplicationInfo的集合。
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  // 下一个Application的号码。nextAppNumber将参与到Application ID的生成规则中。
  private var nextAppNumber = 0

  // 所有Driver信息（DriverInfo）的集合。
  private val drivers = new HashSet[DriverInfo]
  // 已经完成的DriverInfo的集合。
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  // 正等待调度的Driver所对应的DriverInfo的集合。
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  // 下一个Driver的号码。
  private var nextDriverNumber = 0

  Utils.checkHost(address.host, "Expected hostname")

  // Master的度量系统
  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  // 应用程序的度量系统
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  // 有关Master的度量来源
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  // Master的WebUI
  private var webUi: MasterWebUI = null

  // Master的公开地址。可通过Java系统环境变量SPARK_PUBLIC_DNS配置，默认为Master的RpcEnv的地址。
  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  // Master的Spark URL（即spark://host:port格式的地址）。
  private val masterUrl = address.toSparkURL
  // Master的WebUI的URL
  private var masterWebUiUrl: String = _

  /**
    * Master所处的状态。Master的状态包括：
    * - 支持（STANDBY）
    * - 激活（ALIVE）
    * - 恢复中（RECOVERING）
    * - 完成恢复（COMPLETING_RECOVERY）
    */
  private var state = RecoveryState.STANDBY

  // 持久化引擎（PersistenceEngine）。
  private var persistenceEngine: PersistenceEngine = _

  // 领导选举代理（LeaderElectionAgent）。
  private var leaderElectionAgent: LeaderElectionAgent = _

  // 当Master被选举为领导后，用于集群状态恢复的任务。
  private var recoveryCompletionTask: ScheduledFuture[_] = _

  // 检查Worker超时的任务
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  /**
    * 是否允许Application能够在所有节点间调度。
    * 在所有节点间执行循环调度是Spark在实现更好的配置内存方法之前的临时解决方案，
    * 通过此方案可以避免Application总是固定在一小群节点上执行。
    * 可通过spark.deploy.spreadOut属性配置，默认为true。
    */
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  // 应用程序默认的最大内核数。可通过spark.deploy.defaultCores属性配置，默认为java.lang.Integer.MAX_VALUE。
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  // SparkUI是否采用反向代理。可通过spark.ui.reverseProxy属性配置，默认为false。
  val reverseProxy = conf.getBoolean("spark.ui.reverseProxy", false)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  // 是否提供REST服务以提交应用程序。可通过spark.master.rest.enabled属性配置，默认为true。
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  // REST服务的实例，类型为StandaloneRestServer。
  private var restServer: Option[StandaloneRestServer] = None
  // REST服务绑定的端口。
  private var restServerBoundPort: Option[Int] = None

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // 创建MasterWebUI并绑定端口
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    // 拼接得到Master WebUI的URL，赋值给masterWebUiUrl
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort

    // 如果WebUI开启了反向代理
    if (reverseProxy) {
      // 将masterWebUiUrl设置为从spark.ui.reverseProxyUrl属性获得的反向代理的URL。
      masterWebUiUrl = conf.get("spark.ui.reverseProxyUrl", masterWebUiUrl)
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }

    // 启动检查Worker超时的任务
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        // 向自己发送CheckForWorkerTimeOut消息，会调用timeOutDeadWorkers()方法
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    // 创建并启动StandaloneRestServer的REST服务
    if (restServerEnabled) {
      // StandaloneRestServer的端口可通过spark.master.rest.port属性配置，默认为6066。
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    // 将masterSource注册到masterMetricsSystem
    masterMetricsSystem.registerSource(masterSource)
    // 启动Master的度量系统
    masterMetricsSystem.start()
    // 启动Application的度量系统
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    // 将masterMetricsSystem和applicationMetricsSystem的ServletContextHandler添加到MasterWebUI。
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    // 根据RECOVERY_MODE创建持久化引擎和领导选举代理
    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      // RECOVERY_MODE为ZOOKEEPER
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        // 持久化引擎为ZooKeeperPersistenceEngine，领导选举代理为ZooKeeperLeaderElectionAgent
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))

      // RECOVERY_MODE为FILESYSTEM
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        // 持久化引擎为FileSystemPersistenceEngine，领导选举代理为MonarchyLeaderAgent
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))

      // RECOVERY_MODE为CUSTOM
      case "CUSTOM" =>
        // 通过spark.deploy.recoveryMode.factory属性配置创建持久化引擎和领导选举代理的工厂类
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        // 创建对应的实例
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))

      // 其他情况
      case _ =>
        // 持久化引擎为BlackHolePersistenceEngine，领导选举代理为MonarchyLeaderAgent
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  // 当Master被选举为领导时，领导选举代理（LeaderElectionAgent）将会调用Master的electedLeader方法
  override def electedLeader() {
    // 向自己发送ElectedLeader消息
    self.send(ElectedLeader)
  }

  /**
    * 当Master没有被选举为领导时，
    * ZooKeeperLeaderElectionAgent将会调用revokedLeadership方法撤销Master的领导关系。
    */
  override def revokedLeadership() {
    // 向自己发送RevokedLeadership消息
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    // 当Master成为Leader时会收到自己给自己发送的ElectedLeader消息
    case ElectedLeader =>
      // 从持久化引擎中读取出持久化的ApplicationInfo、DriverInfo、WorkerInfo等信息
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        // 如果没有任何持久化信息，那么将Master的当前状态设置为激活（ALIVE）
        RecoveryState.ALIVE
      } else {
        // 如果有持久化信息，那么将Master的当前状态设置为恢复中（RECOVERING）
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) { // 如果Master的当前状态为RECOVERING
        // 对整个集群的状态进行恢复
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        // 恢复完成后，创建延时任务recoveryCompletionTask
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // 向Master自身发送CompleteRecovery消息
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }

    // 当Master成为Leader后，进行恢复操作，恢复操作结束后会向自己发送CompleteRecovery消息
    case CompleteRecovery => completeRecovery()

    // 当Master没有被选举为领导时，会向自己发送RevokedLeadership消息
    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      // Master将退出
      System.exit(0)

    /**
     * Driver在启动后需要向Master注册Application信息，
     * 这样Master就能将Worker上的资源及Executor分配给Driver。
     * Driver通过向Master发送RegisterApplication消息来注册Application信息。
     */
    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      if (state == RecoveryState.STANDBY) {
        // 如果Master当前的状态是STANDBY，那么不作任何处理
        // ignore, don't send response
      } else {
        // Master的状态不是STANDBY
        logInfo("Registering app " + description.name)
        /**
         * 创建ApplicationInfo，会给Application分配ID。
         * 此ID的生成规则为app-${yyyyMMddHHmmss}-${nextAppNumber}。
         */
        val app = createApplication(description, driver)
        // 注册ApplicationInfo
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        // 对ApplicationInfo持久化
        persistenceEngine.addApplication(app)

        /**
         * 发送RegisteredApplication消息，以表示注册Application信息成功。
         * 该消息将携带如下信息：
         * - 为Application生成的ID
         * - Master自身的RpcEndpointRef
         */
        driver.send(RegisteredApplication(app.id, self))
        // 进行资源调度
        schedule()
      }

    /**
      * Executor在运行的整个生命周期中，
      * 会向Master不断发送ExecutorStateChanged消息报告自身的状态改变。
       */
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      // 获取Executor对应的ExecutorDesc
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          // 获取对应的Application ID
          val appInfo = idToApp(appId)
          // 获取Executor当前的状态
          val oldState = exec.state
          // 更新Executor状态
          exec.state = state

          if (state == ExecutorState.RUNNING) { // 更新的状态为RUNNING
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            // 重置ApplicationInfo的_retryCount为0
            appInfo.resetRetryCount()
          }

          // 向Executor上Application所在的Driver发送ExecutorUpdated消息
          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, false))

          if (ExecutorState.isFinished(state)) { // 更新状态为KILLED、FAILED、LOST或EXITED
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              // 如果Application还未完成，维护对应ApplicationInfo中对该Executor的记录
              appInfo.removeExecutor(exec)
            }

            // 维护对应Worker中对该Executor的记录
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            /**
              * 如果Executor非正常退出且重试次数已经超过了MAX_EXECUTOR_RETRIES的限制，
              * 那么Executor所属的Application在没有任何Executor处于RUNNING状态时将被彻底移除。
              */
            if (!normalExit
                && appInfo.incrementRetryCount() >= MAX_EXECUTOR_RETRIES
                && MAX_EXECUTOR_RETRIES >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }

    case DriverStateChanged(driverId, state, exception) =>
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    /**
      * 为了让Master及时得知Worker的最新状态，Worker需要向Master发送心跳，
      * Master将根据Worker的心跳更新Worker的最后心跳时间，以便为整个集群的健康工作提供参考。
      */
    case Heartbeat(workerId, worker) =>
      // 从idToWorker中找出缓存的WorkerInfo
      idToWorker.get(workerId) match {
        case Some(workerInfo) => // 已经缓存了对应的WorkerInfo
          // 将WorkerInfo的最后心跳时间（lastHeartbeat）更新为系统当前时间的时间戳
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None => // 如果idToWorker中没有缓存的WorkerInfo
          if (workers.map(_.id).contains(workerId)) {
            /**
              * workers中有对应的WorkerInfo，
              * 说明定时任务checkForWorkerTimeOutTask()检查到Worker超时，但是WorkerInfo的状态不是DEAD，
              * 那么在调用removeWorker()方法时将WorkerInfo从idToWorker中清除，此时的workers中仍然持有WorkerInfo。
              *
              * 向Worker发送ReconnectWorker消息。
              */
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            /**
              * workers中也没有对应的WorkerInfo，
              * 说明checkForWorkerTimeOutTask已经发现Worker很长时间没有心跳，
              * 并且WorkerInfo的状态为DEAD后，将WorkerInfo从workers中也移除了。
              */
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    /**
      * Worker在向Master注册成功后，会向Master发送WorkerLatestState消息。
      * WorkerLatestState消息将携带：
      * - Worker的身份标识
      * - Worker节点的所有Executor的描述信息
      * - 调度到当前Worker的所有Driver的身份标识
      */
    case WorkerLatestState(workerId, executors, driverIds) =>
      // 根据Worker的身份标识，从idToWorker取出注册的WorkerInfo
      idToWorker.get(workerId) match {
        case Some(worker) =>
          // 遍历executors中的每个ExecutorDescription
          for (exec <- executors) {
            // 与WorkerInfo的executors中保存的ExecutorDesc按照应用ID和Executor ID进行匹配
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) { // 匹配不成功
              // master doesn't recognize this executor. So just tell worker to kill it.
              // 向Worker发送KillExecutor消息，杀死不匹配的Executor
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          // 遍历driverIds中的每个Driver ID
          for (driverId <- driverIds) {
            // 与WorkerInfo的drivers中保存的Driver ID进行匹配
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              // 向Worker发送KillDriver消息，杀死不匹配的Driver
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForWorkerTimeOut =>
      // 处理CheckForWorkerTimeOut消息，检查Worker是否超时
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // 处理Worker的注册
    case RegisterWorker(
        id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) { // 如果Master的状态是STANDBY
        // 向Worker回复MasterInStandby消息
        context.reply(MasterInStandby)
      } else if (idToWorker.contains(id)) { // Worker重复注册
        // 向Worker回复RegisterWorkerFailed消息
        context.reply(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        // 创建WorkerInfo
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl)
        if (registerWorker(worker)) { // 注册WorkerInfo
          // 持久化引擎对WorkerInfo持久化
          persistenceEngine.addWorker(worker)
          // 向Worker回复RegisteredWorker消息
          context.reply(RegisteredWorker(self, masterWebUiUrl))
          // 对资源进行调度
          schedule()
        } else { // 其他原因导致的注册失败
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          // 向Worker回复RegisterWorkerFailed消息
          context.reply(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    case RequestSubmitDriver(description) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

    case RequestKillDriver(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    /**
      * 如果Driver启用了ExecutorAllocationManager，
      * 那么ExecutorAllocationManager将通过StandaloneAppClient中的
      * ClientEndpoint向Master发送RequestExecutors消息请求Executor。
      */
    case RequestExecutors(appId, requestedTotal) =>
      // 调用handleRequestExecutors()方法进行处理
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker)
    // 调用finishApplication()方法处理Application
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    // 遍历从持久化引擎中读取的ApplicationInfo
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        // 注册ApplicationInfo
        registerApplication(app)
        // 将ApplicationInfo的状态设置为UNKNOWN
        app.state = ApplicationState.UNKNOWN

        /**
          * 让Driver切换Master后重连，
          * 发送的MasterChanged消息携带了被选举为领导的Master和此Master的masterWebUiUrl属性
          */
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    // 遍历从持久化引擎中读取的DriverInfo
    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      // 注册DriverInfo
      drivers += driver
    }

    // 遍历从持久化引擎中读取的WorkerInfo
    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        // 注册WorkerInfo
        registerWorker(worker)
        // 将WorkerInfo的状态修改为UNKNOWN
        worker.state = WorkerState.UNKNOWN

        /**
          * 让Worker切换Master后重连，
          * 发送的MasterChanged消息携带了被选举为领导的Master和此Master的masterWebUiUrl属性
          */
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    // 如果Master的状态不是RECOVERING，直接返回
    if (state != RecoveryState.RECOVERING) { return }

    // 将Master的状态修改为COMPLETING_RECOVERY
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    // 将workers中状态为UNKNOWN的所有WorkerInfo，通过调用removeWorker()方法从Master中移除。
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    // 将apps中状态为UNKNOWN的所有ApplicationInfo，通过调用finishApplication()方法从Master中移除。
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    // 从drivers中过滤出还没有分配Worker的所有DriverInfo，进行遍历
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) { // 由集群监管的Driver
        logWarning(s"Re-launching ${d.id}")
        // 重新调度运行指定的Driver
        relaunchDriver(d)
      } else {
        // 否则移除Master维护的关于指定Driver的相关信息和状态
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    // 将Master的状态设置为ALIVE
    state = RecoveryState.ALIVE
    // 调用schedule方法进行资源调度
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    // Application要求的每个Executor所需的内核数
    val coresPerExecutor = app.desc.coresPerExecutor
    // Application要求的每个Executor所需的最小内核数
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    /**
      * 是否在每个Worker上只分配一个Executor。
      * 当Application没有配置coresPerExecutor时，oneExecutorPerWorker为true。
      */
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    // Application要求的每个Executor所需的内存大小（单位为字节）。
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    /**
      * usableWorkers用于缓存从workers中选出的状态为ALIVE、空闲空间满足ApplicationInfo要求的
      * 每个Executor使用的内存大小、空闲内核数满足coresPerExecutor的所有WorkerInfo。
      *
      * numUsable表示可用的Worker的数量，即usableWorkers的大小
      */
    val numUsable = usableWorkers.length
    // 用于保存每个Worker给Application分配的内核数的数组。通过数组索引与usableWorkers中的WorkerInfo相对应。
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    // 用于保存每个Worker给应用分配的Executor数的数组。通过数组索引与usableWorkers中的WorkerInfo相对应。
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    /**
      * 给Application要分配的内核数。
      * coresToAssign取usableWorkers中所有WorkerInfo的空闲内核数之和与Application还需的内核数中的最小值。
      */
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app.
      * 用于判断usableWorkers中索引为参数pos指定的位置上的WorkerInfo是否能运行Executor
      **/
    def canLaunchExecutor(pos: Int): Boolean = {
      // 条件一：给Application要分配的内核数满足（大于等于）Application要求的每个Executor所需的最小内核数
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      // 条件二：可用Worker剩余的空闲CPU Core数量是否满足（大于等于）Application要求的每个Executor所需的最小内核数
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      // 判断是否每个Worker上可以启动多个Executor，或者对应Worker上还没有分配Executor
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) { // 满足判断条件
        // 计算需要的内存
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        // 判断能存是否足够
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        // 判断Application申请的Executor是否达到了上限
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        // 根据判断条件决定是否可以启动Executor
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // 每个Worker上只能启动一个Executor，且该Worker上已经分配过Executor了
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    // 获取所有可以运行Executor的Worker的索引
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    // 当freeWorkers不为空
    while (freeWorkers.nonEmpty) {
      // 那么遍历freeWorkers中的每一个索引位置
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        // 分配内核、Executor
        while (keepScheduling && canLaunchExecutor(pos)) {
          // 维护CPU Core相关计数
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 维护Executor计数
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          /**
            * 如果spreadOutApps为true，则将keepScheduling设置为false，
            * 这会导致对pos位置上的WorkerInfo的资源调度提前结束，
            * 那么应用需要的其他Executor资源将会在其他WorkerInfo上调度。
            *
            * 如果spreadOutApps为false，
            * 那么应用需要的Executor资源将会不断从pos位置的WorkerInfo上调度，
            * 直到pos位置的WorkerInfo上的的资源被使用完。
            */
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      // 更新空闲的Worker
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
    *
    * 对Application的Executor资源的调度
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps if app.coresLeft > 0) {
      // 获取Application要求的每个Executor使用的内核数coresPerExecutor
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
      // Filter out workers that don't have enough resources to launch an executor
      val usableWorkers = workers.toArray
        // 找出workers中状态为ALIVE
        .filter(_.state == WorkerState.ALIVE)
        .filter(worker =>
          // 空闲空间满足Application要求的每个Executor使用的内存大小
          worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          // 空闲内核数满足coresPerExecutor
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        // 按照空闲内核数倒序排列
        .sortBy(_.coresFree).reverse

      // 在Worker上进行Executor的调度，返回在各个Worker上分配的内核数
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        // 将Worker上的资源分配给Executor
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
    *
    * 用于将Worker的资源分配给Executor，并运行Executor
    *
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    /**
      * 根据Worker分配给Application的内核数（assignedCores）与每个Executor需要的内核数（coresPerExecutor），
      * 计算在Worker上要运行的Executor数量（numExecutors）。
      * 如果没有指定coresPerExecutor，说明assignedCores指定的所有内核都由一个Executor使用。
      */
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    /**
      * 计算给Executor分配的内核数（coresToAssign）。
      * 如果指定了coresPerExecutor，那么coresToAssign等于coresPerExecutor，
      * 否则coresToAssign等于assignedCores（即将所有内核分配给一个Executor使用）。
      */
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    // 循环numExecutors次
    for (i <- 1 to numExecutors) {
      // 调用launchExecutor方法，在Worker上创建Executor
      val exec = app.addExecutor(worker, coresToAssign)
      // 运行Executor
      launchExecutor(worker, exec)
      // 将ApplicationInfo的状态设置为RUNNING
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
    *
    * 主要完成对Driver的资源调度
   */
  private def schedule(): Unit = {
    // 如果Master的状态不是ALIVE则直接返回
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    // 从workers中过滤出状态为ALIVE的WorkerInfo
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    // 计算状态为ALIVE的WorkerInfo的数量
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    // 遍历waitingDrivers中正在等待的Driver对应的DriverInfo对象
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      // 遍历状态为ALIVE的WorkerInfo对象
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        // 判断Worker的内存大小和内核数是否都满足Driver的需要
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          // 如果满足，则将该Worker作为Driver并运行
          launchDriver(worker, driver)
          // 将该Worker从waitingDrivers中移除
          waitingDrivers -= driver
          // 标记launched为true
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }

    // 在Worker上启动Executor
    startExecutorsOnWorkers()
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    // 添加新的Executor的信息
    worker.addExecutor(exec)

    /**
      * 向Worker发送LaunchExecutor消息，LaunchExecutor消息携带着下列信息：
      * - masterUrl
      * - Application的ID
      * - Executor的ID
      * - Application的描述信息ApplicationDescription
      * - Executor分配获得的内核数
      * - Executor分配获得的内存大小
      */
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))

    /**
      * 向提交应用的Driver发送ExecutorAdded消息，ExecutorAdded消息携带着下列信息：
      * - 此消息携带着Executor的ID
      * - Worker的ID
      * - Worker的host和port
      * - Executor分配获得的内核数
      * - Executor分配获得的内存大小
      */
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  // 将WorkerInfo添加到workers、idToWorker、addressToWorker等缓存中。
  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    // 从workers中移除host和port与要注册的WorkerInfo的host和port一样，且状态为DEAD的WorkerInfo。
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) { // addressToWorker中包含地址相同的WorkerInfo
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) { // 此WorkerInfo的状态为UNKNOWN
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        // 调用removeWorker()方法，移除此WorkerInfo的相关状态
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        // 否则返回false
        return false
      }
    }

    // 将要注册的WorkerInfo添加到workers、idToWorker、addressToWorker等缓存中
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker

    // 对WebUI的反向代理进行设置
    if (reverseProxy) {
       webUi.addProxyTargets(worker.id, worker.webUiAddress)
    }
    true
  }

  // 用于移除Master维护的关于指定Worker的相关信息和状态
  private def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    // 将WorkerInfo的状态置为DEAD
    worker.setState(WorkerState.DEAD)
    // 从idToWorker和addressToWorker等缓存中移除WorkerInfo
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    // 移除代理信息
    if (reverseProxy) {
      webUi.removeProxyTargets(worker.id)
    }

    // 将Worker上的Executor都作为丢失状态处理
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      // 向此Worker的所有Executor所服务的Driver发送ExecutorUpdated消息
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
      // 更新Executor的状态为LOST
      exec.state = ExecutorState.LOST
      // 移除分配给Application的Executor的描述信息
      exec.application.removeExecutor(exec)
    }
    // 对Worker所处理的所有Driver进行处理
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) { // 如果Driver是被监管的
        logInfo(s"Re-launching ${driver.id}")
        // 重新调度运行Driver
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        // 移除Driver的相关信息和状态
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    // 移除WorkerInfo的持久化数据
    persistenceEngine.removeWorker(worker)
  }

  // 用于重新调度运行指定的Driver
  private def relaunchDriver(driver: DriverInfo) {
    // 表示Driver提交的应用没有被任何Worker处理
    driver.worker = None
    // 将DriverInfo的状态置为重新运行（RELAUNCHING）
    driver.state = DriverState.RELAUNCHING
    // 将DriverInfo放入所有等待调度的Driver的集合waitingDrivers中
    waitingDrivers += driver
    // 调用schedule()方法对Driver重新调度运行
    schedule()
  }

  // 创建Application
  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    // 生成Application ID
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  // 将ApplicationInfo添加到apps、idToApp、endpointToApp、addressToApp、waitingApps等缓存中
  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
    if (reverseProxy) {
      webUi.addProxyTargets(app.id, app.desc.appUiUrl)
    }
  }

  private def finishApplication(app: ApplicationInfo) {
    // 调用了removeApplication()方法移除Application及分配给Application的Executor
    removeApplication(app, ApplicationState.FINISHED)
  }

  // 用于移除Master中缓存的Application及Application相关的Driver信息
  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      // 移除Master中缓存的ApplicationInfo及ApplicationInfo相关的DriverInfo
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (reverseProxy) {
        webUi.removeProxyTargets(app.id)
      }
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }

      // 将ApplicationInfo作为已完成的历史应用
      completedApps += app // Remember it in our history
      waitingApps -= app

      // 遍历分配给Application的Executor
      for (exec <- app.executors.values) {
        // 杀死Executor
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        // 向DriverEndpoint发送ApplicationRemoved消息，告诉Driver驱动的Application已经被移除。
        app.driver.send(ApplicationRemoved(state.toString))
      }

      // 移除ApplicationInfo的持久化信息
      persistenceEngine.removeApplication(app)

      // 调用schedule方法进行资源调度
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      // 向每个Worker发送ApplicationFinished消息，以告知Application已完成。
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    // 根据Application ID获取对应的ApplicationInfo对象
    idToApp.get(appId) match {
      case Some(appInfo) => // 能够获取到
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        // 更改ApplicationInfo的Executor总数
        appInfo.executorLimit = requestedTotal
        // 进行资源调度
        schedule()
        true
      case None => // 不能获取到，不做任何处理
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    // 过滤出所有超时的Worker，过滤条件是最后收到的心跳时间距离当前时间超过WORKER_TIMEOUT_MS
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    // 遍历超时的Worker
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) { // Worker状态不为DEAD
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        // 移除Worker
        removeWorker(worker)
      } else { // Worker状态为DEAD
        // 等待足够长的时间后将它从workers列表中移除
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  // 用于运行Driver
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    // 在WorkerInfo和DriverInfo之间建立关系，表示Driver被调度到Worker上运行
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 向Worker发送LaunchDriver消息，Worker接收到LaunchDriver消息后将运行Driver
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    // 将DriverInfo的状态修改为RUNNING
    driver.state = DriverState.RUNNING
  }

  // 用于移除Master维护的关于指定Driver的相关信息和状态
  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]) {
    // 从DriverInfo的集合drivers中找到指定的DriverInfo
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        // 从drivers中移除找到的DriverInfo
        drivers -= driver
        // 如果已经完成的DriverInfo的集合completedDrivers中的元素数量大于等于RETAINED_DRIVERS
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          // 移除completedDrivers中开头的一些DriverInfo
          completedDrivers.trimStart(toRemove)
        }
        // 将找到的DriverInfo放入completedDrivers集合中
        completedDrivers += driver
        // 移除DriverInfo的持久化数据
        persistenceEngine.removeDriver(driver)
        // 对DriverInfo的一些属性进行修改
        driver.state = finalState
        driver.exception = exception
        // 移除Worker处理的Driver信息
        driver.worker.foreach(w => w.removeDriver(driver))
        // 由于腾出了Driver占用的资源，所以对其他Application和Driver进行调度
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  // 以JVM进程方式启动Master时的入口方法
  def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    // 创建SparkConf
    val conf = new SparkConf
    /**
      * 解析传入的参数。
      * 命令行参数指定的值会覆盖系统环境变量指定的值。
      * 属性指定的值会覆盖系统环境变量或命令行参数的值。
      */
    val args = new MasterArguments(argStrings, conf)
    // 调用startRpcEnvAndEndpoint()方法启动
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    // 该操作最后调用了Dispatcher中线程池的awaitTermination()方法
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
    *
    * 用于创建Master对象，并将Master对象注册到RpcEnv中完成对Master对象的启动。
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    // 创建SecurityManager
    val securityMgr = new SecurityManager(conf)
    // 创建RpcEnv
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    // 创建Master，将Master（Master继承了ThreadSafeRpcEndpoint）注册到RpcEnv中，获得Master的RpcEndpointRef。
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    // 向Master发送BoundPortsRequest消息，并获得返回的BoundPortsResponse消息。
    val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    // 返回创建的RpcEnv、BoundPortsResponse消息携带的WebUIPort、REST服务的端口（restPort）等信息。
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
