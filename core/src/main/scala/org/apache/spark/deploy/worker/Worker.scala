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

package org.apache.spark.deploy.worker

import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}

import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
  * Worker是Spark在local-cluster部署模式和Standalone部署模式中对工作节点的资源和Executor进行管理的服务。
  * Worker一方面向Master汇报自身所管理的资源信息，一方面接收Master的命令运行Driver或者为Application运行Executor。
  * 同一个机器上可以同时部署多个Worker服务，一个Worker也可以启动多个Executor。
  * 当Executor完成后，Worker将回收Executor使用的资源。
  *
  * @param rpcEnv
  * @param webUiPort 参数指定的WebUI的端口
  * @param cores 内核数
  * @param memory 内存大小
  * @param masterRpcAddresses Master的RpcEnv地址（即RpcAddress）的数组。
  *                           由于一个集群为了可靠性和容错，需要多个Master节点，因此用数组来存储它们的RpcEnv地址。
  * @param endpointName Worker注册到RpcEnv的名称
  * @param workDirPath 字符串表示的Worker的工作目录
  * @param conf
  * @param securityMgr
  */
private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager)
  extends ThreadSafeRpcEndpoint with Logging {

  // Worker的RpcEnv的host
  private val host = rpcEnv.address.host
  // Worker的RpcEnv的端口
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  // A scheduled executor used to send messages at the specified time.
  /**
    * 用于发送消息的调度执行器。
    * forwordMessageScheduler只能调度执行一个线程，执行的线程以worker-forward-message-scheduler作为名称。
    */
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir. Used to provide the implicit parameter of `Future`
  // methods.
  /**
    * 通过Executors的newSingleThreadExecutor方法创建的线程执行器，
    * 用于清理Worker的工作目录。由cleanupThreadExecutor执行的线程以worker-cleanup-thread作为名称。
    */
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  // 向Master发送心跳的时间间隔，是spark.worker.timeout属性值的1/4，默认为15秒。
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  // 固定为6，代表连接Master的前六次尝试。
  private val INITIAL_REGISTRATION_RETRIES = 6
  // 固定为16，代表连接Master最多有16次尝试。
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  // 固定为0.500，是为了确保尝试的时间间隔不至于过小的下边界。
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  /**
    * 0～1范围的随机数与FUZZ_MULTIP-LIER_INTERVAL_LOWER_BOUND的和，
    * 所以REGISTRATION_RETRY_FUZZ_MU-LTIPLIER的真实范围在0.5～1.5之间。
    * 加入随机数是为了避免各个Worker在同一时间发送心跳。
    */
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }

  /**
    * 代表前六次尝试的时间间隔。
    * INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS取10和
    * REGIST-RATION_RETRY_FUZZ_MULTIPLIER乘积的最近整数，
    * 因此这六次尝试的时间间隔在5～15s之间。
    */
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  /**
    * 代表最后十次尝试的时间间隔。
    * PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS取60和
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER乘积的最近整数，
    * 因此最后十次尝试的时间间隔在30～90s之间。
    */
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  /**
    * 是否对旧的Application产生的文件夹及文件进行清理。
    * 可通过spark.worker.cleanup.enabled属性配置，默认为false。
    */
  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  /**
    * 对旧的Application产生的文件夹及文件进行清理的时间间隔。
    * 可通过spark.worker.cleanup.interval属性配置，默认为1800秒。
    */
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  /**
    * Application产生的文件夹及文件保留的时间。
    * 可通过spark.worker.cleanup.appDataTtl属性配置，默认为7天。
    */
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  private val testing: Boolean = sys.props.contains("spark.testing")
  // Master的RpcEndpointRef。
  private var master: Option[RpcEndpointRef] = None
  // 处于激活（ALIVE）状态的Master的URL。
  private var activeMasterUrl: String = ""
  // 处于激活（ALIVE）状态的Master的WebUI的URL。
  private[worker] var activeMasterWebUiUrl : String = ""
  // Worker的WebUI的URL。
  private var workerWebUiUrl: String = ""
  // Worker的URL，格式为spark://$name@${host}:${port}
  private val workerUri = RpcEndpointAddress(rpcEnv.address, endpointName).toString
  // 标记Worker是否已经注册到Master。
  private var registered = false
  // 标记Worker是否已经连接到Master。
  private var connected = false
  // Worker的身份标识。
  private val workerId = generateWorkerId()
  // 环境变量SPARK_HOME的值。
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  // 由java.io.File表示的Worker的工作目录。
  var workDir: File = null
  // 已经完成的Executor的身份标识与ExecutorRunner之间的映射关系。
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  // Driver的身份标识与DriverRunner之间的映射关系。
  val drivers = new HashMap[String, DriverRunner]
  // Executor的身份标识与ExecutorRunner之间的映射关系。
  val executors = new HashMap[String, ExecutorRunner]
  // 已经完成的Driver的身份标识与DriverRunner之间的映射关系。
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  // Application的ID与对应的目录集合之间的映射关系。
  val appDirectories = new HashMap[String, Seq[String]]
  // 已经完成的Application的ID的集合。
  val finishedApps = new HashSet[String]

  // 保留的Executor的数量。可通过spark.worker.ui.retainedExecutors属性配置，默认为1000。
  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
  // 保留的Driver的数量。可通过spark.worker.ui.retainedDrivers属性配置，默认为1000。
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
  /**
    * 外部的Shuffle服务。这里的服务端实现类是ExternalShuffleService。
    * 虽然创建了ExternalShuffleService，但是只有配置了外部的Shuffle服务时才会启动它。
    */
  private val shuffleService = new ExternalShuffleService(conf, securityMgr)

  // Worker的公共地址，如果设置了环境变量SPARK_PUBLIC_DNS，则为环境变量SPARK_PUBLIC_DNS的值，否则为host。
  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  // Worker的WebUI，类型为WebUI的子类WorkerWebUI。
  private var webUi: WorkerWebUI = null

  // 连接尝试次数的计数器。
  private var connectionAttemptCount = 0

  // Worker的度量系统
  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  // 有关Worker的度量来源
  private val workerSource = new WorkerSource(this)

  // 由于Worker向Master进行注册的过程是异步的，此变量保存线程返回的java.util.concurrent.Future。
  private var registerMasterFutures: Array[JFuture[_]] = null
  // Worker向Master进行注册重试的定时器。
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  // Worker向Master进行注册的线程池。此线程池的大小与Master节点的数量一样，启动的线程以worker-register-master-threadpool为前缀。
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )

  // 当前Worker已经使用的内核数。
  var coresUsed = 0
  // 当前Worker已经使用的内存大小。
  var memoryUsed = 0

  // 空闲内核数
  def coresFree: Int = cores - coresUsed
  // 空闲内存大小
  def memoryFree: Int = memory - memoryUsed

  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      // 创建目录
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  // 在向RpcEnv中注册Worker时，会触发对Worker的onStart()方法
  override def onStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    // 创建Worker的工作目录
    createWorkDir()
    // 启动外部Shuffle服务
    shuffleService.startIfEnabled()
    // 创建Worker的Web UI并绑定端口
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()

    // 拼接Worker的WebUI的URL，并保存到workerWebUiUrl属性
    workerWebUiUrl = s"http://$publicAddress:${webUi.boundPort}"

    // 向Master注册Worker
    registerWithMaster()

    // 将workerSource注册到metricsSystem，启动metricsSystem
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    // 将metricsSystem的ServletContextHandler添加到WorkerWebUI
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  // 修改Worker中保存的激活的Master信息
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    // 修改各类字段
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    master = Some(masterRef)
    connected = true
    if (conf.getBoolean("spark.ui.reverseProxy", false)) {
      logInfo(s"WorkerWebUI is available at $activeMasterWebUiUrl/proxy/$workerId")
    }
    // Cancel any outstanding re-registration attempts because we found a new master
    // 取消注册尝试，即取消了调用tryRegisterAllMasters()方法产生的注册任务和registrationRetryTimer
    cancelLastRegistrationRetry()
  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    // 遍历masterRpcAddresses中的每个Master的RPC地址
    masterRpcAddresses.map { masterAddress =>
      // 然后向registerMasterThreadPool提交向Master注册Worker的任务。
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            // 调用了registerWithMaster()方法完成注册
            registerWithMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress = masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  registerWithMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registeration retry, or do nothing if no retry
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  /**
    * Worker在启动后，需要加入到Master管理的整个集群中，以参与Driver、Executor的资源调度。
    * Worker要加入Master管理的集群，就必须将Worker注册到Master。
    */
  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        // 向所有的Master注册当前Worker
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        // 创建定时任务，定时向Worker自身发送ReregisterWithMaster消息
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              // 发送ReregisterWithMaster消息
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  private def registerWithMaster(masterEndpoint: RpcEndpointRef): Unit = {
    /**
      * 向Master发送RegisterWorker消息，消息携带信息如下：
      * - Worker的ID
      * - host
      * - port
      * - 内核数
      * - 内存大小
      */
    masterEndpoint.ask[RegisterWorkerResponse](RegisterWorker(
      workerId, host, port, self, cores, memory, workerWebUiUrl))
      .onComplete {
        // This is a very fast action so we can use "ThreadUtils.sameThread"
        // 发送且收到成功的响应
        case Success(msg) =>
          Utils.tryLogNonFatalError {
            // 返回结果由handleRegisterResponse()方法处理
            handleRegisterResponse(msg)
          }
        case Failure(e) =>
          logError(s"Cannot register with master: ${masterEndpoint.address}", e)
          System.exit(1)
      }(ThreadUtils.sameThread)
  }

  // 处理Worker向Master注册的响应，有三种可能
  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
      // 如果Master回复了RegisteredWorker消息，说明Worker已经成功在Master上注册
      case RegisteredWorker(masterRef, masterWebUiUrl) =>
        logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
        // 标记已注册
        registered = true
        // 切换Master，修改激活的Master的信息
        changeMaster(masterRef, masterWebUiUrl)
        // 提交以HEARTBEAT_MILLIS作为间隔向Worker自身发送SendHeartbeat消息的定时任务
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // 向自己发送SendHeartbeat消息，以便向Master发送心跳
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        if (CLEANUP_ENABLED) { // 如果允许清理工作目录
          logInfo(
            s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          // 提交以CLEANUP_INTERVAL_MILLIS作为间隔向Worker自身发送WorkDirCleanup消息的定时任务
          forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              // 定时清理工作目录
              self.send(WorkDirCleanup)
            }
          }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

        val execs = executors.values.map { e =>
          new ExecutorDescription(e.appId, e.execId, e.cores, e.state)
        }

        /**
          * 发送汇报状态的消息，WorkerLatestState消息携带了以下信息：
          * - Worker的身份标识
          * - Worker节点的所有Executor的描述信息
          * - 调度到当前Worker的所有Driver的身份标识
          */
        masterRef.send(WorkerLatestState(workerId, execs.toList, drivers.keys.toSeq))

      // 如果Master回复了RegisterWorkerFailed消息，说明Worker在Master上注册失败了。
      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          // 如果Worker还未向任何Master节点注册成功，那么退出Worker进程。
          System.exit(1)
        }

      // 如果Master回复了RegisterWorkerFailed消息，说明Master还未准备好或者处于Standby的状态，并不是领导身份。
      case MasterInStandby => // 不做任何处理
        // Ignore. Master not yet ready.
    }
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    // 在Worker向Maseter成功后，会向自己发送SendHeartbeat消息
    case SendHeartbeat =>
      // 当连接到Master时，调用sendToMaster()方法
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = executors.values.map(_.appId).toSet
      val cleanupFuture = concurrent.Future {
        val appDirs = workDir.listFiles()
        if (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        appDirs.filter { dir =>
          // the directory is used by an application - check that the application is not running
          // when cleaning up
          val appIdFromDir = dir.getName
          val isAppStillRunning = appIds.contains(appIdFromDir)
          dir.isDirectory && !isAppStillRunning &&
          !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir)
        }
      }(cleanupThreadExecutor)

      cleanupFuture.onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }(cleanupThreadExecutor)

    // 当Master被选举为领导时，将会向Worker发送MasterChanged消息
    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      // 改变激活的Master的相关信息
      changeMaster(masterRef, masterWebUiUrl)

      // 遍历executors中的每个ExecutorRunner
      val execs = executors.values.
        // 利用ExecutorRunner的信息创建ExecutorDescription
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))

      /**
        * 向Master发送WorkerSchedulerStateResponse消息，该消息携带了如下信息：
        * - Worker的ID
        * - ExecutorDescription列表
        * - 所有Driver的ID
        *
        * Master接收到WorkerSchedulerStateResponse消息后将更新Worker的调度状态。
        */
      masterRef.send(WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq))

    // Master在处理Heartbeat消息时有可能向Worker回复ReconnectWorker消息
    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      // 向Master重新注册Worker
      registerWithMaster()

    /**
      * Master向Worker发送LaunchExecutor消息以运行Executor，该消息携带了以下信息：
      * - masterUrl
      * - Application的ID
      * - Executor的ID
      * - 应用的描述信息
      * - Executor分配获得的内核数
      * - Executor分配获得的内存大小
      */
    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      // 判断发送LaunchExecutor消息的Master是否是激活的Master
      if (masterUrl != activeMasterUrl) {
        // 如果不是，则什么都不做
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          // 在Worker的工作目录下创建Executor的工作目录
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          /**
            * 给Executor创建本地目录，并将这些目录缓存到Worker的appDirectories属性中。
            * 这些目录将通过环境变量SPARK_EXECUTOR_DIRS传递给Executor，并在Application完成后由Worker删除。
            */
          val appLocalDirs = appDirectories.getOrElse(appId,
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              val appDir = Utils.createDirectory(dir, namePrefix = "executor")
              Utils.chmod700(appDir)
              appDir.getAbsolutePath()
            }.toSeq)
          appDirectories(appId) = appLocalDirs
          // 创建ExecutorRunner，并将ExecutorRunner放入executors。
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager
          // 启动ExecutorRunner
          manager.start()
          // 更新一集使用的内核数和已经使用的内存大小
          coresUsed += cores_
          memoryUsed += memory_
          // 向Master发送ExecutorStateChanged消息以更新Executor的状态
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          case e: Exception =>
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
        }
      }

    // Worker接收ExecutorStateChanged消息，以对Executor的状态发生变化后进行处理
    case executorStateChanged @ ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      handleExecutorStateChanged(executorStateChanged)

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to kill executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    // Master将向Worker发送LaunchDriver消息以运行Driver
    case LaunchDriver(driverId, driverDesc) =>
      logInfo(s"Asked to launch driver $driverId")
      // 创建DriverRunner
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      // 将Driver的身份标识与DriverRunner的关系放入drivers中缓存
      drivers(driverId) = driver
      // 调用DriverRunner的start()方法启动DriverRunner
      driver.start()

      // 更新Worker已经使用的内核数和内存大小
      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem

    case KillDriver(driverId) =>
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }

    case driverStateChanged @ DriverStateChanged(driverId, state, exception) =>
      handleDriverStateChanged(driverStateChanged)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      appDirectories.remove(id).foreach { dirList =>
        logInfo(s"Cleaning up local directories for application $id")
        dirList.foreach { dir =>
          Utils.deleteRecursively(new File(dir))
        }
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      // 向Master发送消息
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop() {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) {
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
    state match {
      case DriverState.ERROR =>
        logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
      case DriverState.FAILED =>
        logWarning(s"Driver $driverId exited with failure")
      case DriverState.FINISHED =>
        logInfo(s"Driver $driverId exited successfully")
      case DriverState.KILLED =>
        logInfo(s"Driver $driverId was killed by user")
      case _ =>
        logDebug(s"Driver $driverId changed state to $state")
    }
    sendToMaster(driverStateChanged)
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    trimFinishedDriversIfNecessary()
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
  }

  // 处理Executor的状态变化。
  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged):
    Unit = {
    // 向Master转发ExecutorStateChanged消息
    sendToMaster(executorStateChanged)
    // 获取ExecutorStateChanged消息中携带的Executor的状态
    val state = executorStateChanged.state
    if (ExecutorState.isFinished(state)) { // 是否为KILLED、FAILED、LOST或EXITED
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {
        case Some(executor) => // 更新相关的缓存信息
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
          executors -= fullId
          finishedExecutors(fullId) = executor
          trimFinishedExecutorsIfNecessary()
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      // 可能需要清理应用
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  // 以JVM进程方式启动Worker
  def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    // 创建SparkConf
    val conf = new SparkConf
    // 解析参数，将Spark属性配置文件中以spark.开头的属性保存到SparkConf。
    val args = new WorkerArguments(argStrings, conf)
    // 调用startRpcEnvAndEndpoint()方法创建并启动Worker对象
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir, conf = conf)
    // 该操作最后调用了Dispatcher中线程池的awaitTermination()方法
    rpcEnv.awaitTermination()
  }

  // 以对象方式启动Worker
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    // 成Worker的RpcEnv的名称，格式为sparkWorker + "Woker标识号"
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    // 创建SecurityManager
    val securityMgr = new SecurityManager(conf)
    // 创建RpcEnv
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    // 将所有Master的Spark URL（格式为spark://host:port）转换为RpcAddress地址。RpcAddress只包含host和port两个属性。
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    // 创建Worker，并且将Worker（Worker也继承了ThreadSafeRpcEndpoint）注册到刚创建的RpcEnv中。
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    // 返回Worker的RpcEnv
    rpcEnv
  }

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val pattern = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r
    val result = cmd.javaOpts.collectFirst {
      case pattern(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
