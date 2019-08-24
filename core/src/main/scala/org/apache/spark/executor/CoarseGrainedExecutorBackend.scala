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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * CoarseGrainedExecutorBackend是Driver与Executor通信的后端接口，但只在local-cluster和Standalone模式下才会使用。
 * 由于CoarseGrainedExecutorBackend作为单独的进程存在，所以CoarseGrainedExecutorBackend的伴生对象中实现了main()方法。
 * 在main()方法中将创建CoarseGrainedExecutorBackend实例。
 *
 * @param rpcEnv
 * @param driverUrl     Driver的Spark格式的URL。例如，spark://CoarseGrainedScheduler@host:port。
 * @param executorId    Master分配给Executor的身份标识
 * @param hostname      主机名
 * @param cores         分配给Executor的内核数
 * @param userClassPath 用户指定的类路径
 * @param env           Executor所需的SparkEnv
 */
private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  // 标记CoarseGrainedExecutorBackend是否正在停止
  private[this] val stopping = new AtomicBoolean(false)
  // Executor实例
  var executor: Executor = null
  // DriverEndpoint的RpcEndpointRef
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  // Executor所需的SparkEnv中的closureSerializer的实例
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  /**
   * 在创建CoarseGrainedExecutorBackend时，并将它注册到Executor自身的RpcEnv中。
   * 在注册到RpcEnv的过程中会触发对CoarseGrainedExecutorBackend的onStart()方法的调用。
   */
  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    // 获取Driver的RpcEndpoint的RpcEndpointRef
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      // 向Driver的RpcEndpoint发送RegisterExecutor消息注册Executor
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        // Always receive `true`. Just ignore it
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }

  // 接收消息，对RegisteredExecutor、LaunchTask、KillTask、StopExecutor等消息进行处理
  override def receive: PartialFunction[Any, Unit] = {
    // DriverEndpoint回复RegisteredExecutor，表示Executor注册成功
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        // 创建Executor实例，并由executor字段持有
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)

    case LaunchTask(data) =>
      // 判断Executor是否被创建了
      if (executor == null) { // Executor还未创建
        // 告诉DriverExecutor丢失
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else { // Executor创建成功
        // 反序列化Task的TaskDescription数据
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        // 调用Executor的launchTask()方法启动Task
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        // 如果Executor为空，告诉DriverEndpoint移除Executor，退出当前进程
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        // 否则通知Executor杀死该Task
        executor.killTask(taskId, interruptThread)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

  case Shutdown =>
    // 设置停止标记为true
    stopping.set(true)
    // 启动一个线程停止Executor
    new Thread("CoarseGrainedExecutorBackend-stop-executor") {
      override def run(): Unit = {
        // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
        // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
        // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
        // Therefore, we put this line in a new thread.
        executor.stop()
      }
    }.start()
  }

  // 当有连接与当前RpcEndpoint断开时被调用
  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      // 如果是Driver与该RpcEndpoint断开连接，说明该Executor已经无用了，需要退出当前进程
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  /**
   * 在Standalone模式下，由于CoarseGrainedExecutorBackend与Executor在同一个进程内，所以它对Task的状态能够准确实时地捕获。
   * 利用这一优势，CoarseGrainedExecutorBackend可以将Task的状态发送给DriverEndpoint，以便对Task的状态进行更新。
   */
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    // 构造StatusUpdate消息
    val msg = StatusUpdate(executorId, taskId, state, data)
    driver match {
        // 发送给DriverEndpoint
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }

  /**
   * This function can be overloaded by other child classes to handle
   * executor exits differently. For e.g. when an executor goes down,
   * back-end may not want to take the parent process down.
   */
  protected def exitExecutor(code: Int,
                             reason: String,
                             throwable: Throwable = null,
                             notifyDriver: Boolean = true) = {
    // 处理日志
    val message = "Executor self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    // 判断是否需要通知Driver
    if (notifyDriver && driver.nonEmpty) {
      // 向Driver发送RemoveExecutor消息
      driver.get.ask[Boolean](
        RemoveExecutor(executorId, new ExecutorLossReason(reason))
      ).onFailure { case e =>
        logWarning(s"Unable to notify the driver due to " + e.getMessage, e)
      }(ThreadUtils.sameThread)
    }
    // 直接退出
    System.exit(code)
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: Seq[URL]) {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      // 从CoarseGrainedSchedulerBackend获取所需的Spark属性信息和密钥
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      // 创建名为driverPropsFetcher的RpcEnv。此RpcEnv主要用于从Driver拉取属性信息，并非Executor的SparkEnv中的RpcEnv。
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(driverUrl)

      // 向DriverEndpoint发送RetrieveSparkAppConfig消息，从CoarseGrainedSchedulerBackend获取所需的Spark属性信息和密钥。
      val cfg = driver.askWithRetry[SparkAppConfig](RetrieveSparkAppConfig)
      // 将CoarseGrainedSchedulerBackend回复的SparkAppConfig消息中携带的Spark属性和Application的ID属性保存到props中。
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      // 创建Executor自己的SparkConf，内部参数都是从Driver获取的。
      val driverConf = new SparkConf()
      // 将props中的属性全部保存到SparkConf中
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      if (driverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          driverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startCredentialUpdater(driverConf)
      }

      // 创建Executor自身的SparkEnv
      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, cfg.ioEncryptionKey, isLocal = false)

      // 创建并注册CoarseGrainedExecutorBackend实例
      env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
        env.rpcEnv, driverUrl, executorId, hostname, cores, userClassPath, env))
      /**
        * 创建并注册WorkerWatcher实例。WorkerWatcher是Worker的守望者，
        * 当发生Worker进程退出、连接断开、网络出错等情况时，
        * 终止CoarseGrainedExecutorBackend进程。
        * Worker与CoarseGrainedExecutorBackend进程相辅相成。
        */
      workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
      SparkHadoopUtil.get.stopCredentialUpdater()
    }
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    // 对执行参数进行解析
    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    // 检查参数是否合法，不合法则打印使用方式并退出
    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    // 将解析得到的变量作为参数，调用run()方法
    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
    System.exit(0)
  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
      |Usage: CoarseGrainedExecutorBackend [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

}
