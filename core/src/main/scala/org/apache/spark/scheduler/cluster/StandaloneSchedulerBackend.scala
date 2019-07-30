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

import java.util.concurrent.Semaphore

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager.
  *
  * 在local-cluster模式和Standalone部署模式下的SchedulerBackend实现
  * 继承自CoarseGrainedSchedulerBackend
 */
private[spark] class StandaloneSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {

  // 即StandaloneAppClient。
  private var client: StandaloneAppClient = null
  // 标记StandaloneSchedulerBackend是否正在停止。
  private var stopping = false
  // 运行器后端接口LauncherBackend。具体实现与LocalSchedulerBackend的launcherBackend属性的实现类似。
  private val launcherBackend = new LauncherBackend() {
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  // Application的ID。
  @volatile private var appId: String = _

  /**
    * 使用Java的信号量（Semaphore）实现的栅栏，用于等待Application向Master注册完成后，
    * 将Application的当前状态（此时为正在运行，即RUNNING）告知LauncherServer。
    */
  private val registrationBarrier = new Semaphore(0)

  // Application可以申请获得的最大内核数。可通过spark.cores.max属性配置。
  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  // Application期望获得的内核数，如果设置了maxCores，则为maxCores，否则为0。
  private val totalExpectedCores = maxCores.getOrElse(0)

  // StandaloneSchedulerBackend的启动方法
  override def start() {
    // 调用父类的start()方法，创建并注册DriverEndpoint
    super.start()
    // 与LauncherServer建立连接
    launcherBackend.connect()

    // The endpoint for executors to talk to us
    // // 生成Driver URL，格式为spark://CoarseGrainedScheduler@${driverHost}:${driverPort}，Executor将通过此URL与Driver通信。
    val driverUrl = RpcEndpointAddress(
      sc.conf.get("spark.driver.host"),
      sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

    // 拼接参数列表args
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")

    // 获取额外的Java参数extraJavaOpts，通过spark.executor.extraJavaOptions属性配置
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    // 获取额外的类路径classPathEntries，通过spark.executor.extraClassPath属性配置
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    // 获取额外的库路径libraryPathEntries，通过spark.executor.extraLibraryPath属性配置
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    // 获取测试类路径testingClassPath，通过spark.testing和java.class.path属性配置
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    /**
      * 从SparkConf中获取需要传递给Executor用于启动的配置sparkJavaOpts，包括：
      * - 以spark.auth开头的配置（但不包括spark.authenticate.secret）
      * - 以spark.ssl开头的配置
      * - 以spark.rpc开头的配置
      * - 以spark.开头且以.port结尾的配置
      * - 以spark.port.开头的配置
      */
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    // 将extraJavaOpts与sparkJavaOpts合并到javaOpts
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    /**
      * 创建Command。Command类定义了执行Executor的命令。
      * - 以org.apache.spark.executor.CoarseGrainedExecutorBackend作为Command的mainClass属性
      * - 以args作为Command的arguments属性
      * - 以SparkContext的executorEnvs属性作为Command的environment属性
      * - 以classPathEntries作为Command的classPathEntries属性
      * - 以libraryPathEntries作为Command的libraryPathEntries属性
      * - 以javaOpts作为Command的javaOpts属性
      */
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)

    // 获取Spark UI的http地址appUIAddress
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    // 获取每个Executor分配的内核数coresPerExecutor，可通过spark.executor.cores属性配置
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    /**
      * 获取Executor的初始限制initialExecutorLimit。
      * 如果启用了动态分配Executor，那么initialExecutorLimit被设置为0，
      * ExecutorAllocationManager之后会将真实的初始限制值传递给Master。
      */
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        Some(0)
      } else {
        None
      }

    // 创建Application描述信息
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit)

    /**
      * 创建并启动StandaloneAppClient。
      * StandaloneAppClient的start方法将创建ClientEndpoint，
      * 并向SparkContext的SparkEnv的RpcEnv注册ClientEndpoint，
      * 进而引起对ClientEndpoint的启动和向Master注册Application。
      */
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()

    // 调用LauncherBackend的setState()方法向LauncherServer传递应用已经提交（SUBMITTED）的状态。
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)

    /**
      * 等待注册Application完成。
      * 当注册Application成功后，Master将向ClientEndpoint发送RegisteredApplication消息，
      * 进而调用StandaloneSchedulerBackend的connected方法释放信号量，
      * 这样waitForRegistration()方法将可以获得信号量。
      */
    waitForRegistration()

    // 调用LauncherBackend的setState()方法向LauncherServer传递应用正在运行（RUNNING）的状态。
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  // 用于停止StandaloneSchedulerBackend
  override def stop(): Unit = synchronized {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    // 记录传入的Application ID
    this.appId = appId
    // 释放信号量
    notifyContext()
    // 向LaunchServer发送Application ID
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message, workerLost = workerLost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    Option(client) match {
      // 调用了StandaloneAppClient的requestTotalExecutors()方法向Master请求所需的所有Executor资源。
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  private def waitForRegistration() = {
    // 尝试获取信号量
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = synchronized {
    try {
      // 标记正在停止
      stopping = true

      // 调用父类的方法
      super.stop()

      // 停止StandaloneAppClient
      client.stop()

      // 调用关闭回调函数
      val callback = shutdownCallback
      if (callback != null) {
        callback(this)
      }
    } finally {
      launcherBackend.setState(finalState)
      launcherBackend.close()
    }
  }

}
