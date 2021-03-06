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

package org.apache.spark.ui

import java.util.{Date, ServiceLoader}

import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationAttemptInfo, ApplicationInfo,
  UIRoot}
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.{EnvironmentListener, EnvironmentTab}
import org.apache.spark.ui.exec.{ExecutorsListener, ExecutorsTab}
import org.apache.spark.ui.jobs.{JobProgressListener, JobsTab, StagesTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener
import org.apache.spark.ui.storage.{StorageListener, StorageTab}
import org.apache.spark.util.Utils

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    val environmentListener: EnvironmentListener,
    val storageStatusListener: StorageStatusListener,
    val executorsListener: ExecutorsListener,
    val jobProgressListener: JobProgressListener,
    val storageListener: StorageListener,
    val operationGraphListener: RDDOperationGraphListener,
    var appName: String,
    val basePath: String,
    val startTime: Long)
  extends WebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  // 标记当前SparkUI能否提供“杀死”Stage或者Job的链接。
  val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)

  // 当前应用的ID
  var appId: String = _

  /** Initialize all components of the server. */
  def initialize() {
    // 构建页面布局并给每个WebUITab中的所有WebUIPage创建对应的ServletContextHandler。
    val jobsTab = new JobsTab(this)
    attachTab(jobsTab)
    val stagesTab = new StagesTab(this)
    attachTab(stagesTab)
    attachTab(new StorageTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    /**
      * 调用JettyUtils的createStaticHandler方法创建对静态目录org/apache/spark/ui/static提供
      * 文件服务的ServletContextHandler，并使用attachHandler方法追加到SparkUI的服务中。
      */
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    // 调用JettyUtils的createRedirectHandler方法创建几个将用户对源路径的请求重定向到目标路径的ServletContextHandler。
    attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))
    attachHandler(ApiRootResource.getServletHandler(this))
    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    attachHandler(createRedirectHandler(
      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest,
      httpMethods = Set("GET", "POST")))
  }
  initialize()

  def getSparkUser: String = {
    environmentListener.systemProperties.toMap.get("user.name").getOrElse("<unknown>")
  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop() {
    super.stop()
    logInfo("Stopped Spark web UI at %s".format(appUIAddress))
  }

  /**
   * Return the application UI host:port. This does not include the scheme (http://).
   */
  private[spark] def appUIHostPort = publicHostName + ":" + boundPort

  private[spark] def appUIAddress = s"http://$appUIHostPort"

  def getSparkUI(appId: String): Option[SparkUI] = {
    if (appId == this.appId) Some(this) else None
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appId,
      name = appName,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        duration = 0,
        lastUpdated = new Date(startTime),
        sparkUser = "",
        completed = false
      ))
    ))
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoList.find(_.id == appId)
  }
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.getAppName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"
  val DEFAULT_RETAINED_STAGES = 1000
  val DEFAULT_RETAINED_JOBS = 1000

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }

  def createLiveUI(
      sc: SparkContext,
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      jobProgressListener: JobProgressListener,
      securityManager: SecurityManager,
      appName: String,
      startTime: Long): SparkUI = {
    create(Some(sc), conf, listenerBus, securityManager, appName,
      jobProgressListener = Some(jobProgressListener), startTime = startTime)
  }

  def createHistoryUI(
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long): SparkUI = {
    val sparkUI = create(
      None, conf, listenerBus, securityManager, appName, basePath, startTime = startTime)

    val listenerFactories = ServiceLoader.load(classOf[SparkHistoryListenerFactory],
      Utils.getContextOrSparkClassLoader).asScala
    listenerFactories.foreach { listenerFactory =>
      val listeners = listenerFactory.createListeners(conf, sparkUI)
      listeners.foreach(listenerBus.addListener)
    }
    sparkUI
  }

  /**
   * Create a new Spark UI.
   *
   * @param sc optional SparkContext; this can be None when reconstituting a UI from event logs.
   * @param jobProgressListener if supplied, this JobProgressListener will be used; otherwise, the
   *                            web UI will create and register its own JobProgressListener.
   */
  private def create(
      sc: Option[SparkContext],
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String = "",
      jobProgressListener: Option[JobProgressListener] = None,
      startTime: Long): SparkUI = {

    // 获取作业进度监听器
    val _jobProgressListener: JobProgressListener = jobProgressListener.getOrElse {
      // 如果没有的话会新建并添加到事件总线中
      val listener = new JobProgressListener(conf)
      listenerBus.addListener(listener)
      listener
    }

    // >>>>> 新建了一些监听器

    // 用于对JVM参数、Spark属性、Java系统属性、classpath等进行监控
    val environmentListener = new EnvironmentListener
    // 用于维护Executor的存储状态的StorageStatusListener
    val storageStatusListener = new StorageStatusListener(conf)
    // 用于准备将Executor的信息展示在ExecutorsTab的ExecutorsListener
    val executorsListener = new ExecutorsListener(storageStatusListener, conf)
    // 用于准备将Executor相关存储信息展示在BlockManagerUI的StorageListener
    val storageListener = new StorageListener(storageStatusListener)
    // 用于构建RDD的DAG（有向无关图）的RDDOperationGraphListener
    val operationGraphListener = new RDDOperationGraphListener(conf)

    // >>>>> 添加新建的监听器
    listenerBus.addListener(environmentListener)
    listenerBus.addListener(storageStatusListener)
    listenerBus.addListener(executorsListener)
    listenerBus.addListener(storageListener)
    listenerBus.addListener(operationGraphListener)

    // 构造SparkUI
    new SparkUI(sc, conf, securityManager, environmentListener, storageStatusListener,
      executorsListener, _jobProgressListener, storageListener, operationGraphListener,
      appName, basePath, startTime)
  }
}
