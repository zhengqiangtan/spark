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

package org.apache.spark.scheduler

import java.io.{FileInputStream, InputStream}
import java.util.{NoSuchElementException, Properties}

import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  // 返回根调度池
  def rootPool: Pool

  // 对调度池进行构建
  def buildPools(): Unit

  // 向调度池内添加TaskSetManager
  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    // 直接向根调度池添加TaskSetManager
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  /**
    * 用户指定的文件系统中的调度分配文件。
    * 此文件可以通过spark.scheduler.allocation.file属性配置，
    * FairSchedulableBuilder将从文件系统中读取此文件提供的公平调度配置。
    */
  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file")
  /**
    * 默认的调度文件名。
    * 常量DEFAULT_SCHEDULER_FILE的值固定为"fairscheduler.xml"，
    * FairSchedulableBuilder将从ClassPath中读取此文件提供的公平调度配置。
    *
    * <allocations>
    *     <pool name="production">
    *         <schedulingMode>FAIR</schedulingMode>
    *         <weight>1</weight>
    *         <minShare>2</minShare>
    *     </pool>
    *     <pool name="test">
    *         <schedulingMode>FIFO</schedulingMode>
    *         <weight>2</weight>
    *         <minShare>3</minShare>
    *     </pool>
    * </allocations>
    */
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  // 即spark.scheduler.pool，此属性的值作为放置TaskSetManager的公平调度池的名称。
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  // 默认的调度池名。常量DEFAULT_POOL_NAME的值固定为"default"。
  val DEFAULT_POOL_NAME = "default"
  // 固定为"minShare"，即XML文件的<Pool>节点的子节点<mindshare>。节点<mindshare>的值将作为Pool的minShare属性。
  val MINIMUM_SHARES_PROPERTY = "minShare"
  // 固定为"schedulingMode"，即XML文件的<Pool>节点的子节点<schedulingMode>。
  // 节点<schedulingMode>的值将作为Pool的调度模式（schedulingMode）属性。
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  // 权重属性。固定为"weight"，即XML文件的<Pool>节点的子节点<weight>。节点<weight>的值将作为Pool的权重（weight）属性。
  val WEIGHT_PROPERTY = "weight"
  // 固定为"@name"，即XML文件的<Pool>节点的name属性。name属性的值将作为Pool的调度池名（poolName）属性。
  val POOL_NAME_PROPERTY = "@name"
  // 调度池属性。常量POOLS_PROPERTY的值固定为"pool"。
  val POOLS_PROPERTY = "pool"
  // 默认的调度模式FIFO
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  // 公平调度算法中Schedulable的minShare属性的默认值，固定为0。
  val DEFAULT_MINIMUM_SHARE = 0
  // 默认的权重，固定为1。
  val DEFAULT_WEIGHT = 1

  // 构建公平调度池
  override def buildPools() {
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulerAllocFile.map { f =>
          // 从文件系统中读取公平调度配置的文件输入流
          new FileInputStream(f)
        }.getOrElse {
          Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        }
      }

      // 解析文件输入流并构建调度池
      is.foreach { i => buildFairSchedulerPool(i) }
    } finally {
      is.foreach(_.close())
    }

    // finally create "default" pool
    // 构建默认的调度池
    buildDefaultPool()
  }

  // 当根调度池及其子调度池中不存在名为default的调度池时，构建默认调度池
  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      // 创建默认调度池
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      // 向根调度池的调度队列中添加默认的子调度池
      rootPool.addSchedulable(pool)
      logInfo("Created default pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  // 对文件输入流进行解析并构建调度池
  private def buildFairSchedulerPool(is: InputStream) {
    // 将文件输入流转换为XML
    val xml = XML.load(is)
    // 读取XML的每一个<Pool>节点
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {
      // 读取<Pool>的name属性作为调度池的名称
      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      var schedulingMode = DEFAULT_SCHEDULING_MODE
      var minShare = DEFAULT_MINIMUM_SHARE
      var weight = DEFAULT_WEIGHT

      val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text
      if (xmlSchedulingMode != "") {
        try {
          // 读取<Pool>的子节点<schedulingMode>的值作为调度池的调度模式属性
          schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
        } catch {
          case e: NoSuchElementException =>
            logWarning(s"Unsupported schedulingMode: $xmlSchedulingMode, " +
              s"using the default schedulingMode: $schedulingMode")
        }
      }

      // 读取<Pool>的子节点<minShare>的值作为调度池的minShare属性
      val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text
      if (xmlMinShare != "") {
        minShare = xmlMinShare.toInt
      }

      // 读取<Pool>的子节点<weight>的值作为调度池的权重（weight）属性
      val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text
      if (xmlWeight != "") {
        weight = xmlWeight.toInt
      }

      // 创建子调度池
      val pool = new Pool(poolName, schedulingMode, minShare, weight)
      // 将创建的子调度池添加到根调度池的调度队列
      rootPool.addSchedulable(pool)
      logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  // 添加TaskSetManager
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    var poolName = DEFAULT_POOL_NAME
    // 以默认调度池作为TaskSetManager的父调度池
    var parentPool = rootPool.getSchedulableByName(poolName)
    // 判断默认调度池是否存在
    if (properties != null) { // 默认父调度池不存在
      // 以spark.scheduler.pool属性指定的调度池作为TaskSetManager的父调度池
      poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      parentPool = rootPool.getSchedulableByName(poolName)

      if (parentPool == null) { // 指定的父调度池也不存在
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        // 创建新的父调度池
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        // 将父调度池添加到根调度池中
        rootPool.addSchedulable(parentPool)
        logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
    }

    // 将TaskSetManager放入父调度池
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
