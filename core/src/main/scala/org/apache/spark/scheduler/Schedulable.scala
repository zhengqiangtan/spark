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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  // 依赖的Pool可以调度实体，通过该字段可以构建一颗树
  var parent: Pool
  // child queues
  // 同步队列，用于管理一套可调度的实体集
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  // 调度模式，不同调度模式对应不同的调度算法
  def schedulingMode: SchedulingMode
  // 权重
  def weight: Int
  // 时间
  def minShare: Int
  // 当前正在运行的任务数量
  def runningTasks: Int
  // 优先级
  def priority: Int
  // 调度对象对应的Stage ID
  def stageId: Int
  // 调度实体名称
  def name: String

  // 添加指定的Schedulable
  def addSchedulable(schedulable: Schedulable): Unit
  // 移除指定的Schedulable
  def removeSchedulable(schedulable: Schedulable): Unit
  // 根据调度实体的名称，查询调度对象
  def getSchedulableByName(name: String): Schedulable
  // 当执行任务的Executor失败时，会调用该函数进行处理
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  // 检查是否有需要推测执行任务，由TaskScheduler定期调用
  def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean
  // 对TaskSetManager按照调度算法进行排序，返回排序后的TaskSetManager
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
