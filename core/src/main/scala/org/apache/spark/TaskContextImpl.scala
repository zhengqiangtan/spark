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

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.util._

/**
  * TaskContext唯一的实现类
  *
  * @param stageId Task所属Stage的身份标识
  * @param partitionId Task对应的分区索引
  * @param taskAttemptId TaskAttempt的身份标识
  * @param attemptNumber TaskAttempt号
  * @param taskMemoryManager Task内存管理器TaskMemoryManager
  * @param localProperties
  * @param metricsSystem 度量系统MetricsSystem
  * @param taskMetrics 用于跟踪Task执行过程的度量信息，类型为TaskMetrics
  */
private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes.
    * 保存任务执行完成后需要回调的TaskCompletionListener的数组
    **/
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails.
    * 保存任务执行失败后需要回调的TaskFailureListener的数组
    **/
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // Whether the corresponding task has been killed.
  /**
    * TaskContextImpl相对应的TaskAttempt是否已经被kill的状态。
    * 之所以用interrupted作为TaskAttempt被kill的状态变量，
    * 是因为kill实际是通过对执行TaskAttempt的线程进行中断实现的。
    */
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  // TaskContextImpl相对应的Task是否已经完成的状态
  @volatile private var completed: Boolean = false

  // Whether the task has failed.
  // TaskContextImpl相对应的Task是否已经失败的状态
  @volatile private var failed: Boolean = false

  // 用于向onCompleteCallbacks中添加TaskCompletionListener
  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  // 用于向onFailureCallbacks中添加TaskFailureListener
  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    onFailureCallbacks += listener
    this
  }

  /** Marks the task as failed and triggers the failure listeners.
    * 标记Task执行失败
    **/
  private[spark] def markTaskFailed(error: Throwable): Unit = {
    // failure callbacks should only be called once
    // 判断Task是否已经被标记为失败。如果是，则返回
    if (failed) return
    // 将Task标记为失败
    failed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process failure callbacks in the reverse order of registration
    // 对onFailureCallbacks进行反向排序，遍历排序后的TaskFailureListener
    onFailureCallbacks.reverse.foreach { listener =>
      try {
        // 将错误信息交给任务失败监听器处理，如果发生了异常，这些异常将被收集到errorMsgs中。
        listener.onTaskFailure(this, error)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskFailureListener", e)
      }
    }
    if (errorMsgs.nonEmpty) { // errorMsgs不为空
      // 对外抛出携带errorMsgs的TaskCompletionListenerException。
      throw new TaskCompletionListenerException(errorMsgs, Option(error))
    }
  }

  /** Marks the task as completed and triggers the completion listeners.
    * 标记Task执行完成
    **/
  private[spark] def markTaskCompleted(): Unit = {
    // 将任务标记为已完成
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    // 对onCompleteCallbacks进行反向排序，遍历排序后的TaskCompletionListener
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        // 将任务完成的消息交给任务完成监听器，如果发生了异常，这些异常将被收集到errorMsgs中。
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) { // errorMsgs不为空
      // 对外抛出携带errorMsgs的TaskCompletionListenerException。
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation.
    * 标记Task已经被kill
    **/
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  // Task是否已经完成
  override def isCompleted(): Boolean = completed

  // Task是否在本地运行
  override def isRunningLocally(): Boolean = false

  // Task是否已经被kill
  override def isInterrupted(): Boolean = interrupted

  // 获取指定key对应的本地属性值
  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  // 通过Source名称从MetricsSystem中获取Source序列
  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  // 向TaskMetrics注册累加器
  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

}
