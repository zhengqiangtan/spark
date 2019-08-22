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

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 *
 * 低层次的Task调度器接口，目前只有TaskSchedulerImpl一个具体实现类。
 * 该接口允许可插拔式地使用不同的Task调度器，每个调度器将对单个SparkContext中的Task进行调度。
 * TaskScheduler接收DAGScheduler为每个Stage提交的TaskSet，同时将这些Task发送到集群中进行运行。
 * TaskScheduler还会将各类事件通知给DAGScheduler。
 */
private[spark] trait TaskScheduler {

  // Application ID
  private val appId = "spark-application-" + System.currentTimeMillis

  // 获取根调度池
  def rootPool: Pool

  // 获取调度模式
  def schedulingMode: SchedulingMode

  // 启动方法
  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  // 启动前的钩子方法
  def postStartHook() { }

  // Disconnect from the cluster.
  // 停止方法
  def stop(): Unit

  // Submit a sequence of tasks to run.
  // 提交Task
  def submitTasks(taskSet: TaskSet): Unit

  // Cancel a stage.
  // 取消Task
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  // 设置DAGScheduler
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  // 获取默认的并行度
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   *
   * 收到Executor的心跳
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean

  /**
   * Get an application ID associated with the job.
   *
   * 获取Application的ID
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a lost executor
   *
   * Executor丢失
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Get an application's attempt ID associated with the job.
   *
   * 获取Application的Attempt ID
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]
}
