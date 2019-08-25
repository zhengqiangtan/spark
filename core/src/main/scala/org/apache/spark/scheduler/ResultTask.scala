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

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId         id of the stage this task belongs to
 *                        当前Task所属的Stage的ID
 * @param stageAttemptId  attempt id of the stage this task belongs to
 *                        当前Task所属的Stage Attempt的ID
 * @param taskBinary      broadcasted version of the serialized RDD and the function to apply on each
 *                        partition of the given RDD. Once deserialized, the type should be
 *                        (RDD[T], (TaskContext, Iterator[T]) => U).
 *                        广播后的RDD及执行函数依赖，反序列化将得到
 *                        (RDD[_], (TaskContext, Iterator[T]) => U)元组类型的结构
 * @param partition       partition of the RDD this task is associated with
 *                        该Task所对应的RDD的分区
 * @param locs            preferred task execution locations for locality scheduling
 *                        该Task的偏好位置
 * @param outputId        index of the task in this job (a job can launch tasks on only a subset of the
 *                        input RDD's partitions).
 *                        Task在Job中的索引
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param metrics         a `TaskMetrics` that is created at driver side and sent to executor side.
 *
 *                        The parameters below are optional:
 * @param jobId           id of the job this task belongs to
 *                        Task所属Job的ID
 * @param appId           id of the app this task belongs to
 *                        Task所属的Application的ID
 * @param appAttemptId    attempt id of the app this task belongs to
 *                        Task所属的Application Attempt的ID
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    metrics: TaskMetrics,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[U](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    // 反序列化开始时间
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    /**
     * 对序列化的Task进行反序列化，得到RDD和要执行的函数
     * 可回顾[[DAGScheduler.submitMissingTasks()]]方法对RDD和执行函数进行序列化广播的操作
     */
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    // 统计反序列化所用时间
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    // 调用RDD的iterator方法进行迭代计算，调用函数进行最终的处理
    func(context, rdd.iterator(partition, context))
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
