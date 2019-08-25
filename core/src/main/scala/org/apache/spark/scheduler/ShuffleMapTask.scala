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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * ShuffleMapTask类似于Hadoop中的MapTask，
 * 它对输入数据计算后，将输出的数据在Shuffle之前映射到不同的分区，
 * 下游处理各个分区的Task将知道处理哪些数据。
 *
 * @param stageId         id of the stage this task belongs to
 *                        当前Task所属的Stage的ID
 * @param stageAttemptId  attempt id of the stage this task belongs to
 *                        当前Task所属的Stage Attempt的ID
 * @param taskBinary      broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                        the type should be (RDD[_], ShuffleDependency[_, _, _]).
 *                        广播后的RDD及ShuffleDependency依赖，反序列化将得到
 *                        (RDD[_], ShuffleDependency[_, _, _])元组类型的结构
 * @param partition       partition of the RDD this task is associated with
 *                        该Task所对应的RDD的分区
 * @param locs            preferred task execution locations for locality scheduling
 *                        该Task的偏好位置
 * @param metrics         a `TaskMetrics` that is created at driver side and sent to executor side.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 *
 *                        The parameters below are optional:
 * @param jobId           id of the job this task belongs to
 *                        Task所属Job的ID
 * @param appId           id of the app this task belongs to
 *                        Task所属的Application的ID
 * @param appAttemptId    attempt id of the app this task belongs to
 *                        Task所属的Application Attempt的ID
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    metrics: TaskMetrics,
    localProperties: Properties,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null, new Properties)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    // 记录反序列化开始时间
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    // 获取反序列化器
    val ser = SparkEnv.get.closureSerializer.newInstance()
    /**
     * 对Task数据进行反序列化，得到RDD和ShuffleDependency
     * 可回顾[[DAGScheduler.submitMissingTasks()]]方法对RDD和ShuffleDependency进行序列化广播的操作
     */
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    // 得到反序列化时间
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    // Shuffle写出器
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 将计算的中间结果写入磁盘文件
      val manager = SparkEnv.get.shuffleManager
      // 获取对指定分区的数据进行磁盘写操作的SortShuffleWriter
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 调用RDD的iterator方法进行迭代计算，将计算的中间结果写入磁盘文件
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
