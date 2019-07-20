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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
 * The ResultStage object captures the function to execute, `func`, which will be applied to each
 * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
 * of the RDD, for actions like first() and lookup().
  *
  * ResultStage可以使用指定的函数对RDD中的分区进行计算并得出最终结果。
  * ResultStage是最后执行的Stage，此阶段主要进行作业的收尾工作
  * （例如，对各个分区的数据收拢、打印到控制台或写入到HDFS）。
  *
  * @param id Unique stage ID
  *           Stage的身份标识
  * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
  *   on, while for a result stage, it's the target RDD that we ran an action on
  *   当前Stage包含的RDD
  * @param func 对RDD的分区进行计算的函数。
  * @param partitions
  * @param parents List of stages that this stage depends on (through shuffle dependencies).
  *                当前Stage的父Stage列表，一个Stage可以有一到多个父亲Stage。
  * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
  *                   第一个提交当前Stage的Job的身份标识（即Job的id）。
  *                   当使用FIFO调度时，通过firstJobId首先计算来自较早Job的Stage，或者在发生故障时更快的恢复。
  * @param callSite Location in the user program associated with this stage: either where the target
  *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
  *   应用程序中与当前Stage相关联的调用栈信息。
  */
private[spark] class ResultStage(
    id: Int,
    rdd: RDD[_],
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite) {

  /**
   * The active job for this result stage. Will be empty if the job has already finished
   * (e.g., because the job was cancelled).
    *
    * ResultStage处理的ActiveJob
   */
  private[this] var _activeJob: Option[ActiveJob] = None

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   *
   * This can only be called when there is an active job.
    *
    * 找出当前Job的所有分区中还没有完成的分区的索引
   */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    // 通过ActiveJob的Boolean类型数组finished来判断，finished记录了每个分区是否完成
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}
