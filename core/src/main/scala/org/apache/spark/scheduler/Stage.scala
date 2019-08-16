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

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
  *           Stage的身份标识
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
  *   当前Stage包含的RDD，归属于本Stage的最后一个RDD
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
  *   当前Stage的Task数量
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
  *                当前Stage的父Stage列表，一个Stage可以有一到多个父亲Stage。
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
  *                   第一个提交当前Stage的Job的身份标识（即Job的id）。
  *                   当使用FIFO调度时，通过firstJobId首先计算来自较早Job的Stage，或者在发生故障时更快的恢复。
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
  *   应用程序中与当前Stage相关联的调用栈信息。
 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
  extends Logging {

  // 当前Stage最后一个RDD的分区数量
  val numPartitions = rdd.partitions.length

  /** Set of jobs that this stage belongs to.
    * 当前Stage所属的Job的身份标识集合。
    * 一个Stage可以属于一到多个Job。
    **/
  val jobIds = new HashSet[Int]

  // 存储待处理分区的索引的集合。
  val pendingPartitions = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage.
    * 用于生成Stage下一次尝试的身份标识。
    **/
  private var nextAttemptId: Int = 0

  // 记录当前调用栈信息
  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [StageInfo] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
    *
    * Stage最近一次尝试的信息。
   */
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed with a FetchFailure. We keep track of these
   * failures in order to avoid endless retries if a stage keeps failing with a FetchFailure.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
    *
    * 发生过FetchFailure的Stage尝试的身份标识的集合。
    * 此属性用于避免在发生FetchFailure后无止境的重试。
   */
  private val fetchFailedAttemptIds = new HashSet[Int]

  // 清空fetchFailedAttemptIds
  private[scheduler] def clearFailures() : Unit = {
    fetchFailedAttemptIds.clear()
  }

  /**
   * Check whether we should abort the failedStage due to multiple consecutive fetch failures.
   *
   * This method updates the running set of failed stage attempts and returns
   * true if the number of failures exceeds the allowable number of failures.
    *
    * 用于将发生FetchFailure的Stage尝试的身份标识添加到fetchFailedAttemptIds中，
    * 并返回发生FetchFailure的次数是否已经超过了允许发生FetchFailure的次数的状态。
    * 允许发生FetchFailure的次数固定为4。
   */
  private[scheduler] def failedOnFetchAndShouldAbort(stageAttemptId: Int): Boolean = {
    fetchFailedAttemptIds.add(stageAttemptId)
    // 判断是否达到最大失败次数，如果达到，当前Stage将被终止
    fetchFailedAttemptIds.size >= Stage.MAX_CONSECUTIVE_FETCH_FAILURES // 4
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID.
    * 用于创建新的Stage尝试
    **/
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    // 创建新的StageInfo
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
    // 自增nextAttemptId
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage.
    * 回最近一次Stage尝试的StageInfo
    **/
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed).
    * 找到还未执行完成的分区
    **/
  def findMissingPartitions(): Seq[Int]
}

private[scheduler] object Stage {
  // The number of consecutive failures allowed before a stage is aborted
  // 在连续4次执行失败后当前Stage会被终止
  val MAX_CONSECUTIVE_FETCH_FAILURES = 4
}
