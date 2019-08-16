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

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
  *
  * ShuffleMapStage是DAG调度流程的中间Stage，
  * 它可以包括一到多个ShuffleMapTask，这些ShuffleMapTask将生成用于Shuffle的数据。
  * ShuffleMapStage一般是ResultStage或者其他ShuffleMapStage的前置Stage，
  * ShuffleMapTask则通过Shuffle与下游Stage中的Task串联起来。
  * 从ShuffleMapStage的命名可以看出，它将对Shuffle的数据映射到下游Stage的各个分区中。
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
  * @param shuffleDep 与ShuffleMapStage相对应的ShuffleDependency
  */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  // 与ShuffleMapStage相关联的ActiveJob的列表
  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  // ShuffleMapStage可用的Map任务的输出数量，这也代表了执行成功的Map任务数。
  private[this] var _numAvailableOutputs: Int = 0

  /**
   * List of [[MapStatus]] for each partition. The index of the array is the map partition id,
   * and each value in the array is the list of possible [[MapStatus]] for a partition
   * (a single task might run multiple times).
    *
    * ShuffleMapStage的各个分区与其对应的MapStatus列表的映射关系。
    * 由于map任务可能会运行多次，因而可能会有多个MapStatus。
   */
  private[this] val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list.
    * 向ShuffleMapStage相关联的ActiveJob的列表中添加ActiveJob
    **/
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list.
    * 向ShuffleMapStage相关联的ActiveJob的列表中删除ActiveJob
    **/
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   * This should be kept consistent as `outputLocs.filter(!_.isEmpty).size`.
   */
  def numAvailableOutputs: Int = _numAvailableOutputs

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   * This should be the same as `outputLocs.contains(Nil)`.
    *
    * 当_numAvailableOutputs与numPartitions相等时为true。
    * 也就是说，ShuffleMapStage的所有分区的map任务都执行成功后，ShuffleMapStage才是可用的。
   */
  def isAvailable: Boolean = _numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed).
    * 找到所有还未执行成功而需要计算的分区
    **/
  override def findMissingPartitions(): Seq[Int] = {
    // 从outputLocs中查找
    val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }

  /**
    * 当某一分区的任务执行完成后，
    * 首先将分区与MapStatus的对应关系添加到outputLocs中，然后将可用的输出数加一。
    */
  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    // 得到分区对应的MapStatus列表
    val prevList = outputLocs(partition)
    // 将status添加到该列表中
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) { // 如果之前该分区没有任何MapStatus
      // 将可用的输出数加一
      _numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      _numAvailableOutputs -= 1
    }
  }

  /**
   * Returns an array of [[MapStatus]] (index by partition id). For each partition, the returned
   * value contains only one (i.e. the first) [[MapStatus]]. If there is no entry for the partition,
   * that position is filled with null.
   */
  def outputLocInMapOutputTrackerFormat(): Array[MapStatus] = {
    outputLocs.map(_.headOption.orNull)
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        _numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, _numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
