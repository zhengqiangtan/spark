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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 *
  * @param stageId // Stage的id
  * @param attemptId // 当前Stage尝试的id
  * @param name // 当前Stage的名称
  * @param numTasks // 当前Stage的Task数量
  * @param rddInfos // RDD信息（即RDDInfo）的序列
  * @param parentIds // 当前Stage的父亲Stage的身份标识序列
  * @param details // 详细的线程栈信息
  * @param taskMetrics // Task的度量信息
  * @param taskLocalityPreferences // 用于存储任务的本地性偏好
  */
@DeveloperApi
class StageInfo(
    val stageId: Int,
    val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo],
    val parentIds: Seq[Int],
    val details: String,
    val taskMetrics: TaskMetrics = null,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) {
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler.
    * DAGScheduler将当前Stage提交给TaskScheduler的时间
    **/
  var submissionTime: Option[Long] = None
  /** Time when all tasks in the stage completed or when the stage was cancelled.
    * 当前Stage中的所有Task完成的时间（即Stage完成的时间）或者Stage被取消的时间
    **/
  var completionTime: Option[Long] = None
  /** If the stage failed, the reason why.
    * 如果Stage失败了，用于记录失败的原因
    **/
  var failureReason: Option[String] = None

  /**
   * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
    *
    * 存储了所有聚合器计算的最终值
   */
  val accumulables = HashMap[Long, AccumulableInfo]()

  // 当Stage失败时会调用的方法
  def stageFailed(reason: String) {
    // 保存Stage失败的原因和Stage完成的时间
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  // 根据completionTime和failureReason来获取状态字符串表示
  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    // 获取RDD的祖先依赖中属于窄依赖的RDD序列，给得到的每个RDD创建RDDInfo对象
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    // 给当前Stage的RDD创建对应的RDDInfo对象，与ancestorRddInfos中所有RDDInfo对象合并到一个序列中
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    // 创建StageInfo对象
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences)
  }
}
