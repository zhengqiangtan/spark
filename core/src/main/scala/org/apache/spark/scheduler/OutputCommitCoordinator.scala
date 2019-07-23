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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  // OutputCommitCoordinatorEndpoint的NettyRpcEndpointRef引用
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int

  private val NO_AUTHORIZED_COMMITTER: TaskAttemptNumber = -1

  /**
   * Map from active stages's id => partition id => task attempt with exclusive lock on committing
   * output for that partition.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
    *
    * 缓存Stage的各个分区的任务尝试
   */
  private val authorizedCommittersByStage = mutable.Map[StageId, Array[TaskAttemptNumber]]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
    * 用于判断authorizedCommittersByStage是否为空
   */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
    *
    * 用于向OutputCommitCoordinatorEndpoint发送AskPermissionToCommitOutput，
    * 并根据OutputCommitCoordinatorEndpoint的响应确认是否有权限将Stage的指定分区的输出提交到HDFS上。
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = {
    // 构造消息
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        // 发送消息并等待回复
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by the DAGScheduler when a stage starts.
    *
    * 用于启动给定Stage的输出提交到HDFS的协调机制
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(
      stage: StageId,
      maxPartitionId: Int): Unit = {
    // 构造一个数组，值全为NO_AUTHORIZED_COMMITTER（-1）
    val arr = new Array[TaskAttemptNumber](maxPartitionId + 1)
    java.util.Arrays.fill(arr, NO_AUTHORIZED_COMMITTER)
    synchronized {
      // 向authorizedCommittersByStage字典中添加键值对
      authorizedCommittersByStage(stage) = arr
    }
  }

  // Called by DAGScheduler
  // 用于停止给定Stage的输出提交到HDFS的协调机制
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    // 将给定Stage及对应的TaskAttemptNumber数组从authorizedCommittersByStage中删除。
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  // 给定Stage的指定分区的任务执行完成后将调用taskCompleted方法
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber,
      reason: TaskEndReason): Unit = synchronized {
    // 获取对应的TaskAttemptNumber数组
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success => // 任务执行成功
      // The task output has been committed successfully
      case denied: TaskCommitDenied => // 任务提交被拒绝
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
          s"attempt: $attemptNumber")
      case otherReason => // 其他原因
        if (authorizedCommitters(partition) == attemptNumber) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          /**
            * 将给定Stage的对应TaskAttemptNumber数组中指定分区的值修改为NO_AUTHORIZED_COMMITTER，
            * 以便于之后的TaskAttempt能够有权限提交。
            */
          authorizedCommitters(partition) = NO_AUTHORIZED_COMMITTER
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      // 向OutputCommitCoordinatorEndpoint发送StopCoordinator消息以停止OutputCommitCoordinatorEndpoint
      coordinatorRef.foreach(_ send StopCoordinator)
      // 将coordinatorRef置为None
      coordinatorRef = None
      // 清空authorizedCommittersByStage
      authorizedCommittersByStage.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  // 用于判断给定的任务尝试是否有权限将给定Stage的指定分区的数据提交到HDFS。
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    // 从authorizedCommittersByStage缓存中找到给定Stage的指定分区的TaskAttemptNumber
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters(partition) match {
          case NO_AUTHORIZED_COMMITTER => // 获取的TaskAttemptNumber等于NO_AUTHORIZED_COMMITTER
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            /**
              * 说明当前是首次提交给定Stage的指定分区的输出，
              * 因此给定TaskAttemptNumber有权限将给定Stage的指定分区的输出提交到HDFS。
              * 并将给定分区的索引与attemptNumber的关系保存到TaskAttemptNumber数组中，
              * 标识已有任务尝试了提交。
              */
            authorizedCommitters(partition) = attemptNumber
            true
          case existingCommitter => // 已存在任务尝试将给定Stage的指定分区的输出提交到HDFS
            logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; existingCommitter = $existingCommitter")
            // 返回false，标识当前attemptNumber没有权限将给定Stage的指定分区的输出提交到HDFS
            false
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing attempt number $attemptNumber of" +
          s"partition $partition to commit")
        false
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator => // 此消息将停止OutputCommitCoordinatorEndpoint
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // 此消息将通过OutputCommitCoordinator的handleAskPermissionToCommit方法处理，进而确认客户端是否有权限将输出提交到HDFS。
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
    }
  }
}
