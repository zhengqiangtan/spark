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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 *
  * @param dagScheduler 当前JobWaiter等待执行完成的Job的调度者
  * @param jobId 当前JobWaiter等待执行完成的Job的身份标识
  * @param totalTasks 等待完成的Job包括的Task数量
  * @param resultHandler 执行结果的处理器
  * @tparam T
  */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener with Logging {

  // 等待完成的Job中已经完成的Task数量
  private val finishedTasks = new AtomicInteger(0)
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  /**
    * 用来代表Job完成后的结果。
    * 如果totalTasks等于零，说明没有Task需要执行，此时将被直接设置为Success。
    */
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  // Job是否已经完成
  def jobFinished: Boolean = jobPromise.isCompleted

  // 返回jobPromise的future
  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
    *
    * 取消对Job的执行
   */
  def cancel() {
    // 使用DAGScheduler的cancelJob()方法来取消Job
    dagScheduler.cancelJob(jobId)
  }

  // Job执行成功后将调用该方法
  override def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized { // 加锁进行回调
      resultHandler(index, result.asInstanceOf[T])
    }
    // 完成Task数量自增，如果所有Task都完成了就调用JobPromise的success()方法
    if (finishedTasks.incrementAndGet() == totalTasks) {
      jobPromise.success(())
    }
  }

  // Job执行失败后将调用该方法
  override def jobFailed(exception: Exception): Unit = {
    // 调用jobPromise的相关方法将其设置为Failure
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}
