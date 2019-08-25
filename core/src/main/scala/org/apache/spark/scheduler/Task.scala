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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId         id of the stage this task belongs to
 *                        Task所属Stage的身份标识
 * @param stageAttemptId  attempt id of the stage this task belongs to
 *                        Stage尝试的身份标识
 * @param partitionId     index of the number in the RDD
 *                        Task对应的分区索引
 * @param metrics         a `TaskMetrics` that is created at driver side and sent to executor side.
 *                        用于跟踪Task执行过程的度量信息，类型为TaskMetric
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 *                        Task执行所需的属性信息
 *
 *                        The parameters below are optional:
 * @param jobId           id of the job this task belongs to
 *                        Task所属Job的身份标识
 * @param appId           id of the app this task belongs to
 *                        Task所属Application的身份标识，即SparkContext的_applicationId属性
 * @param appAttemptId    attempt id of the app this task belongs to
 *                        Task所属Application尝试的身份标识，即SparkContext的_applicationAttemptId属性
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    // The default value is only used in tests.
    val metrics: TaskMetrics = TaskMetrics.registered,
    @transient var localProperties: Properties = new Properties,
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None) extends Serializable {

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * 模板方法，此方法是运行Task的入口
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    // 将TaskAttempt注册到BlockInfoManager
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // 创建TaskAttempt的上下文
    context = new TaskContextImpl(
      stageId,
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)

    // 将任务尝试的上下文保存到ThreadLocal中
    TaskContext.setTaskContext(context)
    // 获取运行TaskAttempt的线程
    taskThread = Thread.currentThread()

    if (_killed) {
      // 如果任务尝试已经被kill，则将任务尝试及其上下文标记为被kill的状态
      kill(interruptThread = false)
    }

    // 创建调用者上下文
    new CallerContext("TASK", appId, appAttemptId, jobId, Option(stageId), Option(stageAttemptId),
      Option(taskAttemptId), Option(attemptNumber)).setCurrentContext()

    try {
      // 调用子类实现的runTask方法运行任务尝试
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          /**
           * 捕获到任何错误，调用TaskContextImpl的markTaskFailed()方法，
           * 执行所有TaskFailureListener的onTaskFailure()方法。
           */
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        throw e
    } finally {
      // Call the task completion callbacks.
      // 无论任务尝试是否成功，都会执行所有TaskCompletionListener的onTaskCompletion()方法
      context.markTaskCompleted()
      try {
        // 释放任务尝试所占用的堆内存和堆外内存
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for unrolling blocks
          // 释放任务尝试所占用的堆内存和堆外内存，以便唤醒任何等待MemoryManager管理的执行内存的任务尝试
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP)
          // Notify any tasks waiting for execution memory to be freed to wake up and try to
          // acquire memory again. This makes impossible the scenario where a task sleeps forever
          // because there are no other tasks left to notify it. Since this is safe to do but may
          // not be strictly necessary, we should revisit whether we can remove this in the future.
          val memoryManager = SparkEnv.get.memoryManager
          memoryManager.synchronized { memoryManager.notifyAll() }
        }
      } finally {
        // 移除ThreadLocal中保存的当前任务尝试线程的上下文
        TaskContext.unset()
      }
    }
  }

  // Task内存管理器TaskMemoryManager
  private var taskMemoryManager: TaskMemoryManager = _

  // 用于设置Task的taskMemoryManager
  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  // 运行Task的接口
  def runTask(context: TaskContext): T

  // 获取当前Task偏好的位置信息
  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskScheduler.
  // MapOutputTracker跟踪的年代信息。此属性由TaskScheduler设置，用于故障迁移。
  var epoch: Long = -1

  // Task context, to be initialized in run().
  // Task执行的上下文信息，即TaskContextImpl。TaskContextImpl将被设置到ThreadLocal中，以保证其线程安全。
  @transient protected var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  // 运行任务尝试的线程
  @volatile @transient private var taskThread: Thread = _

  // A flag to indicate whether the task is killed. This is used in case context is not yet
  // initialized when kill() is invoked.
  // Task是否被kill的状态
  @volatile @transient private var _killed = false

  // 对RDD进行反序列化所花费的时间
  protected var _executorDeserializeTime: Long = 0
  // 对RDD进行反序列化所花费的CPU时间
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * Whether the task has been killed.
    *
    * 用于判断任务尝试是否已被杀死，实际返回了Task的_killed属性
   */
  def killed: Boolean = _killed

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
    * 用于获取Task的_executorDeserializeTime属性
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  // 用于获取Task的_executorDeserializeCpuTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   *
   * 收集Task使用的累加器的最新值，并更新到TaskMetrics中。
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      context.taskMetrics.internalAccums.filter { a =>
        // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
        // value will be updated at driver side.
        // Note: internal accumulators representing task metrics always count failed values
        !a.isZero || a.name == Some(InternalAccumulator.RESULT_SIZE)
      // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not filter
      // them out.
      } ++ context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   *
   * 用于kill TaskAttempt线程
   */
  def kill(interruptThread: Boolean) {
    // 将Task和TaskContextImpl标记为已经被kill
    _killed = true
    if (context != null) {
      context.markInterrupted()
    }
    if (interruptThread && taskThread != null) { // 如果interruptThread为true
      // 会利用Java线程的中断机制中断任务尝试线程
      taskThread.interrupt()
    }
  }
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Task {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
      task: Task[_],
      currentFiles: mutable.Map[String, Long],
      currentJars: mutable.Map[String, Long],
      serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task properties separately so it is available before full task deserialization.
    val propBytes = Utils.serialize(task.localProperties)
    dataOut.writeInt(propBytes.length)
    dataOut.write(propBytes)

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task)
    Utils.writeByteBuffer(taskBytes, out)
    out.close()
    out.toByteBuffer
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.
   *
   * @return (taskFiles, taskJars, taskProps, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
    : (HashMap[String, Long], HashMap[String, Long], Properties, ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    val propLength = dataIn.readInt()
    val propBytes = new Array[Byte](propLength)
    dataIn.readFully(propBytes, 0, propLength)
    val taskProps = Utils.deserialize[Properties](propBytes)

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, taskProps, subBuffer)
  }
}
