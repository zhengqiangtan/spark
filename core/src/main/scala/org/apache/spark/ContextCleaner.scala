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

package org.apache.spark

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  // 缓存AnyRef的虚引用。
  private val referenceBuffer = new ConcurrentLinkedQueue[CleanupTaskWeakReference]()

  // 缓存顶级的AnyRef引用。
  private val referenceQueue = new ReferenceQueue[AnyRef]

  // 缓存清理工作的监听器数组。
  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  // 用于具体清理工作的线程。此线程为守护线程，名称为Spark Context Cleaner。
  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  /**
    * 用于执行GC（Garbage Collection，垃圾收集）的调度线程池，
    * 此线程池只包含一个线程，启动的线程名称以context-cleaner-periodic-gc开头。
    */
  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * How often to trigger a garbage collection in this JVM.
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
    *
    * 执行GC的时间间隔。可通过spark.cleaner.periodicGC.interval属性进行配置，默认是30分钟。
   */
  private val periodicGCInterval =
    sc.conf.getTimeAsSeconds("spark.cleaner.periodicGC.interval", "30min")

  /**
   * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
    *
    * 清理非Shuffle的其他数据是否是阻塞式的。
    * 可通过spark.cleaner.referenceTracking.blocking属性进行配置，默认是true。
   */
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
    *
    * 清理Shuffle数据是否是阻塞式的。
    * 可通过spark.cleaner.referenceTracking.blocking.shuffle属性进行配置，默认是false。
    * 清理Shuffle数据包括清理MapOutputTracker中指定ShuffleId对应的map任务状态
    * 和ShuffleManager中注册的ShuffleId对应的Shuffle元数据。
   */
  private val blockOnShuffleCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking.shuffle", false)

  // ContextCleaner是否停止的状态标记。
  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  def start(): Unit = {
    // 将cleaningThread设置为守护线程。
    cleaningThread.setDaemon(true)
    // 指定名称为Spark Context Cleaner。
    cleaningThread.setName("Spark Context Cleaner")
    // 启动cleaningThread线程。
    cleaningThread.start()
    // 设置以periodicGCInterval作为时间间隔定时进行GC操作的任务。
    periodicGCService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = System.gc()
    }, periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected. */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          // 匹配Task进行不同的清理
          reference.map(_.task).foreach { task =>
            logDebug("Got cleaning task " + task)
            referenceBuffer.remove(reference.get)
            task match {
              case CleanRDD(rddId) => // RDD
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) => // Shuffle
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) => // Broadcast
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) => // Accumulator
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) => // Checkpoint
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      // 移除RDD持久化
      sc.unpersistRDD(rddId, blocking)
      // 调用所有监听器的rddCleaned方法，移除对RDD的跟踪监听器
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup. */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logInfo("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logInfo("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}
