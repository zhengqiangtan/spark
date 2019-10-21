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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.{max, min}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 *                        即TaskSetManager所属的TaskSchedulerImpl
 * @param taskSet         the TaskSet to manage scheduling for
 *                        当前TaskSetManager管理的TaskSet
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 *                        最大任务失败次数
 * @param clock           系统时钟
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  private val conf = sched.sc.conf

  // Quantile of tasks at which to start speculation
  // 开始推测执行的任务分数。可以通过spark.speculation.quantile属性进行配置，默认为0.75。
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  // 推测的乘数，将用于可推测执行Task的检查。可通过spark.speculation.multiplier属性进行配置，默认为1.5。
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Limit of bytes for total size of results (default is 1GB)
  // 结果总大小的字节限制，默认为1GB。可通过spark.driver.maxResultSize属性进行配置。
  val maxResultSize = Utils.getMaxResultSize(conf)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  // TaskSet包含的Task数组，即TaskSet的tasks属性。
  val tasks = taskSet.tasks
  // TaskSet包含的Task的数量，即tasks数组的长度。
  val numTasks = tasks.length

  /**
    * 对每个Task的复制运行数进行记录的数组。
    * copiesRunning按照索引与tasks数组的同一索引位置的Task相对应，记录对应Task的复制运行数量。
    */
  val copiesRunning = new Array[Int](numTasks)

  /**
    * 对每个Task是否执行成功进行记录的数组。
    * successful按照索引与tasks数组的同一索引位置的Task相对应，记录对应的Task是否执行成功。
    */
  val successful = new Array[Boolean](numTasks)

  /**
    * 对每个Task的执行失败次数进行记录的数组。
    * numFailures按照索引与tasks数组的同一索引位置的Task相对应，记录对应Task的执行失败次数。
    */
  private val numFailures = new Array[Int](numTasks)

  /**
    * 对每个Task的所有执行尝试信息进行记录的数组。
    * taskAttempts按照索引与tasks数组的同一索引位置的Task相对应，记录对应Task的所有TaskInfo。
    */
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)

  // 执行成功的Task数量
  var tasksSuccessful = 0

  // 用于公平调度算法的权重
  var weight = 1
  // 用于公平调度算法的参考值
  var minShare = 0
  // 进行调度的优先级
  var priority = taskSet.priority
  // 调度池所属的Stage的身份标识
  var stageId = taskSet.stageId
  // TaskSetManager的名称，可能会参与到公平调度算法中
  val name = "TaskSet_" + taskSet.id
  // TaskSetManager的父Pool
  var parent: Pool = null
  // 所有Task执行结果的总大小
  var totalResultSize = 0L
  // 计算过的Task数量
  var calculatedTasks = 0

  // TaskSetManager所管理的TaskSet的Executor或节点的黑名单
  private val taskSetBlacklistHelperOpt: Option[TaskSetBlacklist] = {
    if (BlacklistTracker.isBlacklistEnabled(conf)) {
      Some(new TaskSetBlacklist(conf, stageId, clock))
    } else {
      None
    }
  }

  // 正在运行的Task的集合
  val runningTasksSet = new HashSet[Long]

  // 正在运行的Task的数量
  override def runningTasks: Int = runningTasksSet.size

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  /**
    * 当TaskSetManager所管理的TaskSet中的所有Task都执行成功了，不再有更多的Task尝试被启动时，就处于“僵尸”状态。
    * 例如，每个Task至少有一次尝试成功，或者TaskSet被舍弃了，TaskSetManager将会进入“僵尸”状态。
    * 直到所有的Task都运行成功为止，TaskSetManager将一直保持在“僵尸”状态。
    * TaskSetManager的“僵尸”状态并不是无用的，在这种状态下TaskSetManager将继续跟踪、记录正在运行的Task。
    */
  var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. These collections may contain duplicates
  // for two reasons:
  // (1): Tasks are only removed lazily; when a task is launched, it remains
  // in all the pending lists except the one that it was launched from.
  // (2): Tasks may be re-added to these lists multiple times as a result
  // of failures.
  // Duplicates are handled in dequeueTaskFromList, which ensures that a
  // task hasn't already started running before launching it.
  // 每个Executor上待处理的Task的集合，即Executor的身份标识与待处理Task的身份标识的集合之间的映射关系。
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  // 每个Host上待处理的Task的集合，即Host与待处理Task的身份标识的集合之间的映射关系。
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  // 每个机架上待处理的Task的集合，即机架与待处理Task的身份标识的集合之间的映射关系。
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  // 没有任何本地性偏好的待处理Task的身份标识的集合
  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  // 所有待处理的Task的身份标识的集合
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  /**
    * 能够进行推测执行的Task的身份标识的集合。这类Task占所有Task的比例非常小。
    * 所谓推测执行，是指当Task的一次尝试运行非常缓慢，
    * 根据推测，如果此时可以另起一次尝试运行，后来的尝试运行也比原先的尝试运行要快。
    */
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  // Task的身份标识与TaskAttempt的信息（如启动时间、完成时间等）之间的映射关系。
  val taskInfos = new HashMap[Long, TaskInfo]

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  // 异常打印到日志的时间间隔，默认为10000。可通过spark.logging.exceptionPrintInterval属性进行配置。
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  // 缓存异常信息、异常次数及最后发生此异常的时间之间的映射关系。此属性与EXCEPTION_PRINT_INTERVAL配合使用。
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  // MapOutputTracker的epoch，用于故障转移
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  // 将所有Task添加到待处理列表，该操作会翻转Task索引进行遍历，以保证索引越小的Task越先被启动
  for (i <- (0 until numTasks).reverse) {
    // 调用addPendingTask()方法进行处理
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  // Task的本地性级别的数组，通过computeValidLocalityLevels()方法计算
  var myLocalityLevels = computeValidLocalityLevels()
  // 与myLocalityLevels中的每个本地性级别相对应，表示对应本地性级别的等待时间。
  var localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  // 当前的本地性级别在myLocalityLevels中的索引
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  // 在当前的本地性级别上运行Task的时间
  var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  // 当发现序列化后的Task的大小超过了100KB，此属性将被设置为true，并且打印警告级别的日志。
  var emittedTaskSizeWarning = false

  /** Add a task to all the pending-task lists that it should be on.
    * 将待处理Task的索引按照Task的偏好位置，
    * 添加到pendingTasksForExecutor、pendingTasksForHost、pendingTasksForRack、
    *   pendingTasksWithNoPrefs、allPendingTasks等缓存中。
    **/
  private def addPendingTask(index: Int) {
    // 获取tasks中指定索引位置上的Task的偏好位置序列，并遍历这些偏好位置序列
    for (loc <- tasks(index).preferredLocations) { // 遍历的是TaskLocation对象
      // 进行匹配，根据不同的偏好位置，更新到不同的字典中进行记录
      loc match {
        case e: ExecutorCacheTaskLocation => // Task所需要计算的数据在Executor上
          // 更新到pendingTasksForExecutor字典
          pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation => // Task所需要计算的数据存储在HDFS上
          // 获取对应节点上还存活的Executor集合
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) => // 存在还存活的Executor
              // 遍历所有存活的Executor
              for (e <- set) {
                // 更新到pendingTasksForExecutor字典
                pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        case _ => // 其它偏好位置，无处理
      }
      // 更新pendingTasksForHost
      pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      // 获取Task本地性级别中节点所在的机架，更新pendingTasksForRack
      for (rack <- sched.getRackForHost(loc.host)) {
        pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer) += index
      }
    }

    // 如果Task没有偏好位置信息，则将其索引添加到pendingTasksWithNoPrefs中进行记录
    if (tasks(index).preferredLocations == Nil) {
      pendingTasksWithNoPrefs += index
    }

    // 将所有Task的索引添加到allPendingTasks
    allPendingTasks += index  // No point scanning this whole list to find the old task there
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   *
   * 用于从给定的Task列表中按照索引，
   * 从高到低找出满足条件（不在黑名单中、Task的复制运行数等于0、Task没有成功）的Task的索引
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int]): Option[Int] = {
    // 获取被过滤的Task数量
    var indexOffset = list.size
    // 进行遍历
    while (indexOffset > 0) {
      indexOffset -= 1
      // 获取对应的索引
      val index = list(indexOffset)
      // 判断对应的Task是否被指定的Executor或节点拉黑
      if (!isTaskBlacklistedOnExecOrNode(index, execId, host)) { // 没有被拉黑
        // This should almost always be list.trimEnd(1) to remove tail
        // 从list中移除对应的索引
        list.remove(indexOffset)
        // 判断Task是否存在复制任务，或者Task是否已经成功
        if (copiesRunning(index) == 0 && !successful(index)) {
          // Task没有复制任务，且还未完成，则将其返回
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host
   * 判断指定节点上是否已经存在指定Task的推测执行任务
   */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  // 判断某个Executor或节点是否拉黑了指定的Task
  private def isTaskBlacklistedOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    // 判断Executor或节点黑名单中是否拉黑了指定的Task
    taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTask(host, index) ||
        blacklist.isExecutorBlacklistedForTask(execId, index)
    }
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   *
   * 用于根据指定的Host、Executor和本地性级别，
   * 从可推测执行的Task中找出可推测执行的Task在TaskSet中的索引和相应的本地性级别
   *
   * Labeled as protected to allow tests to override providing speculative tasks if necessary
   */
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    // 从speculatableTasks集合中移除已经完成的Task
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    // 判断某个推测执行Task是否可以运行在指定的Host上
    def canRunOnHost(index: Int): Boolean = {
      !hasAttemptOnHost(index, host) && // 对应Host上还没有该Task的推测执行任务
        !isTaskBlacklistedOnExecOrNode(index, execId, host) // 该Task没有将对应Executor或Host拉黑了
    }

    if (!speculatableTasks.isEmpty) { // 非空才会执行
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming

      // 检查是否满足PROCESS_LOCAL本地化
      // 遍历所有可以在对应Host上运行推测执行任务的Task
      for (index <- speculatableTasks if canRunOnHost(index)) {
        // 获取Task运行的偏好位置
        val prefs = tasks(index).preferredLocations
        // 获取偏好位置对应的Executor的ID
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });
        // 如果指定的Executor属于偏好位置中的Executor，就说明该Task可以推测执行
        if (executors.contains(execId)) {
          // 从speculatableTasks中移除
          speculatableTasks -= index
          // 返回索引和本地化级别
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      // 不满足PROCESS_LOCAL本地化偏好，检查指定的本地性级别小于等于NODE_LOCAL本地化偏好
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        // 遍历所有可以在对应Host上运行推测执行任务的Task
        for (index <- speculatableTasks if canRunOnHost(index)) {
          // 获取Task运行的偏好位置所在的Host
          val locations = tasks(index).preferredLocations.map(_.host)
          // 如果指定的Host属于偏好位置中的Host，就说明该Task可以推测执行
          if (locations.contains(host)) {
            // 从speculatableTasks中移除
            speculatableTasks -= index
            // 返回索引和本地化级别
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      // 不满足NODE_LOCAL本地化偏好，检查指定的本地性级别小于等于NO_PREF本地化偏好
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        // 遍历所有可以在对应Host上运行推测执行任务的Task
        for (index <- speculatableTasks if canRunOnHost(index)) {
          // 获取Task运行的偏好位置
          val locations = tasks(index).preferredLocations
          // 偏好位置数量为0，说明该Task可以推测执行
          if (locations.size == 0) {
            // 从speculatableTasks中移除
            speculatableTasks -= index
            // 返回索引和本地化级别
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      // 不满足NO_PREF本地化偏好，检查指定的本地性级别小于等于RACK_LOCAL本地化偏好
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        // 获取TaskSchedulerImpl的机架信息
        for (rack <- sched.getRackForHost(host)) {
          // 遍历所有可以在对应Host上运行推测执行任务的Task
          for (index <- speculatableTasks if canRunOnHost(index)) {
            // 获取Task运行的偏好位置的机架信息
            val racks = tasks(index).preferredLocations.map(_.host).flatMap(sched.getRackForHost)
            // Task运行的偏好位置的机架信息包含TaskSchedulerImpl的机架信息，说明该Task可以推测执行
            if (racks.contains(rack)) {
              // 从speculatableTasks中移除
              speculatableTasks -= index
              // 返回索引和本地化级别
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      // 不满足RACK_LOCAL本地化偏好，检查指定的本地性级别小于等于ANY本地化偏好，即无本地偏好
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        // 遍历所有可以在对应Host上运行推测执行任务的Task
        for (index <- speculatableTasks if canRunOnHost(index)) {
          // 从speculatableTasks中移除
          speculatableTasks -= index
          // 返回索引和本地化级别
          return Some((index, TaskLocality.ANY))
        }
      }
    }
    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   * 根据指定的Host、Executor和本地性级别，找出要执行的Task的索引、相应的本地性级别及是否进行推测执行
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    for (index <- dequeueTaskFromList(execId, host, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false)) // 选择指定Executor的待处理Task
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, host, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false)) // 选择指定Host上的待处理Task
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, host, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false)) // 选择没有本地性偏好的待处理Task
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, host, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false)) // 选择指定Host所在机架上的待处理Task
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, host, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false)) // 选择在任意的本地性上的待处理Task
      }
    }

    // find a speculative task if all others tasks have been scheduled
    // 上面的基本本地性级别都没有可调度的Task，则尝试找一个推测执行Task进行调度。
    dequeueSpeculativeTask(execId, host, maxLocality).map { // 选择可推断的Task及其本地性
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * 根据给定资源和本地性级别提供运行的Task
   *
   * @param execId      the executor Id of the offered resource
   *                    指定的Executor
   * @param host        the host Id of the offered resource
   *                    指定的节点
   * @param maxLocality the maximum locality we want to schedule the tasks at
   *                    可提供的最大本地性级别
   *
   * @return 可以运行的Task的TaskDescription
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    // 根据Host和Executor的黑名单判断指定的Host和Executor是否可以运行Task
    val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTaskSet(host) ||
        blacklist.isExecutorBlacklistedForTaskSet(execId)
    }
    // 当前TaskSetManager不处于"僵尸"状态，且指定Host和Executor不在黑名单中才执行
    if (!isZombie && !offerBlacklisted) {
      // 当前时间
      val curTime = clock.getTimeMillis()

      // 指定的允许的最大本地化级别
      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        // 计算允许的本地性级别，会传入当前时间
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      // 根据指定的Host、Executor和本地性级别，找到合适的Task
      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) =>
        // Found a task; do some bookkeeping and return a task description
        // 根据要执行的Task的索引找到要执行的Task
        val task = tasks(index)
        // 为Task生成新的身份标识
        val taskId = sched.newTaskId()
        // Do various bookkeeping
        // 增加复制运行数
        copiesRunning(index) += 1
        // 获取TaskAttempt号attemptNum
        val attemptNum = taskAttempts(index).size
        // 创建Task尝试信息
        val info = new TaskInfo(taskId, index, attemptNum, curTime,
          execId, host, taskLocality, speculative)
        // 将Task的身份标识与TaskInfo的对应关系放入taskInfos
        taskInfos(taskId) = info
        // 将TaskInfo添加到taskAttempts中
        taskAttempts(index) = info :: taskAttempts(index)
        // Update our locality level for delay scheduling
        // NO_PREF will not affect the variables related to delay scheduling
        // 获取Task的本地性偏好级别
        if (maxLocality != TaskLocality.NO_PREF) {
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
        }
        // Serialize and return the task
        val startTime = clock.getTimeMillis()
        // 序列化Task
        val serializedTask: ByteBuffer = try {
          Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
        } catch {
          // If the task cannot be serialized, then there's no point to re-attempt the task,
          // as it will always fail. So just abort the whole task-set.
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            abort(s"$msg Exception during serialization: $e")
            throw new TaskNotSerializableException(e)
        }
        if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
          !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
        }
        // 将Task放入正在运行的Task中
        addRunningTask(taskId)

        // We used to log the time it takes to serialize the task, but task size is already
        // a good proxy to task serialization time.
        // val timeTaken = clock.getTime() - startTime
        // 生成Task的名称
        val taskName = s"task ${info.id} in stage ${taskSet.id}"
        logInfo(s"Starting $taskName (TID $taskId, $host, executor ${info.executorId}, " +
          s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit} bytes)")

        sched.dagScheduler.taskStarted(task, info)
        // 创建并返回TaskDescription对象
        new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
          taskName, index, serializedTask)
      }
    } else {
      None
    }
  }

  // 当TaskSet可能已经完成的时候进行一些清理工作
  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   *
   * 用于获取可以启动Task的本地性级别，需要根据基于等待时间的延迟调度来计算。
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    /**
     * 用于判断给定的待处理Task数组中是否有需要被调度的Task，
     * 在判断期间，还会将已经启动TaskAttempt或者已执行完成的Task从对应的集合中移除。
     * 这里的集合是指pendingTasksForExecutor、pendingTasksForHost或pendingTasksForRack。
     *
     * 该方法其实很简单：
     * 1. 如果以上三类集合中存在未完成且未启动TaskAttempt的Task，则表示还存在需要被调度的Task，直接返回true。
     * 2. 如果存在已启动TaskAttempt的Task，或者是已完成的Task，则将它们从集合中移除。
     * 3. 如果遍历完没有找到第1种情况所描述的Task，则表示没有需要被调度的Task，返回false。
     */
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      // 从后向前遍历待处理的Task的ID集合
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        // 维护索引
        indexOffset -= 1
        // 取出对应的Task的ID
        val index = pendingTaskIds(indexOffset)
        // 如果该Task的复制运行数为0，且该Task还没有成功执行完
        if (copiesRunning(index) == 0 && !successful(index)) {
          // 该Task需要被调度
          return true
        } else {
          // 否则说明该Task正在运行，或者已经运行完了，则可以移除该Task
          pendingTaskIds.remove(indexOffset)
        }
      }
      // 没有找到需要被调度的Task，返回false
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    /**
     * 判断本地性级别对应的待处理Task的缓存结构中是否有Task需要处理
     * 该方法在后面会被调用，传入的pendingTasks参数将会是
     * pendingTasksForExecutor、pendingTasksForHost、pendingTasksForRack等集合
     */
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      // 判断待处理Task集合pendingTasks中是否有需要被调度的Task
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>

          /**
           * 使用tasksNeedToBeScheduledFrom()判断指定集合是否还有需要被调度的Task。
           * 注意该方法的返回值：
           * 1. 返回true，表示还存在需要被调度的Task。
           * 2. 返回false，表示不存在需要被调度的Task。
           *
           * 如果返回false，则表示对应的Executor、Host或Rack上没有待调度的Task了，
           * 因此可以在else分支记录这些Executor、Host或Rack的ID，最终将它们移除。
           */
          if (tasksNeedToBeScheduledFrom(tasks)) {
            // 如果有，直接返回true
            true
          } else {
            // 否则记录该Executor的ID，并返回false
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      /**
       * 遍历记录的key，这里的key可以是executorId、host、rackId，
       * 然后从pendingTasksForExecutor、pendingTasksForHost、pendingTasksForRack等集合中，
       * 移除以executorId、host、rackId为键对应的键值对，因为它们已经没有待调度的Task了。
       */
      emptyKeys.foreach(id => pendingTasks.remove(id))
      // 返回需要被调度的Task集合
      hasTasks
    }

    // 从currentLocalityIndex所记录的索引开始，在myLocalityLevels数组中遍历本地性级别
    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      // 查找每个遍历到的本地性级别是否有Task需要被调度
      val moreTasks = myLocalityLevels(currentLocalityIndex) match { // 根据支持的本地性进行分别判断
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }

      // 判断遍历到的本地性级别是否有需要被调度的Task
      if (!moreTasks) { // 当前遍历到的本地性级别没有需要被调度的Task
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        // 将最后的Task启动时间更新为curTime
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        // 因为该本地性级别没有Task需要被调度，因此直接跳入下一个本地性级别
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        /**
         * curTime - lastLaunchTime表示距离上一次Task被调度的时间，
         * 首先，走到这里说明当前遍历到的本地性级别存在需要被调度的Task，
         * 同时，距离上一次Task被调度的时间超过了该本地性级别所要求的等待时间，
         * 由于等待时间太长，放弃调度该本地性级别上的待调度Task。
         *
         * 更新lastLaunchTime，加上当前遍历到的本地化级别的等待时间。
         * 然后跳入更低的本地性级别。
         */
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        //
        lastLaunchTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        // 跳入更低的本地性级别
        currentLocalityIndex += 1
      } else {
        /**
         * 走到这里，说明当前遍历到的本地性级别中存在需要被调度的Task，
         * 且距离上一次Task被调度的时间还未超过该本地性级别要求的等待时间，
         * 因此可以调度该本地性级别中待调度的Task，返回当前本地性级别即可。
         */
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    // 未能找到允许的本地性级别，那么返回最终遍历到的极低本地性级别
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
    *
    * 从myLocalityLevels中找出指定的本地性级别所对应的索引
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been blacklisted to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * blacklist.  The most common scenario would be if there are fewer executors than
   * spark.task.maxFailures. We need to detect this so we can fail the task set, otherwise the job
   * will hang.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't blacklisted on).
   *
   * 判断当前TaskSet是否被指定的Host或Executor拉黑，
   * 如果被拉黑就终止当前TaskSet的执行；
   * 该方法判断的是极端情况，即TaskSet已经没有可以运行的节点或Executor了。
   *
   * @param hostToExecutors 指定的Host及该Host上的Executor集合
   */
  private[scheduler] def abortIfCompletelyBlacklisted(
      hostToExecutors: HashMap[String, HashSet[String]]): Unit = {
    // 遍历Executor和节点黑名单
    taskSetBlacklistHelperOpt.foreach { taskSetBlacklist =>
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          // 所有等待执行Task集合allPendingTasks中最后一个没有复制任务且未完成的Task的索引
          val indexOffset = allPendingTasks.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }

          if (indexOffset == -1) {
            // 没有获取到，返回None
            None
          } else {
            // 否则返回Task的索引
            Some(allPendingTasks(indexOffset))
          }
        }

        // 遍历待执行的Task
        pendingTask.foreach { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          // 遍历传入的hostToExecutors参数中的Host和Executor，判断TaskSet是否被这些Host或Executor拉黑
          val blacklistedEverywhere = hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            // 判断Host是否拉黑了TaskSet，或者该Host是否拉黑了待执行的Task
            val nodeBlacklisted =
              taskSetBlacklist.isNodeBlacklistedForTaskSet(host) ||
              taskSetBlacklist.isNodeBlacklistedForTask(host, indexInTaskSet)
            if (nodeBlacklisted) {
              // 如果被拉黑，返回true
              true
            } else {
              // Check if the task can run on any of the executors
              // 否则判断Executor是否拉黑了TaskSet，或者该Executor是否拉黑了待执行的Task
              execsOnHost.forall { exec =>
                  taskSetBlacklist.isExecutorBlacklistedForTaskSet(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTask(exec, indexInTaskSet)
              }
            }
          }
          if (blacklistedEverywhere) { // 如果存在被拉黑情况，则终止当前的TaskSet的执行
            val partition = tasks(indexInTaskSet).partitionId
            abort(s"Aborting $taskSet because task $indexInTaskSet (partition $partition) " +
              s"cannot run anywhere due to node and executor blacklist.  Blacklisting behavior " +
              s"can be configured via spark.blacklist.*.")
          }
        }
      }
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   * 标记指定的Task正在获取计算结果，并通知DAGScheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    // 获取Task对应的TaskInfo
    val info = taskInfos(tid)
    // 标记Task被拉取数据的
    info.markGettingResult()
    // 通知DAGScheduler
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   * 检查是否有足够的配额可供记录结果数据；当Task运行成功后，会使用该方法判断结果数据是否超限
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    // 所有Task执行结果的总大小自增
    totalResultSize += size
    // 计算完成的Task数量自增
    calculatedTasks += 1
    // 当总的结果大小大于最大限制大小（默认1GB）时，终止TaskSet
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
   *
   * 用于对Task的执行结果（对于map任务而言，实际是任务状态）进行处理。
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    // 获取对应的TaskInfo对象
    val info = taskInfos(tid)
    val index = info.index
    // 将taskInfos中缓存的TaskInfo标记为已经完成
    info.markFinished(TaskState.FINISHED)
    // 将Task从正在运行的Task集合中移除
    removeRunningTask(tid)
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    // 将Task的执行结果交给DagScheduler处理
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates, info)
    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    // 杀死此Task正在运行的Task尝试
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for task ${attemptInfo.id} " +
        s"in stage ${taskSet.id} (TID ${attemptInfo.taskId}) on ${attemptInfo.host} " +
        s"as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      sched.backend.killTask(attemptInfo.taskId, attemptInfo.executorId, true)
    }
    if (!successful(index)) {
      // 增加运行成功的Task数量
      tasksSuccessful += 1
      logInfo(s"Finished task ${info.id} in stage ${taskSet.id} (TID ${info.taskId}) in" +
        s" ${info.duration} ms on ${info.host} (executor ${info.executorId})" +
        s" ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      // 将此任务的状态设置为true
      successful(index) = true
      // 如果TaskSet中的所有Task都调度运行成功，将isZombie设置为true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    // 在TaskSet可能已经完成的时候进行一些清理工作
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   *
   * 将Task标记为指定的状态，重新将其添加到待处理集合，同时会通知DAGScheduler
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason) {
    // 获取Task的TaskInfo
    val info = taskInfos(tid)
    if (info.failed || info.killed) {
      return
    }
    // 将该Task从runningTasksSet中移除
    removeRunningTask(tid)
    // 将Task标记为指定的状态
    info.markFinished(state)
    // 将该Task的复制运行数减1
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    // 根据传入的失败理由构造提示信息
    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    // 根据传入的失败理由构造失败异常
    val failureException: Option[Throwable] = reason match {
      // 拉取Shuffle数据失败，此时可能需要重新运行前一个Stage
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        // 将Task标记为成功
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // 将isZombie标记为true，表示当前TaskSet将被舍弃
        isZombie = true
        None

      // 运行时异常
      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        // 获取计数器
        accumUpdates = ef.accums
        if (ef.className == classOf[NotSerializableException].getName) {
          // 未序列化异常
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          // 终止TaskSet
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          // 直接返回
          return
        }
        // 不是未序列化异常
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) { // 缓存的最近发生的异常中包括该异常
            // 获取出现的次数和最近出现的时间
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) { // 判断异常再次出现的间隔时间是否大于10秒
              // 大于10秒，记录异常，只更新时间
              recentExceptions(key) = (0, now)
              // 返回值表示是否记录详细信息，及重复出现的次数
              (true, 0)
            } else {
              // 不大于10秒，异常次数自增
              recentExceptions(key) = (dupCount + 1, printTime)
              // 返回值表示是否记录详细信息，及重复出现的次数
              (false, dupCount + 1)
            }
          } else {
            // 缓存中没有该异常，则记录该异常
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }

        // 根据不同情况记录日志
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on ${info.host}, executor" +
              s" ${info.executorId}: ${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      // Executor丢失异常，且不是由App导致的
      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $tid failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      // Task相关的异常
      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None
    }

    // 通知DAGScheduler
    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, info)

    if (successful(index)) { // 如果该存在其他Task已完成，则记录日志
      logInfo(
        s"Task ${info.id} in stage ${taskSet.id} (TID $tid) failed, " +
        "but another instance of the task has already succeeded, " +
        "so not re-queuing the task to be re-executed.")
    } else {
      // 重新调度Task
      addPendingTask(index)
    }

    /**
     * reason.countTowardsTaskFailures字段用于标记是否将本次的失败归咎于Task自身。
     * 这个字段为true则会增加Task失败次数的计数；当为false时则不会。
     * 例如，当Executor宕机或被杀死时触发Task的失败，则不应该归咎于Task自身。
     */
    if (!isZombie && reason.countTowardsTaskFailures) {
      taskSetBlacklistHelperOpt.foreach(_.updateBlacklistForFailedTask(
        info.host, info.executorId, index))
      assert (null != failureReason)
      // 将Task的失败次数自增1
      numFailures(index) += 1
      // 判断失败次数是否超过最大允许的失败次数，默认为4
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        // 超过最大失败次数，终止TaskSet
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }
    // 可能的清理工作
    maybeFinishTaskSet()
  }

  // 用于终止当前TaskSetManager管理的所有Task的运行
  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    // 调用DAGScheduler告知终止运行了
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    // 将isZombie标记置为true
    isZombie = true
    // 进行清理工作
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   *
   * 向runningTasksSet中添加Task的身份标识，
   * 并调用TaskSetManager的父调度池的increaseRunningTasks方法，
   * 增加父调度池及祖父调度池中记录的当前正在运行的任务数量。
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it.
   * 从runningTasksSet中移除Task的身份标识，
   * 并调用TaskSetManager的父调度池的decreaseRunningTasks方法，
   * 减少父调度池及祖父调度池中记录的当前正在运行的任务数量。
   */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks
   * 在发生Executor丢失的情况下被调用
   */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason) {
    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    // 如果是ShuffleMapTask，且没有外部Shuffle服务
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled) {
      // 遍历得到Task和它TaskAttempt的TaskInfo对象，这些Task所运行的Executor即是丢失的Executor
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        /**
         * 由于没有使用外部Shuffle服务，而且是ShuffleMapTask，Executor丢失后，
         * 就算存在执行成功的Task，但因为Executor丢失了，因此Map任务输出的Shuffle数据也会丢失，
         * 因此需要对这些Task进行重新计算，此时会触发Stage的重新提交。
         */
        if (successful(index)) { // 如果Task已经成功
          successful(index) = false // 将此Task标记为未成功
          copiesRunning(index) -= 1 // 将此Task的复制运行数量减一
          tasksSuccessful -= 1 // 将当前TaskSetManager中成功执行的Task数量减一
          addPendingTask(index) // 将此Task添加到待处理的Task中
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            // 告知DAGScheduler重新提交Task
            tasks(index), Resubmitted, null, Seq.empty, info)
        }
      }
    }

    // 遍历得到Task和它TaskAttempt的TaskInfo对象，这些Task正在运行，且所运行的Executor即是丢失的Executor
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      // 根据Executor丢失具体情况得到exitCausedByApp的值
      val exitCausedByApp: Boolean = reason match {
        // 获取Executor丢失的具体原因是否是由应用程序引起的
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      // 对失败的Task进行处理
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    // 重新计算本地性级别
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   *
   * 用于检查当前TaskSetManager中是否有需要推测执行的Task
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false // 没有可以推测的Task
    }
    // 标记是否找到了需要推测执行的Task
    var foundTasks = false
    // 根据SPECULATION_QUANTILE与总任务数numTasks计算阈值，该阈值用于决定该不该检测是否需要进行推测执行
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    // 当成功完成的任务数量大于该阈值时进行检测
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      // 当前时间
      val time = clock.getTimeMillis()
      // 计算所有已成功完成的任务所耗的时间并从小到大进行排序
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      // 计算耗时中间值
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.length - 1))
      // 计算阈值
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      // 遍历Task的身份标识与TaskAttempt的信息之间的映射字典
      for ((tid, info) <- taskInfos) {
        // 获取该Task在TaskSet中的索引
        val index = info.index
        if (!successful(index) &&  // 如果该Task并没有执行成功
          copiesRunning(index) == 1 &&  // 复制运行数为1
          info.timeRunning(time) > threshold && // Task的运行时间大于阈值
          !speculatableTasks.contains(index)) { // 该Task并没有进行过推测执行
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          // 将该Task的索引添加到speculatableTasks，表示对其进行推测执行
          speculatableTasks += index
          // 更新foundTasks标记
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  // 用于获取某个本地性级别的等待时间
  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    // 默认为3秒
    val defaultWait = conf.get("spark.locality.wait", "3s")
    // 根据本地性级别的不同获取不同的配置项
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }

    if (localityWaitKey != null) {
      // 从SparkConf中获取
      conf.getTimeAsMs(localityWaitKey, defaultWait)
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   * 用于计算有效的本地性级别，这样就可以将Task按照本地性级别，由高到低地分配给允许的Executor。
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    // 导入本地化级别的标量
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    // 构造一个数组
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]

    if (!pendingTasksForExecutor.isEmpty && // Executor上待处理Task集合不为空
      getLocalityWait(PROCESS_LOCAL) != 0 && // PROCESS_LOCAL级别的等待时间不为0
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) { // 还存在已被激活的Executor
      levels += PROCESS_LOCAL // 允许的本地性级别里包括PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && // Host上待处理的Task集合不为空
      getLocalityWait(NODE_LOCAL) != 0 && // NODE_LOCAL级别的等待时间不为0
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) { // Host上存在已被激活的Executor
      levels += NODE_LOCAL // 允许的本地性级别里包括NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) { // 存在没有任何本地性偏好的待处理Task
      levels += NO_PREF // 允许的本地性级别里包括NO_PREF
    }
    if (!pendingTasksForRack.isEmpty && // 机架上待处理的Task的集合不为空
      getLocalityWait(RACK_LOCAL) != 0 && // RACK_LOCAL级别的等待时间不为0
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) { // 机架上存在已被激活的Executor
      levels += RACK_LOCAL // 允许的本地性级别里包括RACK_LOCAL
    }
    levels += ANY // 允许的本地性级别里增加ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray // 返回所有允许的本地性级别
  }

  def recomputeLocality() {
    // 获取currentLocalityIndex索引值所记录的本地化级别
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    // 更新支持的本地化级别数组
    myLocalityLevels = computeValidLocalityLevels()
    // 更新本地化级别所需要的等待的时间
    localityWaits = myLocalityLevels.map(getLocalityWait)
    // 按照currentLocalityIndex之前所记录的本地化级别更新它的索引值
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}
