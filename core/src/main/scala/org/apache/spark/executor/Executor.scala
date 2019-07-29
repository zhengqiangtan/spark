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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{AccumulableInfo, DirectTaskResult, IndirectTaskResult, Task}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 *
  * @param executorId Executor的身份标识
  * @param executorHostname 当前Executor所在的主机名
  * @param env SparkEnv
  * @param userClassPath 用户指定的类路径。可通过spark.executor.extraClassPath属性进行配置。如果有多个类路径，可以在配置时用英文逗号分隔。
  * @param isLocal 是否是Local部署模式
  */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  // 当前执行的Task所需要的文件
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  // 当前执行的Task所需要的Jar包
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  // 空的ByteBuffer
  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  // Start worker thread pool
  // 线程池，运行的线程将以Executor task launch worker作为前缀
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")
  // Executor的度量来源
  private val executorSource = new ExecutorSource(threadPool, executorId)

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  // 是否首先从用户指定的类路径中加载类，然后再去Spark的Jar文件中加载。可通过spark.executor.userClassPathFirst属性配置，默认为false。
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  // Task需要的类加载器
  private val urlClassLoader = createClassLoader()
  // 当使用交互式解释器环境（Read-Evaluate-Print-Loop，简称REPL）时，此类加载器用于加载REPL根据用户键入的代码定义的新类型。
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  /**
    * 直接结果的最大大小。取spark.task.maxDirectResultSize属性（默认为1L<<20，即1048576）
    * 与spark.rpc.message.maxSize属性（默认为128MB）之间的最小值。
    */
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  /**
    * 结果的最大限制。默认为1GB。
    * Task运行的结果如果超过maxResultSize，则会被删除。
    * Task运行的结果如果小于等于maxResultSize且大于maxDirectResultSize，则会写入本地存储体系。
    * Task运行的结果如果小于等于maxDirectResultSize，则会直接返回给Driver。
    */
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  // 用于维护正在运行的Task的身份标识与TaskRunner之间的映射关系。
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  // 只有一个线程的ScheduledThreadPoolExecutor，此线程池运行的线程以driver-heartbeater作为名称。
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  // HeartbeatReceiver的RpcEndpointRef
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
    *
    * 心跳的最大失败次数。可通过spark.executor.heart-beat.maxFailures属性配置，默认为60。
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
    *
    * 心跳失败数的计数器，初始值为0。
   */
  private var heartbeatFailures = 0

  startDriverHeartbeater()

  // 用于运行Task
  def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    // 创建TaskRunner
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    // 将Task的身份标识与TaskRunner的映射关系存入runningTasks字典
    runningTasks.put(taskId, tr)
    // 向线程池提交TaskRunner任务
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean) : Unit = {
    // kill all the running tasks
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner != null) {
        taskRunner.kill(interruptThread)
      }
    }
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  /**
    * TaskRunner实现了Runnable接口，作为线程要执行的任务。
    * @param execBackend 类型为ExecutorBackend。
    *                    SchedulerBackend有CoarseGrainedSchedulerBackend和LocalSchedulerBackend两个子类。
    *                    由于LocalSchedulerBackend也继承了特质ExecutorBackend，并实现了其唯一的方法statusUpdate()，
    *                    所以local部署模式下将LocalSchedulerBackend作为ExecutorBackend。
    *                    其他部署模式使用特质ExecutorBackend的另一个实现类Coarse-GrainedExecutorBackend，
    *                    来跟CoarseGrainedSchedulerBackend配合。
    * @param taskId Task的身份标识
    * @param attemptNumber
    * @param taskName Task的名称
    * @param serializedTask 序列化后的Task
    */
  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskId: Long,
      val attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer)
    extends Runnable {

    /** Whether this task has been killed.
      * Task是否被杀死
      **/
    @volatile private var killed = false

    /** Whether this task has been finished.
      * Task是否完成
      **/
    @GuardedBy("TaskRunner.this")
    private var finished = false

    /** How much the JVM process has spent in GC when the task starts to run.
      * TaskAttempt开始运行前JVM进程执行GC已花费的时间
      **/
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
      *
      * 要运行的Task，通过对Driver传递过来的序列化的Task进行反序列化后获得。
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread)
          }
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
    }

    override def run(): Unit = {

      // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 1. 运行准备 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

      val threadMXBean = ManagementFactory.getThreadMXBean
      // 创建TaskMemoryManager
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      // 设置当前线程上下文的类加载器为replClassLoader
      Thread.currentThread.setContextClassLoader(replClassLoader)
      // 生成新的对闭包序列化的实例ser
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      // 将Task的状态更新为RUNNING
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      var taskStartCpu: Long = 0
      // 计算JVM进程执行GC已经花费的时间
      startGCTime = computeTotalGcTime()

      try {
        /**
          * 对序列化的Task进行反序列化，得到：
          * - taskFiles：任务所需的文件（taskFiles）
          * - taskJars：任务所需的Jar包（taskJars）
          * - taskProps：Task所需的属性信息（task-Props）
          * - taskBytes：任务本身（taskBytes）。
          */
        val (taskFiles, taskJars, taskProps, taskBytes) =
          Task.deserializeWithDependencies(serializedTask)

        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        // 将taskProps放入ThreadLocal
        Executor.taskDeserializationProps.set(taskProps)

        // 从taskFiles和taskJars中获取所需的依赖。
        updateDependencies(taskFiles, taskJars)

        // 将Task的ByteBuffer（即taskBytes）反序列化为Task实例。
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        // 将taskProps保存到Task的localProperties属性中。
        task.localProperties = taskProps
        // 将刚创建的TaskMemoryManager设置为Task的内存管理器。
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 2. 运行 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = try {
          // 调用Task的run()方法进行任务的运行，该方法是模板方法，由子类实现
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {

          // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 3. 资源回收 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

          // 释放所有的Block锁
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          // 清空分配给TaskAttempt的内存
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          // 当前TaskAttempt有内存溢出，抛出异常，记录警告日志
          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          // 当前TaskAttempt存在Block的锁无法释放，抛出异常，记录警告日志
          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }
        }
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 4. 对Task执行返回的结果进行序列化 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 5. 更新相关的度量信息 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>> 6. 将执行结果发送给Driver >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            /**
              * 如果maxResultSize大于0且任务尝试运行的结果超过maxResultSize，
              * 那么只会将结果的大小序列化为serializedResult，并且不会保存执行结果。
              */
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            // 将结果的大小序列化为serializedResult，并不会保存执行结果
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            /**
              * 如果任务尝试运行的结果小于等于maxResultSize且大于maxDirectResultSize，
              * 则会将结果写入本地存储体系，并将结果的大小序列化为serializedResult。
              */
            val blockId = TaskResultBlockId(taskId)
            // 将结果写入本地存储体系
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            // 将结果的大小序列化为serializedResult
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            /**
              * 如果任务尝试运行的结果小于等于maxDirectResultSize，
              * 则会直接将结果序列化为serializedResult。
              */
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            // 直接将结果序列化为serializedResult
            serializedDirectResult
          }
        }

        // 将Task的状态更新为FINISHED，并且将serializedResult发送给Driver
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case _: InterruptedException if task.killed =>
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId)")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // Collect latest accumulator values to report back to the driver
          val accums: Seq[AccumulatorV2[_, _]] =
            if (task != null) {
              task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
              task.collectAccumulatorUpdates(taskFailed = true)
            } else {
              Seq.empty
            }

          val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
            }
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      // 遍历所有的文件
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        // 下载Task所需的文件
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        // 将文件名与时间戳的关系放入currentFiles缓存
        currentFiles(name) = timestamp
      }

      // 遍历所有的Jar包
      for ((name, timestamp) <- newJars) {
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          // 下载Task所需的Jar包
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          // 将Jar文件名与时间戳的关系放入currentJars缓存
          currentJars(name) = timestamp
          // Add it to our class loader
          // 将Jar包添加到urlClassLoader的加载路径中
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver.
    * 报告心跳的任务具体实现方法
    **/
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    // 遍历runningTasks中正在运行的Task
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        // 将每个Task的度量信息更新到数组缓冲accumUpdates中
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    // 创建Heartbeat消息
    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)

    try {
      // 向Driver的HeartbeatReceiver发送Heartbeat消息，并接收HeartbeatReceiver的响应消息HeartbeatResponse。
      val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) { // reregisterBlockManager为true
        logInfo("Told to re-register on heartbeat")
        // 向BlockManagerMaster重新注册BlockManager。
        env.blockManager.reregister()
      }
      // 将heartbeatFailures置为0。
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
    *
    * 在初始化Executor的过程中，Executor会调用该方法启动心跳报告的定时任务。
   */
  private def startDriverHeartbeater(): Unit = {
    // 获取发送心跳报告的时间间隔intervalMs。intervalMs可通过spark.executor.heartbeatInterval属性配置，默认为10s。
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    // 获取心跳定时器第一次执行的时间延迟initialDelay。initialDelay是在intervalMs的基础上累加了随机数。
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    // 定义向Driver报告心跳的任务，任务具体由reportHeartBeat()方法完成
    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }

    // 提交心跳任务
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
