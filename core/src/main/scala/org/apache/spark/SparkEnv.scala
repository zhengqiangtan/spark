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

import java.io.File
import java.net.Socket

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  // 标识当前SparkEnv是否停止的状态
  private[spark] var isStopped = false
  // 所有python实现的Worker的缓存
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  /**
    * HadoopRDD进行任务切分时所需要的元数据的软引用。
    * 例如，HadoopFileRDD将使用hadoopJobMetadata缓存JobConf和InputFormat。
    */
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  // 如果当前SparkEnv处于Driver实例中，那么将创建Driver的临时目录。
  private[spark] var driverTmpDir: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    // 检查Driver Host
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    // Driver的绑定地址
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    // Driver的监听地址
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get("spark.driver.port").toInt

    // IO加密密钥
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) { // spark.io.encryption.enabled属性为true时
      // 调用CryptoStreamUtils的createKey方法创建密钥。
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }

    // 创建并返回
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      port,
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      hostname,
      port,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    // 安全管理器
    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isSaslEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    // RpcEnv
    /**
      * 生成系统名称：
      *   - 如果当前应用为Driver（即SparkEnv位于Driver中），那么systemName为sparkDriver；
      *   - 否则（即SparkEnv位于Executor中），systemName为sparkExecutor。
      */
    val systemName = if (isDriver) driverSystemName else executorSystemName
    // 创建RpcEnv，注意最后一个参数，只有在当前Executor不是Driver的时候才为true
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port, conf,
      securityManager, clientMode = !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    // In the non-driver case, the RPC env's address may be null since it may not be listening
    // for incoming connections.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
      logInfo(s"Setting spark.executor.port to: ${rpcEnv.address.port.toString}")
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    // 创建序列化管理器
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    // 默认为org.apache.spark.serializer.JavaSerializer，通过spark.serializer配置
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    // 实际类型为org.apache.spark.serializer.JavaSerializer
    val closureSerializer = new JavaSerializer(conf)

    // 注册RpcEndpoint或者查找RpcEndpoint
    def registerOrLookupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef = {
      if (isDriver) { // 如果是Driver
        logInfo("Registering " + name)
        // 使用RpcEnv的方法进行注册
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else { // 如果是Executor
        // 向远端Driver的NettyRpcEnv询问获取相关RpcEndpoint的RpcEndpointRef
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    /**
      * 创建广播管理器。
      * 用于将配置信息和序列化后的RDD、Job及ShuffleDependency等信息在本地存储。
      * 如果为了容灾，也会复制到其他节点上。
      */
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    // 创建Map任务输出跟踪器
    val mapOutputTracker = if (isDriver) { // 如果是Driver，创建MapOutputTrackerMaster对象
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else { // 如果是Executor，创建MapOutputTrackerWorker对象
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    /**
      * 如果当前应用程序是Driver，则创建MapOutputTrackerMasterEndpoint，
      * 并且注册到Dispatcher中，注册名为MapOutputTracker。
      * 如果当前应用程序是Executor，则从远端Driver实例的NettyRpcEnv的Dispatcher中
      * 查找MapOutputTrackerMasterEndpoint的引用。
      */
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    // sort和tungsten-sort两种ShuffleManager的实现了都是SortShuffleManager
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    // 由参数spark.shuffle.manager来指定ShuffleManager，默认是SortShuffleManager
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)

    // 初始化ShuffleManager，利用反射机制创建
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    // 是否使用旧的StaticMemoryManager，默认为fasle
    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        // StaticMemoryManager，静态内存管理器，每个内存区域的占比是固定的，是早期版本遗留的MemoryManager
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        // UnifiedMemoryManager，统一内存管理器，动态管理，每个内存区域的占比可以互相挤压调整
        UnifiedMemoryManager(conf, numUsableCores)
      }

    // 块传输服务BlockTransferService对外提供的端口号
    val blockManagerPort = if (isDriver) { // Driver
      conf.get(DRIVER_BLOCK_MANAGER_PORT) // spark.driver.blockManager.port
    } else { // Executor
      conf.get(BLOCK_MANAGER_PORT) // spark.blockManager.port
    }

    // 创建块传输服务
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores)

    /**
      * 创建BlockManagerMaster，Driver和Executor都会持有一个BlockManagerMaster，
      * 因此这个方法对不同的角色有不同的实现，会查找或注册BlockManagerMasterEndpoint：
      *   - 当前应用程序是Driver，则创建BlockManagerMasterEndpoint，并且注册到Dispatcher中，注册名为BlockManagerMaster。
      *   - 当前应用程序是Executor，则从远端Driver实例的NettyRpcEnv的Dispatcher中查找BlockManagerMasterEndpoint的引用。
      */
    val blockManagerMaster = new BlockManagerMaster(
      // 注册或查找RpcEndpoint
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_ENDPOINT_NAME, // BlockManagerMaster
        new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    // 创建BlockManager
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)

    // 创建度量系统
    val metricsSystem = if (isDriver) { // 当前为Driver
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      // 仅创建，等待SparkContext中的任务调度器TaskScheculer告诉度量系统应用程序ID后再启动。
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else { // 当前为Executor
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      // 设置Executor的ID
      conf.set("spark.executor.id", executorId)
      // 创建MetricsSystem并直接启动
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // 创建输出提交协调器
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    /**
      * 如果当前实例是Driver，则创建OutputCommitCoordinatorEndpoint，并且注册到Dispatcher中，注册名为OutputCommitCoordinator。
      * 如果当前应用程序是Executor，则从远端Driver实例的NettyRpcEnv的Dispatcher中查找OutputCommitCoordinatorEndpoint的引用。
      */
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))

    // 将outputCommitCoordinatorRef关联到OutputCommitCoordinator实例上
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    // 创建SparkEnv对象
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) { // 如果是Driver，将创建Driver的临时目录
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      // 使用driverTmpDir字段进行记录
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
