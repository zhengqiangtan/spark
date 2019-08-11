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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
  *
  * 内存管理器。负责对单个节点上内存的分配与回收。有两种：
  * - StaticMemoryManager：静态内存管理器。
  * - UnifiedMemoryManager：统一内存管理器。
  * @param numCores CPU内核数，该值会影响计算的内存页大小
  * @param onHeapStorageMemory 用于存储的堆内存大小。
  * @param onHeapExecutionMemory 用于执行计算的堆内存大小。
  */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  // 用于存储的堆内存的内存池（StorageMemoryPool），大小由onHeapStorageMemory属性指定。
  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  // 用于存储的堆外内存的内存池（StorageMemoryPool）。
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  // 用于计算的堆内存的内存池（ExecutionMemoryPool），大小由onHeapExecutionMemory属性指定。
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  // 用于计算的堆外内存的内存池（ExecutionMemoryPool）。
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  // 对堆内的执行内存池和存储内存池进行容量初始化
  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  // 堆外内存的最大值。可以通过spark.memory.offHeap.size属性指定，默认为0。
  protected[this] val maxOffHeapMemory = conf.getSizeAsBytes("spark.memory.offHeap.size", 0)
  /**
    * 用于存储的堆外内存初始大小。
    * 先通过spark.memory.storageFraction参数确定用于堆外内存中用于存储的占比；
    * 默认值为0.5，即表示offHeapStorageMemory和offHeapExecutionMemoryPool各占总堆外内存的50%；
    * 然后根据将这个比例与总的堆外内存大小相乘，即可得到用于存储的堆外内存初始大小。
    * 需要注意的是，由于StaticMemoryManager不可将堆外内存用于存储，因此该值对StaticMemoryManager无效。
    */
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong

  // 对堆外的执行内存池和存储内存池进行容量初始化
  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  // 需要注意的是，由于StaticMemoryManager不可将堆外内存用于存储，因此该值对StaticMemoryManager无效。
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
    *
    * 返回用于存储的最大堆内存。此方法需要子类实现。
   */
  def maxOnHeapStorageMemory: Long

  /**
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
    *
    * 返回用于存储的最大堆外内存。此方法需要子类实现。
   */
  def maxOffHeapStorageMemory: Long

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
    *
    * 给onHeapStorageMemoryPool和offHeapStorageMemoryPool设置MemoryStore。
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    // 调用了StorageMemoryPool的setMemoryStore()方法
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    *
    * 为存储BlockId对应的Block，从堆内存或堆外内存获取所需大小的内存。
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
    *
    * 为展开BlockId对应的Block，从堆内存或堆外内存获取所需大小的内存。
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
    *
    * 为执行taskAttemptId对应的TaskAttempt，从堆内存或堆外内存获取所需大小（即numBytes）的内存。
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * Release numBytes of execution memory belonging to the given task.
    *
    * 从堆内存或堆外内存释放taskAttemptId对应的TaskAttempt所消费的指定大小（即numBytes）的执行内存。
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
    *
    * 从堆内存及堆外内存释放taskAttemptId代表的TaskAttempt所消费的所有执行内存。
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    // 释放占有的堆内执行内存，并释放占用的堆外执行内存
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
    * 从堆内存或堆外内存释放指定大小的内存。
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match { // 根据内存位置的不同使用不同的内存池来操作
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * Release all storage memory acquired.
    * 从堆内存及堆外内存释放所有内存。
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    // 使用堆内存池和对外内存池的releaseAllMemory()方法实现
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
    * 释放指定大小的展开内存。
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * Execution memory currently in use, in bytes.
    *
    * 获取堆上执行内存池与堆外执行内存池已经使用的执行内存之和。
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
    * onHeapStorageMemoryPool与offHeapStorageMemoryPool中一共占用的存储内存。
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
    *
    * 获取taskAttemptId代表的TaskAttempt在堆上执行内存池与堆外执行内存池所消费的执行内存之和。
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
    *
    * Tungsten的内存模式。tungstenMemoryMode也采用枚举类型MemoryMode来表示堆内存和堆外内存。
    * 当Tungsten在堆内存模式下，数据存储在JVM堆上，这时Tungsten选择onHeapExecutionMemoryPool作为内存池。
    * 当Tungsten在堆外内存模式下，数据则会存储在堆外内存中，这时Tungsten选择offHeapExecutionMemoryPool作为内存池。
    * 可以通过spark.memory.offHeap.enabled属性（默认为false）来配置是否启用Tungsten的堆外内存。
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.getBoolean("spark.memory.offHeap.enabled", false)) {
      require(conf.getSizeAsBytes("spark.memory.offHeap.size", 0) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
    *
    * Tungsten采用的Page的默认大小（单位为字节）。
    * 可通过spark.buffer.pageSize属性进行配置。
    * 如果未指定spark.buffer.pageSize属性，则使用该方法进行计算。
   */
  val pageSizeBytes: Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    // 获取CPU核数，如果指定了numCores就使用numCores，否则使用机器的CPU可用核数
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    // 安全因子
    val safetyFactor = 16
    // 获取对应内存模式下可用的最大Tungsten内存
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    /**
      * 计算页大小。传入的参数是maxTungstenMemory / cores / safetyFactor，
      * 最终得到的页大小是小于maxTungstenMemory / cores / safetyFactor的最大的2的次方值。
      */
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    // 页的大小需要在 1MB ~ 64MB 之间
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    // 尝试从spark.buffer.pageSize参数获取，如果没有指定就使用上面计算的默认值
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
    *
    * Tungsten采用的内存分配器（MemoryAllocator）。
    * 如果tungstenMemoryMode为MemoryMode.ON_HEAP，
    * 那么tungstenMemoryAllocator为堆内存分配器（HeapMemoryAllocator），
    * 否则为使用sun.misc.Unsafe的API分配操作系统内存的分配器UnsafeMemoryAllocator。
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
