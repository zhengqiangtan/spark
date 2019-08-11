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

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    maxOnHeapExecutionMemory: Long,
    override val maxOnHeapStorageMemory: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    maxOnHeapStorageMemory,
    maxOnHeapExecutionMemory) {

  /**
    * 重载构造器，只需要SparkConf和numCores两个参数；
    * maxOnHeapStorageMemory和maxOnHeapExecutionMemory将
    * 使用getMaxExecutionMemory()和getMaxStorageMemory()方法来获取。
    */
  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  // The StaticMemoryManager does not support off-heap storage memory:
  offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize)
  offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize)

  // Max number of bytes worth of blocks to evict when unrolling
  private val maxUnrollMemory: Long = {
    (maxOnHeapStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  override def maxOffHeapStorageMemory: Long = 0L

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    // StaticMemoryManager不支持将堆外内存用于存储，需要检查内存模式
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap storage memory")

    if (numBytes > maxOnHeapStorageMemory) { // 申请的内存大小大于最大的堆内存储内存大小
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxOnHeapStorageMemory bytes)")
      // 直接返回false
      false
    } else {
      // 交给onHeapStorageMemoryPool内存池处理
      onHeapStorageMemoryPool.acquireMemory(blockId, numBytes)
    }
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    // StaticMemoryManager不支持将堆外内存用于存储，需要检查内存模式
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap unroll memory")
    // 获取当前可用于Unroll的存储内存大小
    val currentUnrollMemory = onHeapStorageMemoryPool.memoryStore.currentUnrollMemory
    // 当前空闲的存储内存大小
    val freeMemory = onHeapStorageMemoryPool.memoryFree
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    // 不足的内存大小 = 最大可用于Unroll的存储内存大小 - 当前可用于Unroll的存储内存大小 - 当前空闲的存储内存大小
    val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    // Keep it within the range 0 <= X <= maxNumBytesToFree
    // 计算需要释放的内存大小
    val numBytesToFree = math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))

    /**
      * 委托给onHeapStorageMemoryPool进行处理，注意第3个参数
      * acquireMemory()方法内会尝试腾出额外的空间以满足不足的内存
      */
    onHeapStorageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree)
  }

  private[memory] override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    memoryMode match {// 委托给具体的内存池处理
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }
}


private[spark] object StaticMemoryManager {

  private val MIN_MEMORY_BYTES = 32 * 1024 * 1024

  /**
   * Return the total amount of memory available for the storage region, in bytes.
    * 获取最大的存储内存大小
   */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    // 系统可用的最大内存，通过spark.testing.memory配置，未配置的话则取运行时环境的最大内存
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    // 存储内存占比，默认为0.6，即存储内存占内存池总大小的60%
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    // 存储内存的最大安全系数，默认为0.9，该值用于防止存储内存溢出
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)

    // 因此，存储内存的最大大小为 可用的最大内存 * 0.6 * 0.9，即可用最大内存的54%
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Return the total amount of memory available for the execution region, in bytes.
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    // 系统可用的最大内存，通过spark.testing.memory配置，未配置的话则取运行时环境的最大内存
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    // 判断可用最大内存是否小于32MB，如果小于32MB则抛出异常
    if (systemMaxMemory < MIN_MEMORY_BYTES) {
      throw new IllegalArgumentException(s"System memory $systemMaxMemory must " +
        s"be at least $MIN_MEMORY_BYTES. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }

    // 判断是否有spark.executor.memory配置
    if (conf.contains("spark.executor.memory")) {
      // 获取spark.executor.memory配置，即Execution使用的内存大小
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      // 判断Execution使用的内存大小是否小于32MB，如果小于32MB则抛出异常
      if (executorMemory < MIN_MEMORY_BYTES) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$MIN_MEMORY_BYTES. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }

    // 执行内存占比，默认为0.2，即存储内存占内存池总大小的20%
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    // 执行内存的最大安全系数，默认为0.8，该值用于防止执行内存溢出
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    // 因此，执行内存的最大大小为 可用的最大内存 * 0.2 * 0.8，即可用最大内存的16%
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
