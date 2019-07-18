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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
  *
  * StorageMemoryPool是对用于存储的物理内存的逻辑抽象，通过对存储内存的逻辑管理，提高Spark存储体系对内存的使用效率。
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
  *                   内存模式；用于存储的内存池包括堆内存的内存池和堆外内存的内存池。
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  /**
    * 内存池的名称。
    * - 如果memoryMode是MemoryMode.ON_HEAP，则内存池名称为on-heap storage。
    * - 如果memoryMode是MemoryMode.OFF_HEAP，则内存池名称为off-heap storage。
    */
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  // 已经使用的内存大小（单位为字节）。
  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  // 返回了_memoryUsed属性的值
  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  // 当前StorageMemoryPool所关联的MemoryStore。
  private var _memoryStore: MemoryStore = _
  // 返回了_memoryStore属性引用的MemoryStore
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
    * 设置当前StorageMemoryPool所关联的MemoryStore，实际设置了_memoryStore属性。
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    *
    * 用于给BlockId对应的Block获取numBytes指定大小的内存。
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    // 计算申请内存和可用内存的差值，避免申请过量
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    // 调用重载方法分配
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the amount of space to be freed through evicting blocks
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId, // 需要分配内存的BlockId
      numBytesToAcquire: Long, // 需要分配的大小
      // 空闲大小
      numBytesToFree: Long): Boolean = lock.synchronized { // 加锁
    // 检查参数
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)
    if (numBytesToFree > 0) { // 腾出numBytesToFree属性指定大小的空间
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    // 判断内存是否充足
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) { // 增加已经使用的内存大小
      _memoryUsed += numBytesToAcquire
    }
    // 返回是否成功获得了用于存储Block的内存空间
    enoughMemory
  }

  // 释放内存
  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) {
      // 释放的大小大于已使用大小，则释放当前内存池的所有内存，即将_memoryUsed设置为0。
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      // 否则从已使用内存大小中减去释放的大小
      _memoryUsed -= size
    }
  }

  // 释放当前内存池的所有内存，即将_memoryUsed设置为0。
  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
    *
    * 用于释放指定大小的空间，缩小内存池的大小。
   *
   * @return number of bytes to be removed from the pool's capacity.
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    // 计算释放的大小和空闲大小的最小值
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    // 计算可释放的内存是否足够
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) { // 如果可释放的内存不够
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      // 使用evictBlocksToFreeSpace()方法尝试腾出一些内存
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      // 返回最终释放的大小
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      // 可释放的内存足够，直接返回释放的大小即可
      spaceFreedByReleasingUnusedMemory
    }
  }
}
