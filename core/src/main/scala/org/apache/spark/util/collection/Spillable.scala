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

package org.apache.spark.util.collection

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager) with Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  protected def forceSpill(): Boolean

  // Number of elements read from input since last spill
  // 用于读取_elementsRead的值
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  // 用于将_elements Read加一
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  // 对集合的内存使用进行跟踪的初始内存阈值。
  // 可通过spark.shuffle.spill.initialMemoryThreshold属性配置，默认为5MB。
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  /**
    * 当集合中有太多元素时，强制将集合中的数据溢出到磁盘的阈值。
    * 可通过spark.shuffle.spill.numElementsForceSpillThreshold属性配置，
    * 默认为Long.MAX_VALUE。
    */
  private[this] val numElementsForceSpillThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MaxValue)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  // 对集合的内存使用进行跟踪的初始内存阈值。初始值等于initialMemoryThreshold。
  @volatile private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  // 已经插入到集合的元素数量
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  // 内存中的数据已经溢出到磁盘的字节总数。
  @volatile private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  // 集合产生溢出的次数。
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
    * 用于将PartitionedAppendOnlyMap或PartitionedPairBuffer底层的数据溢出到磁盘
    *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 如果当前集合已经读取的元素数量是32的倍数，且集合当前的内存大小大于等于myMemoryThreshold
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      // 计算尝试获取的内存大小
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      // 为当前任务尝试获取期望大小的内存，得到实际获得的大小
      val granted = acquireMemory(amountToRequest)
      // 更新已经获得的内存大小
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      // 判断是否需要溢出
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    // 如果_elementsRead大于numElementsForceSpillThreshold也代表需要溢出
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) { // 如果应该进行溢出
      _spillCount += 1
      logSpillage(currentMemory)
      // 将集合中的数据溢出到磁盘
      spill(collection)
      // 已读取的元素计数归零
      _elementsRead = 0
      // 更新已经溢出的内存大小
      _memoryBytesSpilled += currentMemory
      // 释放ExternalSorter占用的内存
      releaseMemory()
    }
    // 返回是否进行了溢出
    shouldSpill
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val isSpilled = forceSpill()
      if (!isSpilled) {
        0L
      } else {
        val freeMemory = myMemoryThreshold - initialMemoryThreshold
        _memoryBytesSpilled += freeMemory
        releaseMemory()
        freeMemory
      }
    } else {
      0L
    }
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    freeMemory(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
