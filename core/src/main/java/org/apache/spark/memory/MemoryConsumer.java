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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  // MemoryConsumer要消费的Page的大小
  private final long pageSize;
  // 内存模式（MemoryMode）。MemoryConsumer中提供了getMode()方法获取mode。
  private final MemoryMode mode;
  // 当前消费者已经使用的执行内存的大小。MemoryConsumer中提供了getUsed()方法获取used。
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
  }

  /**
   * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * Returns the size of used memory in bytes.
   */
  protected long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   *
   * For testing.
   *
   * 模板方法，用于调用子类实现的spill方法将数据溢出到磁盘。
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * Note: today, this only frees Tungsten-managed pages.
   *
   * 当TaskAttempt没有足够的内存可用时，TaskMemoryManager将调用此方法把一些数据溢出到磁盘，以释放内存。
   *
   * 抽象方法，需要子类实现。
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`.
   *
   * 用于分配指定大小的长整型数组。
   */
  public LongArray allocateArray(long size) {
    // 计算所需的Page大小。由于长整型占用8个字节，所以需要乘以8。
    long required = size * 8L;
    // 分配指定大小的MemoryBlock
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    // 分配得到的MemoryBlock的大小小于所需的大小
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        // 释放MemoryBlock
        taskMemoryManager.freePage(page, this);
      }
      // 打印内存使用信息并抛出OutOf MemoryError。
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    // 将required累加到used，即更新已经使用的内存大小
    used += required;
    /**
     * 创建并返回LongArray。
     * 调用sun.misc.Unsafe的putLong(object,offset,value)和
     * getLong(object,offset)两个方法来模拟实现长整型数组。
     */
    return new LongArray(page);
  }

  /**
   * Frees a LongArray.
   *
   * 用于释放长整型数组。
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * Throws IOException if there is not enough memory.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        taskMemoryManager.freePage(page, this);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    // 首先更新used
    used -= page.size();
    // 释放MemoryBlock
    taskMemoryManager.freePage(page, this);
  }

  /**
   * Allocates memory of `size`.
   *
   * 用于获得指定大小（单位为字节）的内存。
   */
  public long acquireMemory(long size) {
    // 调用TaskMemoryManager的acquireExecution Memory方法获取指定大小的内存
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    // 更新used
    used += granted;
    // 最后返回实际获得的内存大小
    return granted;
  }

  /**
   * Release N bytes of memory.
   *
   * 用于释放指定大小（单位为字节）的内存。
   */
  public void freeMemory(long size) {
    // 调用TaskMemoryManager的releaseExecutionMemory()方法释放指定大小的内存
    taskMemoryManager.releaseExecutionMemory(size, this);
    // 更新used
    used -= size;
  }
}
