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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 *
 * Tungsten在堆内存模式下使用的内存分配器，与onHeapExecution MemoryPool配合使用。
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  // 关于MemoryBlock的弱引用[1]的缓冲池，用于Page页（即Memory Block）的分配。
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   *
   * 用于判断对于指定大小的MemoryBlock，
   * 是否需要采用池化机制（即从缓冲池bufferPoolsBySize中存取MemoryBlock）。
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    // 当要分配的内存大小大于等于1MB时，需要从bufferPoolsBySize中获取MemoryBlock。
    return size >= POOLING_THRESHOLD_BYTES; // 1MB
  }

  // 用于分配指定大小（size）的MemoryBlock
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    if (shouldPool(size)) { // 指定大小（size）的MemoryBlock需要采用池化机制
      synchronized (this) {
        // 从bufferPoolsBySize的弱引用中获取指定大小的MemoryBlock
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) {
              assert (memory.size() == size);
              // 从MemoryBlock的缓存中获取指定大小的MemoryBlock
              return memory;
            }
          }
          // 没有指定大小的MemoryBlock，移除指定大小的MemoryBlock缓存
          bufferPoolsBySize.remove(size);
        }
      }
    }

    // 指定大小的MemoryBlock不需要采用池化机制或者bufferPoolsBySize中没有指定大小的MemoryBlock
    long[] array = new long[(int) ((size + 7) / 8)];
    // 创建MemoryBlock并返回
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    // 返回创建的MemoryBlock
    return memory;
  }

  // 用于释放MemoryBlock
  @Override
  public void free(MemoryBlock memory) {
    // 获取待释放MemoryBlock的大小
    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }
    if (shouldPool(size)) { // MemoryBlock的大小需要采用池化机制
      // 将MemoryBlock的弱引用放入bufferPoolsBySize中
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(size, pool);
        }
        // 将MemoryBlock的弱引用放入bufferPoolsBySize中
        pool.add(new WeakReference<>(memory));
      }
    } else {
      // Do nothing
    }
  }
}
