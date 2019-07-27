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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 *
 * 继承自MemoryLocation，代表从obj和offset定位的起始位置开始，固定长度（由MemoryBlock的length属性确定）的连续内存块。
 */
public class MemoryBlock extends MemoryLocation {

  // 当前MemoryBlock的连续内存块的长度
  private final long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field is public so that it can be modified by the TaskMemoryManager,
   * which lives in a different package.
   *
   * 当前MemoryBlock的页号。TaskMemoryManager分配由MemoryBlock表示的Page时，将使用此属性。
   */
  public int pageNumber = -1;

  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   *
   * MemoryBlock的大小，即length
   */
  public long size() {
    return length;
  }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   *
   * 创建一个指向由长整型数组使用的内存的MemoryBlock
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
  }

  /**
   * Fills the memory block with the specified byte value.
   *
   * 以指定的字节填充整个MemoryBlock，
   * 即将obj对象从offset开始，长度为length的堆内存替换为指定字节的值。
   * Platform中封装了对sun.misc.Unsafe的API调用，
   * Platform的setMemory方法实际调用了sun.misc.Unsafe的setMemory方法。
   */
  public void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }
}
