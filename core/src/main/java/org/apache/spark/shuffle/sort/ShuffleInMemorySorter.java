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

package org.apache.spark.shuffle.sort;

import java.util.Comparator;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

  final class ShuffleInMemorySorter {

    /**
     * 比较器，比较的对象是PackedRecordPointer
     * PackedRecordPointer中记录了分区号，页号及偏移量
     */
    private static final class SortComparator implements Comparator<PackedRecordPointer> {
      @Override
      public int compare(PackedRecordPointer left, PackedRecordPointer right) {
        // 获取两个分区的ID
        int leftId = left.getPartitionId();
        int rightId = right.getPartitionId();
        // 使用分区ID进行比较
        return leftId < rightId ? -1 : (leftId > rightId ? 1 : 0);
      }
    }

    // 默认使用SortComparator比较器
    private static final SortComparator SORT_COMPARATOR = new SortComparator();

    // 内存消费者，用于申请和释放内存
    private final MemoryConsumer consumer;

    /**
     * An array of record pointers and partition ids that have been encoded by
     * {@link PackedRecordPointer}. The sort operates on this array instead of directly manipulating
     * records.
     *
     * Only part of the array will be used to store the pointers, the rest part is preserved as
     * temporary buffer for sorting.
     *
     * 申请的内存的主要表现方式；排序器将操作该对象，代替直接操作记录
     */
    private LongArray array;

    /**
     * Whether to use radix sort for sorting in-memory partition ids. Radix sort is much faster
     * but requires additional memory to be reserved memory as pointers are added.
     *
     * 是否使用基数排序；基数排序比较快，但需要额外的内存。
     */
    private final boolean useRadixSort;

    /**
     * The position in the pointer array where new records can be inserted.
     */
    private int pos = 0;

    /**
     * How many records could be inserted, because part of the array should be left for sorting.
     */
    private int usableCapacity = 0;

    // 初始的排序缓冲大小
    private int initialSize;

    ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
      this.consumer = consumer;
      assert (initialSize > 0);
      this.initialSize = initialSize;
      this.useRadixSort = useRadixSort;
      // 直接申请初始大小的内存
      this.array = consumer.allocateArray(initialSize);
      // 初始化可用内存容量，当使用基数排序时为申请内存的1/2，否则为申请内存的1/1.5
      this.usableCapacity = getUsableCapacity();
    }

  // 计算可用容量
  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    // 可见基数排序更耗内存
    return (int) (array.size() / (useRadixSort ? 2 : 1.5));
  }

  // 释放内存
  public void free() {
    if (array != null) {
      // 通过consumer的freeArray()方法释放
      consumer.freeArray(array);
      // 将array置为null
      array = null;
    }
  }

  // 获取记录数量
  public int numRecords() {
    return pos;
  }

  // 重置排序器
  public void reset() {
    // 释放内存，重新申请初始内存并计算可用内存
    if (consumer != null) {
      consumer.freeArray(array);
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
    }
    // 将记录条数计数器置为0，表示没有记录
    pos = 0;
  }

  /**
   * 对内存进行扩容
   *
   * @param newArray 扩容后的新的LongArray对象
   */
  public void expandPointerArray(LongArray newArray) {
    // 检查，扩容后的内存大小应该大于现有内存大小
    assert(newArray.size() > array.size());
    // 将当前array中的数据拷贝到newArray
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      pos * 8L
    );
    // 释放当前array占用的内存
    consumer.freeArray(array);
    // 将新的newArray替换旧array
    array = newArray;
    // 重新计算可用容量
    usableCapacity = getUsableCapacity();
  }

  // 判断是否还有剩余空间
  public boolean hasSpaceForAnotherRecord() {
    return pos < usableCapacity;
  }

  // 判断已使用的内存
  public long getMemoryUsage() {
    return array.size() * 8;
  }

  /**
   * Inserts a record to be sorted.
   * 插入一条已经排序的记录
   *
   * @param recordPointer a pointer to the record, encoded by the task memory manager. Due to
   *                      certain pointer compression techniques used by the sorter, the sort can
   *                      only operate on pointers that point to locations in the first
   *                      {@link PackedRecordPointer#MAXIMUM_PAGE_SIZE_BYTES} bytes of a data page.
   *                      由TaskMemoryManager编码的页号和偏移量，需要与partitionId一起编码为新的指针记录，
   *                      即[24 bit 分区号][13 bit 内存页号][27 bit 内存偏移量]
   * @param partitionId the partition id, which must be less than or equal to
   *                    {@link PackedRecordPointer#MAXIMUM_PARTITION_ID}.
   *                    分区号
   */
  public void insertRecord(long recordPointer, int partitionId) {
    // 检查是否还有空闲空间
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    // 将转换后得到的PackedRecordPointer设置到array的pos位置
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   *
   * 索引记录的迭代器
   */
  public static final class ShuffleSorterIterator {

    // LongArray内部封装了MemoryBlock对象
    private final LongArray pointerArray;
    private final int limit;
    // 记录了分区号，页号及偏移量的记录指针
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    /**
     * 构建迭代器
     * @param numRecords 记录条数
     * @param pointerArray 用于记录数据的LongArray
     * @param startingPosition 迭代的起始记录的位置索引
     */
    ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
      this.limit = numRecords + startingPosition;
      this.pointerArray = pointerArray;
      this.position = startingPosition;
    }

    public boolean hasNext() {
      // postition小于limit时表示还有数据
      return position < limit;
    }

    public void loadNext() {
      // 从pointerArray中获取指定位置的long型整数设置到packedRecordPointer
      packedRecordPointer.set(pointerArray.get(position));
      // position
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   * 获取ShuffleSorterIterator迭代器
   */
  public ShuffleSorterIterator getSortedIterator() {
    int offset = 0;
    // 判断是否使用基数排序
    if (useRadixSort) {
      // 基数排序，按照分区号升序排序
      offset = RadixSort.sort(
        array, pos,
        PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
        PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
    } else {
      // 普通TimSort排序
      MemoryBlock unused = new MemoryBlock(
        array.getBaseObject(),
        array.getBaseOffset() + pos * 8L,
        (array.size() - pos) * 8L);
      LongArray buffer = new LongArray(unused);
      Sorter<PackedRecordPointer, LongArray> sorter =
        new Sorter<>(new ShuffleSortDataFormat(buffer));

      // 使用SORT_COMPARATOR，即SortComparator比较器
      sorter.sort(array, 0, pos, SORT_COMPARATOR);
    }
    // 构造为ShuffleSorterIterator后返回
    return new ShuffleSorterIterator(pos, array, offset);
  }
}
