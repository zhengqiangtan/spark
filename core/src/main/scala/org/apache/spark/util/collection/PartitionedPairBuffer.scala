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

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 *
 * The buffer can support up to `1073741823 (2 ^ 30 - 1)` elements.
 *
 * 将键值对缓存在内存中，并支持对元素进行排序的数据结构
 *
 * @param initialCapacity 初始容量值。如果未指定，默认为64
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  // 检查初始容量是否超限，2 ^ 30 - 1
  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  // 同时初始容量要大于或等于1
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  // data数组的当前容量。capacity的初始值等于initialCapacity。
  private var capacity = initialCapacity
  // 记录当前已经放入data的key与value的数量。
  private var curSize = 0
  // 用于保存key和value的数组。
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer
    * 用于将key的分区ID、key及value添加到PartitionedPairBuffer底层的data数组中。
    **/
  def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) { // 如果底层data数组已经满了，则对其进行扩容
      growArray()
    }
    // 从存放方式可以看出，它是顺序进行存放，不考虑键重复的问题
    // 将key及其分区ID作为元组放入data数组
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    // 将value放入data数组
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    // 增加已经放入data数组的key与value的数量
    curSize += 1
    // 对集合大小进行采样
    afterUpdate()
  }

  /** Double the size of the array because we've reached capacity */
  private def growArray(): Unit = {
    // 防止PartitionedPairBuffer的容量超过MAXIMUM_CAPACITY的限制
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }

    // 计算对PartitionedPairBuffer进行扩充后的容量大小
    val newCapacity =
      // 扩容的容量不能超过MAXIMUM_CAPACITY
      if (capacity * 2 < 0 || capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }

    // 创建一个两倍于新容量的大小的新数组
    val newArray = new Array[AnyRef](2 * newCapacity)
    // 将底层data数组的每个元素都拷贝到新数组中
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    // 将新数组设置为底层的data数组
    data = newArray
    // 将PartitionedPairBuffer的当前容量设置为新的容量大小
    capacity = newCapacity
    // 使用SizeTracker的resetSamples()方法对样本进行重置，以便估算准确
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive.
   * 根据给定的对key进行比较的比较器，返回对集合中的数据按照分区ID的顺序进行迭代的迭代器
   */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 生成比较器，如果没有指定，则使用partitionComparator生成比较器
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    // 执行内置排序。这其中用到了TimSort，也就是优化版的归并排序。
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    // 获得对data中的数据进行迭代的迭代器
    iterator
  }

  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      // 依次返回data数组中的键值对
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  // 值为2^30 - 1。data数组的容量不能超过MAXIMUM_CAPACITY，以防止data数组溢出。
  val MAXIMUM_CAPACITY = Int.MaxValue / 2 // 2 ^ 30 - 1
}
