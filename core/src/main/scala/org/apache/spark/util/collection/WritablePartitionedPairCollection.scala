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

import org.apache.spark.storage.DiskBlockObjectWriter

/**
 * A common interface for size-tracking collections of key-value pairs that
 *
 *  - Have an associated partition for each key-value pair.
 *  - Support a memory-efficient sorted iterator
 *  - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 *
 * 对由键值对构成的集合进行大小跟踪的通用接口。
 * 这里的每个键值对都有相关联的分区，例如：
 * key为（0，#），value为1的键值对，真正的键实际是#，而0则是键#的分区ID。
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   *
   * 将键值对与相关联的分区插入到集合中。
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   *
   * 根据给定的对key进行比较的比较器，返回对集合中的数据按照分区ID的顺序进行迭代的迭代器。此方法需要子类实现。
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
  : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   *
   * 迭代每个元素，将每个元素通过DiskBlockObjectWriter写出到磁盘。
   * 迭代的每个元素都是按照分区ID进行排序的。这个操作可能会破坏底层集合。
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
  : WritablePartitionedIterator = {
    // 获得对集合中的数据按照分区ID的顺序进行迭代的迭代器，该方法由子类实现
    val it = partitionedDestructiveSortedIterator(keyComparator)
    // 创建并返回WritablePartitionedIterator的匿名实现类的实例
    new WritablePartitionedIterator {
      private[this] var cur = if (it.hasNext) it.next() else null

      // 使用DiskBlockObjectWriter将键值对写入磁盘
      def writeNext(writer: DiskBlockObjectWriter): Unit = {
        writer.write(cur._1._2, cur._2)
        cur = if (it.hasNext) it.next() else null
      }

      // 用于判断是否还有下一个元素
      def hasNext(): Boolean = cur != null

      // 用于获取下一个元素的分区的ID
      def nextPartition(): Int = cur._1._1
    }
  }
}

private[spark] object WritablePartitionedPairCollection {
  /**
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   *
   * 生成对由partition id和key构成的两个对偶对象按照partition id进行排序的比较器。
   */
  def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    // 对由partition id和key构成的两个对偶对象按照partition id进行排序
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      // 根据partition id进行比较
      a._1 - b._1
    }
  }

  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   *
   * 生成对由partition id和key构成的两个对偶对象先按照partition id进行比较，
   * 再根据指定的键比较器按照key进行第二级比较的比较器。
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        // 对partition id和key构成的两个对偶对象按照partition id比较
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) { // 第一级比较已经区分出了胜负
          // 返回比较结果
          partitionDiff
        } else { // 第一级比较没有区分出胜负
          // 再根据指定的键比较器按照key进行第二级比较
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
 *
 * 将集合内容按照字节写入磁盘的WritablePartitionedIterator。
 */
private[spark] trait WritablePartitionedIterator {
  def writeNext(writer: DiskBlockObjectWriter): Unit

  def hasNext(): Boolean

  def nextPartition(): Int
}
