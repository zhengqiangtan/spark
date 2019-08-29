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

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 *
 * 可以在内存中对任务执行结果进行聚合运算。
 * 最大可以支持375809638（即0.7 × 2^29）个元素。
 *
 * @param initialCapacity 初始容量值。如果未指定，默认为64
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // 用于计算data数组容量增长的阈值的负载因子。固定为0.7。
  private val LOAD_FACTOR = 0.7

  /**
   * data数组的当前容量。
   * capacity的初始值的计算方法为取initialCapacity的二进制位的最高位，其余位补0得到新的整数（记为highBit）。
   * 如果highBit与initialCapacity相等，则capacity等于initialCapacity，
   * 否则将highBit左移一位后作为capacity的值。
   */
  private var capacity = nextPowerOf2(initialCapacity)
  // 计算数据存放位置的掩码。计算mask的表达式为capacity – 1
  private var mask = capacity - 1
  // 记录当前已经放入data的key与聚合值的数量
  private var curSize = 0
  // data数组容量增长的阈值。
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  /**
   * 用于保存key和聚合值的数组。初始大小为2 * capacity，
   * data数组的实际大小之所以是capacity的2倍，是因为key和聚合值各占一位。
   */
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  // data数组中是否已经有了null值
  private var haveNullValue = false
  // 空值，用表示键为null对应的值
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  // 表示data数组是否不再使用
  private var destroyed = false
  // 当destroyed为true时，打印的消息内容为"Map state is invalid from destructive sorting!"。
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** Get the value for a given key */
  def apply(key: K): V = {
    // 检查data数组是否可用
    assert(!destroyed, destructionMessage)
    // 转换key类型并检查是否为空
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      // key为空则返回nullValue，null键只有nullValue这一个值
      return nullValue
    }
    // 根据key的哈希值获取偏移量
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      // 获取data中对应偏移量的key，作为curKey
      val curKey = data(2 * pos)
      // 判断curKey与key是否相同
      if (k.eq(curKey) || k.equals(curKey)) {
        // 相同，说明找到了对应的value，取出返回即可
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) { // 获取的curKey为null
        // 返回null
        return null.asInstanceOf[V]
      } else { // 不相同
        // 更新pos再次查找
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    // 没有找到，返回null
    null.asInstanceOf[V]
  }

  /** Set the value for a key
   * 实现了将key对应的值更新到data数组中
   */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) { // 对key是null值的更新处理
      if (!haveNullValue) { // 当前data中还没有null值
        // 扩充容量
        incrementSize()
      }
      // 将nullValue设置为传入的value
      nullValue = value
      // 标记当前data数组中已经有了null值
      haveNullValue = true
      return
    }

    // 根据key的哈希值与掩码计算元素放入data数组的索引位置pos
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) { // 将key放入data数组
      // 获取2 * pos位置的key
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        // curKey为null，说明data数组的2*pos的索引位置还没有放置元素，k是首次聚合到data数组中
        // 先将k放到data(2*pos)位置
        data(2 * pos) = k
        // 将value放到data(2*pos+1)的位置
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        // 扩充AppendOnlyMap的容量后返回
        incrementSize() // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 如果curKey不等于null并且等于k，说明data数组的2*pos的索引位置已经放置了元素且元素就是k
        // 将value更新到data(2*pos+1)的位置后返回。
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        // 如果curKey不等于null并且不等于k，说明data数组的2*pos的索引位置已经放置了元素，但元素不是k
        // 从data数组的pos位置向后找，计算新位置重新进行判断
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   *
   * @param key        待聚合的Key
   * @param updateFunc 聚合函数。接收两个参数，分别是Boolean类型和泛型类型V。
   *                   Boolean类型的参数表示key是否已经添加到AppendOnlyMap的data数组中进行过聚合。
   *                   V则表示key曾经添加到AppendOnlyMap的data数组进行聚合时生成的聚合值，
   *                   新一轮的聚合将在之前的聚合值上累积。
   * @return
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) { // 对key是null值的缓存集合
      if (!haveNullValue) { // data数组中还没有null值
        // 进行扩容
        incrementSize()
      }
      // 对nullValue进行聚合
      nullValue = updateFunc(haveNullValue, nullValue)
      // 标记当前data数组中已经有了null值
      haveNullValue = true
      return nullValue
    }

    // 根据key的哈希值与掩码计算元素放入data数组的索引位置pos
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) { // 将key放入data数组中，并进行聚合
      // 获取data(2*pos)位置的当前key
      val curKey = data(2 * pos)
      if (curKey.eq(null)) { // curKey为null
        // 说明data数组的2*pos的索引位置还没有放置元素，k是首次聚合到data数组中
        // 调用updateFunc函数时指定的Boolean类型参数值为false且没有曾经的聚合值（即V是null）
        val newValue = updateFunc(false, null.asInstanceOf[V])
        // 将k放到data(2*pos)位置
        data(2 * pos) = k
        // 获得聚合值newValue放到data(2*pos+1)的位置
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        // 扩充AppendOnlyMap的容量
        incrementSize()
        // 后返回newValue
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) { // 如果curKey不等于null并且等于k
        // 说明data数组的2*pos的索引位置已经放置了元素，且元素就是k
        // 调用updateFunc函数时指定的Boolean类型参数值为true，且曾经的聚合值就是data(2*pos+1)的元素
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        // 将获得的聚合值newValue更新到data(2*pos+1)的位置
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        // 返回newValue
        return newValue
      } else { // curKey不等于null并且不等于k，说明data数组的2*pos的索引位置已经放置了元素，但元素不是k
        // 从data数组的pos位置向后找，计算新位置重新进行判断
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable
   * 获取迭代器
   */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    // 返回匿名的迭代器
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        // 第一次迭代，如果有null键，则首先发挥null键及其对应的值
        if (pos == -1) { // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          // 自增偏移量
          pos += 1
        }
        // 循环获取键值对
        while (pos < capacity) {
          // 键不为null
          if (!data(2 * pos).eq(null)) {
            // 返回键值对组成的元组
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        // 获取下一个键值对
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary
   * 用于扩充AppendOnlyMap的容量
   */
  private def incrementSize() {
    // 自增curSize
    curSize += 1
    if (curSize > growThreshold) {
      // 调用该方法进行扩容，扩大一倍
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /** Double the table's size and re-hash everything */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    // 扩容的新容量是当前容量的2倍
    val newCapacity = capacity * 2
    // 新容量不可超过MAXIMUM_CAPACITY，即1 << 29
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")

    // 创建一个两倍于当前容量的数组
    val newData = new Array[AnyRef](2 * newCapacity)
    // 计算新数组的掩码
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0

    // 将老数组中的元素拷贝到新数组的指定索引位置
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        // 取得原位置上的键和值
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        // 计算存放对应键的新索引位置，使用新的mask掩码进行rehash计算
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          // 检查新索引上是否存在键
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) { // 对应新索引位置上的键为null
            // 更新键和值
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            // 结束循环
            keepGoing = false
          } else { // 新索引位上存在不为null的键，则往后找位置
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }

    // 将新数组作为扩充容量后的data数组
    data = newData
    // 将新数组的容量大小改为data数组的容量大小
    capacity = newCapacity
    // 将掩码修改为新计算的掩码
    mask = newMask
    // 重新计算AppendOnlyMap的容量增长阈值
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    // 获取n的中最高的一位1所代表的数字
    val highBit = Integer.highestOneBit(n)
    // 如果与n相同，则取n，否则左移1位，即乘以2
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   *
   * 提供了一种在不使用额外的内存和不牺牲AppendOnlyMap的有效性的前提下，
   * 对AppendOnlyMap的data数组中的数据进行排序的实现。
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0
    /**
     * 将data数组中的元素向前（即向着索引为0的方向）整理排列
     * 该操作会将键值对尽量存放在data数组的前部分空间
     */
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

    /**
     * 使用指定的比较器执行比较、并且进行排序。
     * 这其中用到了TimSort，也就是优化版的归并排序。
     * 最后得到的data数组中的键值对按照索引自增的顺序是有序的。
     */
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    new Iterator[(K, V)] { // 生成迭代访问data数组中的迭代器
      var i = 0
      var nullValueReady = haveNullValue

      def hasNext: Boolean = (i < newIndex || nullValueReady)

      def next(): (K, V) = {
        if (nullValueReady) { // 首先返回键为null的键值对
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          // 然后迭代返回剩余的元素
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  // data数组的容量不能超过该值，以防止data数组溢出
  val MAXIMUM_CAPACITY = (1 << 29)
}
