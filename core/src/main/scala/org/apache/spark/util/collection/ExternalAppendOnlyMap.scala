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

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 *
 * 仅支持添加操作的字典，该字典会在使用空间过大时对数据进行溢写。
 *
 * 对于溢写阈值的设置需要权衡两点：
 * 1. 如果溢写阈值设置过高，可能会由于字典占据过大内存导致OOM。
 * 2. 如果溢写阈值设置过低，可能会导致过于频繁的不必要的溢写，因此导致性能下降。
 *
 * @param createCombiner 值初始化函数
 * @param mergeValue 分区内值合并函数
 * @param mergeCombiners 分区间合并函数
 * @param serializer 序列化器
 * @param blockManager BlockManager
 * @param context
 * @param serializerManager 序列化管理器
 * @tparam K 键类型
 * @tparam V 值类型
 * @tparam C 聚合后的值类型
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get(),
    serializerManager: SerializerManager = SparkEnv.get.serializerManager)
  extends Spillable[SizeTracker](context.taskMemoryManager())
  with Serializable
  with Logging
  with Iterable[(K, C)] {

  // 检查TaskContxt上下文，不可为null
  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  // Backwards-compatibility constructor for binary compatibility
  def this(
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: Serializer,
      blockManager: BlockManager) {
    this(createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
  }

  // 当前用于存储数据的Map，底层其实还是使用了AppendOnlyMap
  @volatile private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  // 溢写的Map
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  // DiskBlockManager对象
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   *
   * 溢写过程中序列化器读写操作的批次大小
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  // 已经溢写的数据的字节总数
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  // 文件缓冲区大小
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics
  private val writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  // Peak size of the in-memory map observed so far, in bytes
  // 内存使用的峰值
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // 键比较器，默认为Hash比较器
  private val keyComparator = new HashComparator[K]
  // 序列化器
  private val ser = serializer.newInstance()

  @volatile private var readingIterator: SpillableIterator = null

  /**
   * Number of files this map has spilled so far.
   * Exposed for testing.
   *
   * 已经溢写的文件数量
   */
  private[collection] def numSpills: Int = spilledMaps.size

  /**
   * Insert the given key and value into the map.
   * 插入键值对数据
   */
  def insert(key: K, value: V): Unit = {
    // 调用insertAll()处理
    insertAll(Iterator((key, value)))
  }

  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   *
   * 插入数据的主要方法
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    // 检查currentMap是否为null，如果currentMap为null，说明已经调用了ExternalAppendOnlyMap的迭代器方法
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    // 聚合函数
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    // 迭代传入的记录
    while (entries.hasNext) {
      // 获取记录
      curEntry = entries.next()
      // 估算当前currentMap的大小
      val estimatedSize = currentMap.estimateSize()
      // 如果currentMap的大小大于之前记录的内存使用峰值，则更新内存使用峰值
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      // 检查是否需要溢写，该方法返回值表示是否发生了溢写
      if (maybeSpill(currentMap, estimatedSize)) {
        // 如果发生溢写，则重新构建一个新的SizeTrackingAppendOnlyMap赋值给currentMap
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      // 进行数据聚合
      currentMap.changeValue(curEntry._1, update)
      // 更新已插入的键值对计数
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   *
   * 重载的insertAll()方法
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   *
   * 溢写操作
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    // 获取对currentMap中的数据使用keyComparator比较器进行排序后的键值对迭代器
    val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
    // 使用spillMemoryIteratorToDisk()方法进行溢写
    val diskMapIterator = spillMemoryIteratorToDisk(inMemoryIterator)
    // 将溢写后得到的迭代器保存到spilledMaps数组中
    spilledMaps += diskMapIterator
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   *
   * 强制溢写
   */
  override protected[this] def forceSpill(): Boolean = {
    assert(readingIterator != null)
    // 使用SpillableIterator进行溢写
    val isSpilled = readingIterator.spill()
    if (isSpilled) {
      currentMap = null
    }
    isSpilled
  }

  /**
   * Spill the in-memory Iterator to a temporary file on disk.
   *
   * 溢写操作，它会将经过排序的当前currentMap中的数据溢写到磁盘
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[(K, C)])
      : DiskMapIterator = {
    // 创建临时文件，命名为"temp_local_"前缀加上UUID字符串
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    // 获取临时文件的DiskBlockObjectWriter
    val writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics)
    // 记录每个批次溢写的键值对数量
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    // 记录溢写操作中每个批次的数据的字节大小
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    // 刷盘操作
    def flush(): Unit = {
      // DiskBlockObjectWriter进行刷盘，返回FileSegment对象
      val segment = writer.commitAndGet()
      // 记录该批次刷盘后的数据大小
      batchSizes += segment.length
      // 更新已溢写数据的总大小
      _diskBytesSpilled += segment.length
      // 重置objectsWritten计数
      objectsWritten = 0
    }

    var success = false
    try {
      // 迭代键值对记录
      while (inMemoryIterator.hasNext) {
        val kv = inMemoryIterator.next()
        // 使用DiskBlockObjectWriter向临时文件写入键值对
        writer.write(kv._1, kv._2)
        // 维护objectsWritten计数
        objectsWritten += 1

        /**
         * 如果写入的键值对数量达到了批次刷盘阈值serializerBatchSize，则进行刷盘
         * serializerBatchSize由spark.shuffle.spill.batchSize参数配置，默认为10000，
         * 也即是10000条键值对刷一次盘
         */
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }

      // 所有键值对记录都迭代完了，检查是否还有未刷盘的数据
      if (objectsWritten > 0) {
        // 进行刷盘，并关闭DiskBlockObjectWriter
        flush()
        writer.close()
      } else { // objectsWritten为0
        // 否则说明最后一次没有任何写出，那么放弃最后一次写出并关闭DiskBlockObjectWriter
        writer.revertPartialWritesAndClose()
      }
      // 标记写出成功
      success = true
    } finally {
      if (!success) { // 写出不成功
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        // 放弃最后一次写出，关闭DiskBlockObjectWriter，并删除临时文件
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    // 将溢写的文件、BlockId及记录了每个批次数据大小的数组封装为DiskMapIterator对象返回
    new DiskMapIterator(file, blockId, batchSizes)
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   * 获取迭代器
   */
  def destructiveIterator(inMemoryIterator: Iterator[(K, C)]): Iterator[(K, C)] = {
    // 返回SpillableIterator迭代器
    readingIterator = new SpillableIterator(inMemoryIterator)
    readingIterator
  }

  /**
   * Return a destructive iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   *
   * 获取迭代器
   */
  override def iterator: Iterator[(K, C)] = {
    // 如果currentMap为null则抛出异常
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }
    if (spilledMaps.isEmpty) {
      /**
       * 如果没有溢写的数据，则返回CompletionIterator迭代器
       * CompletionIterator迭代器对迭代操作进行了封装，
       * 当迭代完成后会调用freeCurrentMap()方法将currentMap置为null并释放内存
       */
      CompletionIterator[(K, C), Iterator[(K, C)]](
        destructiveIterator(currentMap.iterator), freeCurrentMap())
    } else {
      // 否则返回ExternalIterator迭代器
      new ExternalIterator()
    }
  }

  // 将currentMap置为null并释放内存
  private def freeCurrentMap(): Unit = {
    if (currentMap != null) {
      currentMap = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    // 优先队列，小顶堆实现
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    // 获取currentMap中数据经过排序后的迭代器，使用CompletionIterator进行封装
    private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](destructiveIterator(
      currentMap.destructiveSortedIterator(keyComparator)), freeCurrentMap())

    // 将未溢写的键值对迭代器和溢写得到的键值对迭代器保存到Seq中，并为它们创建BufferedIterator
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      // 将it迭代器中接下来键相同的键值对存放到kcPairs中
      val kcPairs = new ArrayBuffer[(K, C)]
      // 将it中接下来键相同的键值对都存入kcPairs
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        // 将kcPairs存入mergeHeap队列
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * @param it iterator to read from
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      // 如果迭代器还存在下一个键值对
      if (it.hasNext) {
        // 获取下一个键值对
        var kc = it.next()
        // 添加到buf中
        buf += kc
        // 将键值对的键进行hash
        val minHash = hashKey(kc)

        /**
         * 由于it迭代器中的键值对已经按照键排序了，因此迭代的元素的键可能是一直相同的。
         * 该才做会不断迭代it迭代器，判断it中下一个键值对的键是否与kc键值对的键相同
         * 如果相同则将该键值对存入buf。
         * 该操作会将it迭代器中元素按照键顺序取出，并将相同的键存入buf数组。
         */
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          // 获取下一个键值对
          kc = it.next()
          // 添加到buf数组
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     *
     * 按键聚合
     *
     * @param key 指定的键
     * @param baseCombiner 之前已经聚合而得到的值
     * @param buffer 装有键数组的Buffer
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      // 遍历StreamBuffer中的数组
      while (i < buffer.pairs.length) {
        // 获取键值对
        val pair = buffer.pairs(i)
        // 如果键与传入的key相同
        if (pair._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          // 则将该键值对从数组中移除
          removeFromBuffer(buffer.pairs, i)
          // 对值进行聚合操作
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      // 如果没有进行聚合，则直接返回原来的值
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     *
     * 移除指定位置的键值对。
     * 该操作会在移除键值对之后，将数组最后一个键值对移动到被移除的键值对所在的索引位置上，
     * 并调整数组的大小
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      // 获取指定位置的键值对
      val elem = buffer(index)
      // 将数组最后一个键值对移动到被移除的键值对所在的索引位置上
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      // 调整数组的大小
      buffer.reduceToSize(buffer.size - 1)
      // 返回移除的键值对
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     * 是否还有下一个键值对，通过mergeHeap是否为空来判断
     */
    override def hasNext: Boolean = mergeHeap.nonEmpty

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     *
     * 获取下一个键值对，实现方式也是对两个迭代器的交替迭代。
     */
    override def next(): (K, C) = {
      // 如果mergeHeap为空，说明没有键值对了，直接抛出异常
      if (mergeHeap.isEmpty) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      // 出队一个StreamBuffer
      val minBuffer = mergeHeap.dequeue()
      // 获取该StreamBuffer内部的数组
      val minPairs = minBuffer.pairs
      // 获取最小键的哈希值
      val minHash = minBuffer.minKeyHash
      // 先得到数组minPairs的0号索引上的键值对
      val minPair = removeFromBuffer(minPairs, 0)
      // 获取键和值
      val minKey = minPair._1
      var minCombiner = minPair._2
      // 检查键的hash是否相同
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      // 创建合并数据用的数组
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)

      /**
       * 当mergeHeap不为空，且mergeHeap队首的StreamBuffer的键的哈希值与minHash相同，
       * 说明还有键相同的键值对。
       */
      while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
        // 出队StreamBuffer
        val newBuffer = mergeHeap.dequeue()
        // 对该StreamBuffer中的键值对进行聚合
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        // 将合并过的StreamBuffer记录到mergedBuffers中
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      // 遍历合并过的StreamBuffer，如果其中还剩有没处理的键值对，则还需要重新处理
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          // 将迭代器中下一批键相同的键值对放入数组
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          // 将该StreamBuffer再次放入mergeHeap中
          mergeHeap.enqueue(buffer)
        }
      }

      // 返回聚合后的键值对
      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     *
     * @param iterator 所关联的键值对迭代器
     * @param pairs 存储了相同的键的数组
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],
        val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      // 如果内部的数组还存在键值对，说明该StreamBuffer不为空
      def isEmpty: Boolean = pairs.length == 0

      // Invalid if there are no more pairs in this stream
      // 获取键的hash
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   * 存储在磁盘上的溢写文件中的键值对的迭代器
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    // 对批次偏移量进行scanLeft操作
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    // 检查最大的偏移量是否与文件长度相同，如果不同则说明溢写文件有问题
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    // 记录批次的索引
    private var batchIndex = 0  // Which batch we're in
    // 文件输入流
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    // 获取下一个批次数据的反序列化流
    private var deserializeStream = nextBatchStream()
    private var nextItem: (K, C) = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      // 批次计数不能超过总批次数量
      if (batchIndex < batchOffsets.length - 1) {
        // 将读取上一个批次数据的反序列化流关闭
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        // 获取当前批次数据的起始偏移量
        val start = batchOffsets(batchIndex)
        // 获取文件输入流
        fileStream = new FileInputStream(file)
        // 使用FileChannel定位到起始偏移量的位置
        fileStream.getChannel.position(start)
        // 批次索引自增1
        batchIndex += 1

        // 获取当前批次数据的终止偏移量
        val end = batchOffsets(batchIndex)

        // 检查起始和终止偏移量是否合法
        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        // 根据对应的偏移量范围构建当前批次数据的缓冲输入流
        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        // 使用SerializerManager进行解压缩、加解密处理
        val wrappedStream = serializerManager.wrapStream(blockId, bufferedStream)
        // 生成反序列化流
        ser.deserializeStream(wrappedStream)
      } else { // 没有剩余批次数据了，做清理操作
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     *
     * 获取当前读取的批次中的下一个键值对
     */
    private def readNextItem(): (K, C) = {
      try {
        // 从反序列化流中读取下一个键和值
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        // 构造为二元元组
        val item = (k, c)
        // 当前批次读取记录的计数自增1
        objectsRead += 1

        /**
         * 如果当前批次读取的数据条数达到了serializerBatchSize，
         * 则需要构建下一个批次的输入流了。
         * serializerBatchSize默认为10000，溢写时也是按照这个值进行批次划分的。
         */
        if (objectsRead == serializerBatchSize) {
          // 要开始新的批次读取了，重置当前批次读取记录的计数为0
          objectsRead = 0
          // 获取下一个批次的输入流
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException => // 读取到文件末尾了
          // 进行清理工作
          cleanup()
          null
      }
    }

    // 是否还有下一个键值对
    override def hasNext: Boolean = {
      // 当前批次没有下一个键值对了
      if (nextItem == null) {
        // 当前批次的输入流也为null ，说明没有剩余了键值对了
        if (deserializeStream == null) {
          return false
        }
        // 如果当前批次的输入流不为null，尝试读取当前批次的下一个键值对
        nextItem = readNextItem()
      }
      // 根据读取的当前批次的下一个键值对是否为null判断是否还有剩余的键值对
      nextItem != null
    }

    // 获取下一个键值对
    override def next(): (K, C) = {
      /**
       * 如果nextItem不为null，则返回nextItem，
       * 否则使用readNextItem()方法来获取
       */
      val item = if (nextItem == null) readNextItem() else nextItem
      // 如果获取不到下一个键值对，抛出异常
      if (item == null) {
        throw new NoSuchElementException
      }
      // 将nextItem置为null
      nextItem = null
      // 返回获取的键值对
      item
    }

    // 清理操作
    private def cleanup() {
      // 将batchIndex直接置为batchOffsets.length，这样就无法继续往下读了
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      // 关闭当前批次的反序列化流和文件流
      val ds = deserializeStream
      if (ds != null) {
        ds.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      // 删除溢写文件
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file}")
        }
      }
    }

    // 初始化时就会向TaskContxt上下文添加Task完成的回调监听器，以在Task完成时执行清理工作
    context.addTaskCompletionListener(context => cleanup())
  }

  private[this] class SpillableIterator(var upstream: Iterator[(K, C)])
    extends Iterator[(K, C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[(K, C)] = null

    private var cur: (K, C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) { // 如果发生过溢写，则直接返回false
        false
      } else {
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s"it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        // 将upstream的数据通过spillMemoryIteratorToDisk()方法溢写到磁盘
        nextUpstream = spillMemoryIteratorToDisk(upstream)
        // 标记发生了溢写
        hasSpilled = true
        true
      }
    }

    def readNext(): (K, C) = SPILL_LOCK.synchronized {
      /**
       * 如果nextUpstream不为null，说明发生过溢写，
       * 此时upstream的数据已经被溢写到磁盘了，
       * 接下来的遍历需要使用nextUpstream迭代器（DiskMapIterator类型）中的数据
       */
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      // 如果还存在下一个元素，则返回
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    // 如果cur为null，说明没有更多的元素了
    override def hasNext(): Boolean = cur != null

    override def next(): (K, C) = {
      // 当前cur是要返回的元素，先记录
      val r = cur
      // cur更新为下一个要返回的元素
      cur = readNext()
      // 返回之前记录的cur
      r
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key.
   * 获取键值对键的哈希值，使用伴生对象的方法生成
   */
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)

  override def toString(): String = {
    this.getClass.getName + "@" + java.lang.Integer.toHexString(this.hashCode())
  }
}

private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   * 用于获取键的哈希值
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   * 哈希比较器
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
