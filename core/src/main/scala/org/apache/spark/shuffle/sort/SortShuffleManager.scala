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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

/**
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * Sort-based shuffle has two different write paths for producing its map output files:
 *
 *  - Serialized sorting: used when all three of the following conditions hold:
 *    1. The shuffle dependency specifies no aggregation or output ordering.
 *    2. The shuffle serializer supports relocation of serialized values (this is currently
 * supported by KryoSerializer and Spark SQL's custom serializers).
 *    3. The shuffle produces fewer than 16777216 output partitions.
 *  - Deserialized sorting: used to handle all other cases.
 *
 * -----------------------
 * Serialized sorting mode
 * -----------------------
 *
 * In the serialized sorting mode, incoming records are serialized as soon as they are passed to the
 * shuffle writer and are buffered in a serialized form during sorting. This write path implements
 * several optimizations:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 * consumption and GC overheads. This optimization requires the record serializer to have certain
 * properties to allow serialized records to be re-ordered without requiring deserialization.
 * See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 * arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 * record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 * partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 * simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 * and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on these optimizations, see SPARK-7081.
 *
 * 管理基于排序的Shuffle——输入的记录按照目标分区ID排序，然后输出到一个单独的map输出文件中。
 * reduce为了读出map输出，需要获取map输出文件的连续内容。
 * 当map的输出数据太大已经不适合放在内存中时，排序后的输出子集将被溢出到文件中，这些磁盘上的文件将被合并生成最终的输出文件。
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  // 检查是否开启溢写
  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    // 1.6版本之后默认开启Shuffle的溢写操作，不会理会spark.shuffle.spill参数
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  /**
   * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    *
    * Shuffle的ID与为此Shuffle生成输出的map任务的数量之间的映射关系。
   */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  // IndexShuffleBlockResolver用于创建和维护Shuffle产生的数据块与物理文件位置之间的映射关系
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   *
   * 用于根据条件创建不同的ShuffleHandle实例。
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // 判断使用哪一种ShuffleHandle
    if (SortShuffleWriter.shouldBypassMergeSort(SparkEnv.get.conf, dependency)) {
      // 需要绕开合并及排序，则创建BypassMergeSortShuffleHandle
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      // 如果可以使用序列化的Shuffle，则创建SerializedShuffleHandle
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      // 其他情况，将创建BaseShuffleHandle
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   *
   * 用于获取对map任务输出的分区数据文件中从startPartition到endPartition-1范围内的数据
   * 进行读取的读取器（即BlockStoreShuffleReader），供reduce任务使用。
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks.
   * 用于根据ShuffleHandle获取ShuffleWriter。
   *
   * @param handle ShuffleDependency具体的ShuffleHandle
   * @param mapId Map任务ID，一般是分区的ID
   * @param context TaskContext上下文对象
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {

    // 将指定的shuffleId和Shuffle对应的map任务数注册到numMapsForShuffle字典中
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get

    // 根据ShuffleHandle的具体类型，创建不同的ShuffleWriter
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
  case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
    new BypassMergeSortShuffleWriter(
      env.blockManager,
      shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
      bypassMergeSortHandle,
      mapId,
      context,
      env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager.
   * 用于根据指定的shuffleId删除此Shuffle过程的所有map任务的数据文件和索引文件
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        // 删除map任务产生的数据文件和索引文件的
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
   * */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1 // 16777216

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   *
   * Shuffle是否可以序列化
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    // Shuffle ID和分区数量
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      /**
       * 如果ShuffleDependency的序列化器不支持重新定位序列化对象，也即是说，
       * ShuffleDependency的序列化器无法对流中输出的序列化后的对象的字节进行排序，则返回false。
       * 这是使用序列化排序模式需要满足的第2个条件。
       */
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      /**
       * ShuffleDependency指定了聚合器，说明存在聚合操作，则返回false。
       * 这是使用序列化排序模式需要满足的第1个条件。
       */
      log.debug(
        s"Can't use serialized shuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")

      /**
       * Shuffle过程产生的分区数大于MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE（16777216），
       * 则返回false。这是使用序列化排序模式需要满足的第3个条件。
       */
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      // 三个条件都满足，返回true
      true
    }
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 *
 * 用于确定何时选择使用序列化的Shuffle
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 *
 * 用于确定何时选择绕开合并和排序的Shuffle路径
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
