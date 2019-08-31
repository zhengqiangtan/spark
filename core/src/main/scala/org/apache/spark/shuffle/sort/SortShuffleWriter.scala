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

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/**
  * 提供了对Shuffle数据的排序功能。
  * SortShuffleWriter使用ExternalSorter作为排序器，
  * 由于ExternalSorter底层使用了Partitioned AppendOnlyMap和PartitionedPairBuffer两种缓存，
  * 因此SortShuffleWriter还支持对Shuffle数据的聚合功能。
  * @param shuffleBlockResolver
  * @param handle
  * @param mapId map任务的身份标识
  * @param context TaskContextImpl对象
  * @tparam K
  * @tparam V
  * @tparam C
  */
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  // handle（即BaseShuffleHandle）的dependency属性（类型为ShuffleDependency）。
  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  // 是否正在停止。
  private var stopping = false

  // map任务的状态，即MapStatus。
  private var mapStatus: MapStatus = null

  // 对Shuffle写入（也就是map任务输出到磁盘）的度量，即ShuffleWrite Metrics。
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output
   * 将map任务的输出结果写到磁盘
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 创建ExternalSorter，
    // dep.mapSideCombine决定了ExternalSorter选择PartitionedAppendOnlyMap还是PartitionedPairBuffer。
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      // 将ShuffleDependency的aggregator和keyOrdering传递给ExternalSorter的aggregator和ordering属性
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      // 未传递ShuffleDependency的aggregator和keyOrdering给ExternalSorter的aggregator和ordering属性
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }

    // 将map任务的输出记录插入到缓存中
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    // 获取Shuffle数据文件
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      // 将map端缓存的数据写入到磁盘中，并生成Block文件对应的索引文件
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      // 将map端缓存的数据写入到磁盘中，返回值记录了各个分区的长度。
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      // 生成Block文件对应的索引文件，用于记录各个分区在Block文件中对应的偏移量，以便于reduce任务拉取时使用。
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      // 构造并返回MapStatus
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  // 用于判断是否绕开合并和排序
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) { // 启用了Map端Combine，则不可使用BypassMergeSort
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      /**
       * 如果ShuffleDependency的mapSideCombine属性为false，
       * 且ShuffleDependency的分区计算器中的分区数量小于等于bypassMergeThreshold，返回true
       */
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
