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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
  *
  * 用于Shuffle执行过程中，reduce任务从其他节点的Block文件中读取
  * 由起始分区（startPartition）和结束分区（endPartition）指定范围内的数据。
 *
  * @param handle
  * @param startPartition 要读取的起始分区ID（分区索引）
  * @param endPartition 要读取的结束分区ID（分区索引）
  * @param context
  * @param serializerManager
  * @param blockManager
  * @param mapOutputTracker
  * @tparam K
  * @tparam C
  */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  // BaseShuffleHandle的dependency属性，即ShuffleDependency。
  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    // 伴随着对本地和远端的数据块的获取
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      // 获取当前reduce任务所需的map任务中间输出数据的BlockManager的BlockManagerId及每个数据块的BlockId与大小
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))

    // Wrap the streams for compression and encryption based on configuration
    // 对各个数据块的输入流进行压缩和加密
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      serializerManager.wrapStream(blockId, inputStream)
    }

    // 序列化器
    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    // 给每个流创建一个key/value的迭代器
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      // 进行反序列化
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    // 对任务的度量信息进行更新
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    // 创建可中断的迭代器，能够支持TaskAttempt的取消操作
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // 处理可能存在的聚合迭代
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // 如果指定了聚合函数且允许在map端进行合并，在reduce端对数据进行聚合
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        // 使用聚合器进行聚合
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // 如果指定了聚合函数，但不允许在map端进行合并，在reduce端对数据进行缓存
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        // 使用聚合器进行聚合
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      // 没有指定聚合函数，那么不作任何处理
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      // 如果指定了排序函数，则创建ExternalSorter，没有指定聚合函数和分区器，只指定了排序函数
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        // 使用ExternalSorter进行排序
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        // 对数据进行缓存
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // 封装并返回CompletionIterator
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      // 如果没有指定排序函数，那么返回aggregatedIter
      case None =>
        aggregatedIter
    }
  }
}
