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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 * SizeTracker
 *
 * @param prev 父RDD
 * @param f    算子函数
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  // 获取分区器
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  // 获取获取当前RDD的所有分区，默认是父RDD的分区
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  // 计算方法
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    /**
     * 调用f函数进行计算，传入的分别是TaskContxt、Partition索引，以及迭代器对象，
     * 第三个参数为迭代器对象，它是通过firstParent[T].iterator(split, context)产生的，
     * 也即是说，第三个参数是通过父RDD的iterator()方法产生的，
     * 通过这种方式，RDD的compute()方法会逐级向前递推，一直推到最终可以获取到数据的RDD，
     * 然后逐级往下计算，先计算父RDD对应的分区数据，父RDD计算的数据会封装为迭代器对象交给自己的子RDD，
     * 这样就形成了从前到后的算子计算链条
     */
    f(context, split.index, firstParent[T].iterator(split, context))

  // 清理Dependency
  override def clearDependencies() {
    // 会将dependencies_置为null
    super.clearDependencies()
    // 将prev置为null
    prev = null
  }
}
