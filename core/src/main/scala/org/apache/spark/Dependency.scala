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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
  *
  * 用于表示RDD之间的依赖关系
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  // 返回当前依赖的RDD
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
  *
  * 如果RDD与上游RDD的分区是一对一的关系，那么RDD和其上游RDD之间的依赖关系属于窄依赖（NarrowDependency）。
  * 有三个子类：OneToOneDependency、RangeDependency和PruneDependency
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
    *
    * 获取某一分区的所有父级别分区序列
    *
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
  *
  * RDD与上游RDD的分区如果不是一对一的关系，或者RDD的分区依赖于上游RDD的多个分区，
  * 那么这种依赖关系就叫做Shuffle依赖（ShuffleDependency）
 *
 * @param _rdd the parent RDD
  *             泛型要求必须是Product2[K,V]及其子类的RDD
 * @param partitioner partitioner used to partition the shuffle output
  *                    分区计算器Partitioner
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used.
 * @param keyOrdering key ordering for RDD's shuffles
  *                    按照K进行排序的scala.math.Ordering的实现类
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
  *                   对map任务的输出数据进行聚合的聚合器。
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
  *                       是否在map端进行合并，默认为false。
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  // 将_rdd转换为RDD[Product2[K, V]]后返回
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  // K的类名
  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  // V的类名
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  // 结合器C的类名
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  // 当前ShuffleDependency的身份标识
  val shuffleId: Int = _rdd.context.newShuffleId()

  // 当前ShuffleDependency的处理器
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  // 将自己注册到SparkContext的ContextCleaner中。
  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // 子RDD的分区与依赖的父RDD的分区相同
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  /**
    * RangeDependency的分区是一对一的，
    * 且索引为partitionId的子RDD分区与索引为partitionId-outStart+inStart的父RDD分区相对应
    * （outStart代表子RDD的分区范围起始值，inStart代表父RDD的分区范围起始值）
    * @param partitionId a partition of the child RDD
    * @return the partitions of the parent RDD that the child partition depends upon
    */
  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
