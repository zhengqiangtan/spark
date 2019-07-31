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

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 *
 * [ Initialized --{@literal >} checkpointing in progress --{@literal >} checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
  *
  * RDDCheckpointData用于保存与检查点相关的信息，
  * 每个RDDCheckpointData实例都与一个RDD实例相关联。
  *
  * @param rdd 个RDDCheckpointData关联的RDD实例
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  /**
    * 检查点的状态。默认为Initialized。cpState的值来自枚举类型CheckpointState，定义了检查点的状态：
    * - 初始化完成（Initialized）
    * - 正在保存检查点（CheckpointingInProgress）
    * - 保存检查点完毕（Checkpointed）
    */
  protected var cpState = Initialized

  // The RDD that contains our checkpointed data
  // 保存检查点数据的RDD，即CheckpointRDD的实现类
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * Return whether the checkpoint data for this RDD is already persisted.
    *
    * 用于判断是否已经为RDDCheckpointData关联的RDD保存了检查点数据
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
    *
    * 用于将RDDCheckpointData关联的RDD的数据保存到检查点的模板方法
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        // 如果检查点的状态是Initialized，那么将cpState设置为CheckpointingInProgress
        cpState = CheckpointingInProgress
      } else {
        // 否则直接返回
        return
      }
    }

    /**
      * 调用了doCheckpoint()方法，由子类实现。
      * RDDCheckpointData的子类有LocalRDDCheckpointData和ReliableRDDCheckpointData。
      */
    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    RDDCheckpointData.synchronized {
      // 由cpRDD持有刚生成的CheckpointRDD
      cpRDD = Some(newRDD)
      // 将cpState设置为Checkpointed
      cpState = Checkpointed
      // 清空RDD的依赖。之所以清空依赖，是因为现在已经有了CheckpointRDD，之前的依赖关系不再需要了。
      rdd.markCheckpointed()
    }
  }

  /**
   * Materialize this RDD and persist its content.
   *
   * Subclasses should override this method to define custom checkpointing behavior.
   * @return the checkpoint RDD created in the process.
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
    *
    * 用于获取cpRDD持有的CheckpointRDD
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * For tests only.
    *
    * 用于获取CheckpointRDD的分区数组
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 */
private[spark] object RDDCheckpointData
