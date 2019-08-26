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

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 *
 * 可插拔的Shuffle系统接口。ShuffleManager会Driver或Executor的SparkEnv被创建时一并创建。
 * 可以通过spark.shuffle.manage配置指定具体的实现类。
 * Driver会将Shuffle操作注册到该组件上，Executor可以询问该组件以读或写数据。
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * 注册一个Shuffle过程
   *
   * @param shuffleId Shuffle过程的ID
   * @param numMaps Shuffle过程的Map任务数量
   * @param dependency 该Shuffle过程的ShuffleDependency
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks.
   * 获取输出数据用的ShuffleWriter；会被Executor的Map任务调用
   *
   * @param handle 具体的ShuffleHandle
   * @param mapId Map任务ID
   * @param context TaskContext上下文对象
   */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   * 获取输出数据用的ShuffleReader；会被Executor的Map任务调用
   *
   * @param handle 具体的ShuffleHandle
   * @param startPartition 拉取的起始Partition的ID，包括
   * @param endPartition 拉取的终止Partition的ID，不包括
   * @param context TaskContxt上下文
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * 取消对指定的Shuffle过程额注册
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   * 用于获取Shuffle Block数据的ShuffleBlockResolver
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
