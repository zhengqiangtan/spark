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

package org.apache.spark.storage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.util.Utils

/**
  * @param id RDD的id
  * @param name RDD的名称
  * @param numPartitions RDD的分区数量
  * @param storageLevel RDD的存储级别
  * @param parentIds RDD的所有父RDD的id序列
  * @param callSite RDD的用户调用栈信息
  * @param scope RDD的操作范围。每一个RDD都有一个RDDOperationScope。
  *              RDDOperationScope与Stage或Job之间并无特殊关系，
  *              一个RDDOperationScope可以存在于一个Stage内，也可以跨越多个Job。
  */
@DeveloperApi
class RDDInfo(
    val id: Int,
    var name: String,
    val numPartitions: Int,
    var storageLevel: StorageLevel,
    val parentIds: Seq[Int],
    val callSite: String = "",
    val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {

  // 缓存的分区数量。
  var numCachedPartitions = 0
  // 使用的内存大小。
  var memSize = 0L
  // 使用的磁盘大小。
  var diskSize = 0L
  // Block存储在外部的大小。
  var externalBlockStoreSize = 0L

  // 是否已经缓存。
  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
  }

  // 由于RDDInfo继承了Ordered，所以重写了compare方法用于排序。
  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  // 用于从RDD构建出对应的RDDInfo
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    // 如果rdd有name字段就采用name字段，否则使用rdd的简单类名
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    // rdd所有依赖的ID集合
    val parentIds = rdd.dependencies.map(_.rdd.id)
    // 构造RDDInfo对象
    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, parentIds, rdd.creationSite.shortForm, rdd.scope)
  }
}
