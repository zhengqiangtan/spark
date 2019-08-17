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

package org.apache.spark.scheduler

/**
 * A location where a task should run. This can either be a host or a (host, executorID) pair.
 * In the latter case, we will prefer to launch the task on that executorID, but our next level
 * of preference will be executors on the same host if this is not possible.
 */
private[spark] sealed trait TaskLocation {
  // 所在主机地址
  def host: String
}

/**
 * A location that includes both a host and an executor id on that host.
 */
private [spark]
case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}

/**
 * A location on a host.
 */
private [spark] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}

/**
 * A location on a host that is cached by HDFS.
 */
private [spark] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}

private[spark] object TaskLocation {
  // We identify hosts on which the block is cached with this prefix.  Because this prefix contains
  // underscores, which are not legal characters in hostnames, there should be no potential for
  // confusion.  See  RFC 952 and RFC 1123 for information about the format of hostnames.
  // 标识内存位置信息的前缀
  val inMemoryLocationTag = "hdfs_cache_"

  // Identify locations of executors with this prefix.
  // 标识Executor位置信息的前缀
  val executorLocationTag = "executor_"

  // 默认创建ExecutorCacheTaskLocation实例
  def apply(host: String, executorId: String): TaskLocation = {
    new ExecutorCacheTaskLocation(host, executorId)
  }

  /**
   * Create a TaskLocation from a string returned by getPreferredLocations.
   * These strings have the form executor_[hostname]_[executorid], [hostname], or
   * hdfs_cache_[hostname], depending on whether the location is cached.
    *
    * 根据传入的字符串确定对应的位置信息实例
   */
  def apply(str: String): TaskLocation = {
    // 去掉"hdfs_cache_"前缀
    val hstr = str.stripPrefix(inMemoryLocationTag)
    if (hstr.equals(str)) { // 如果没有改变，说明不是以"hdfs_cache_"前缀开头的
      if (str.startsWith(executorLocationTag)) { // 判断是否以"executor_"前缀开头
        // 去掉"executor_"前缀
        val hostAndExecutorId = str.stripPrefix(executorLocationTag)
        // 以"_"分割，最多分为2份
        val splits = hostAndExecutorId.split("_", 2)
        // 检查切分结果是否为2份
        require(splits.length == 2, "Illegal executor location format: " + str)
        // 获取host和Executor ID
        val Array(host, executorId) = splits
        // 构建ExecutorCacheTaskLocation对象
        new ExecutorCacheTaskLocation(host, executorId)
      } else { // 否则既不是以"hdfs_cache_"前缀开头，也不是以"executor_"前缀开头
        // 构造HostTaskLocation对象
        new HostTaskLocation(str)
      }
    } else {
      // 以"hdfs_cache_"前缀开头，构造HDFSCacheTaskLocation对象
      new HDFSCacheTaskLocation(hstr)
    }
  }
}
