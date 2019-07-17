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

package org.apache.spark.broadcast

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  // BroadcastManager是否初始化完成的状态。
  private var initialized = false
  // 广播工厂实例
  private var broadcastFactory: BroadcastFactory = null

  // 初始化时会调用该方法
  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) { // 判断是否已经初始化了
        // 创建广播工厂
        broadcastFactory = new TorrentBroadcastFactory
        // 使用广播工厂进行初始化
        broadcastFactory.initialize(isDriver, conf, securityManager)
        // 标记状态
        initialized = true
      }
    }
  }

  def stop() {
    // 停止广播工厂
    broadcastFactory.stop()
  }

  // 下一个广播对象的广播ID，类型为AtomicLong。
  private val nextBroadcastId = new AtomicLong(0)

  // 创建新的广播
  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  // 根据ID移除广播
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
