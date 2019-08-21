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

package org.apache.spark.deploy.client

/**
 * Callbacks invoked by deploy client when various events happen. There are currently four events:
 * connecting to the cluster, disconnecting, being given an executor, and having an executor
 * removed (either due to failure or due to revocation).
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait StandaloneAppClientListener {
  // 有Application建立了连接
  def connected(appId: String): Unit

  /** Disconnection may be a temporary state, as we fail over to a new Master.
   * 当注册Application失败时，该方法会被调用
   **/
  def disconnected(): Unit

  /** An application death is an unrecoverable failure condition.
   * 当注册Application多次重试依旧失败时，或者Application被移除时会调用该方法，
   * 这种失败情况是不可恢复的
   **/
  def dead(reason: String): Unit

  // 有Executor被添加
  def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  // 有Executor被移除
  def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit
}
