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

private[spark] object TaskState extends Enumeration {

  // 启动中，运行中，已结束，已失败，已被杀死，已丢失
  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  // 结束状态，可以是已结束、已失败、已被杀死、已丢失任意一种
  private val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

  type TaskState = Value

  // 判断是否是已失败，LOST和FAILED都属于失败
  def isFailed(state: TaskState): Boolean = (LOST == state) || (FAILED == state)

  // 判断是否是已结束，FINISHED、FAILED、KILLED、LOST都属于已结束
  def isFinished(state: TaskState): Boolean = FINISHED_STATES.contains(state)
}
