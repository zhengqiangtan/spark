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

package org.apache.spark.deploy.master

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rpc.RpcEnv

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 *
 * The implementation of this trait defines how name-object pairs are stored or retrieved.
  *
  * PersistenceEngine用于当Master发生故障后，通过领导选举选择其他Master接替整个集群的管理工作时，
  * 能够使得新激活的Master有能力从故障中恢复整个集群的状态信息，进而恢复对集群资源的管理和分配。
 */
@DeveloperApi
abstract class PersistenceEngine {

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
    *
    * 对对象进行序列化和持久化的抽象方法，具体的实现依赖于底层使用的存储。
   */
  def persist(name: String, obj: Object): Unit

  /**
   * Defines how the object referred by its name is removed from the store.
    *
    * 对对象进行非持久化（从存储中删除）的抽象方法，具体的实现依赖于底层使用的存储。
   */
  def unpersist(name: String): Unit

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
    *
    * 读取匹配给定前缀的所有对象。
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  // 对应用的信息（ApplicationInfo）进行持久化的模板方法，其依赖于persist的具体实现。
  final def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  // 对应用的信息（ApplicationInfo）进行非持久化的模板方法，其依赖于unpersist的具体实现。
  final def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  // 对Worker的信息（WorkerInfo）进行持久化的模板方法，其依赖于persist的具体实现。
  final def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  // 对Worker的信息（WorkerInfo）进行非持久化的模板方法，其依赖于unpersist的具体实现。
  final def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  // 对Driver的信息（DriverInfo）进行持久化的模板方法，其依赖于persist的具体实现。
  final def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  // 对Driver的信息（DriverInfo）进行非持久化的模板方法，其依赖于unpersist的具体实现。
  final def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
    *
    * 读取并反序列化所有持久化的数据。
   */
  final def readPersistedData(
      rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
    }
  }

  // 用于关闭PersistenceEngine。
  def close() {}
}

private[master] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}
