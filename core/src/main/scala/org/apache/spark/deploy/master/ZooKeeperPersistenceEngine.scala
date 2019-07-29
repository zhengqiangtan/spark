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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

/**
  * ZooKeeperPersistenceEngine是基于ZooKeeper的持久化引擎。
  * 对于ApplicationInfo、WorkerInfo及DriverInfo，ZooKeeperPersistenceEngine会将
  * 它们的数据存储到ZooKeeper的不同节点（也称为Znode）中，当要移除它们时，这些节点将被删除。
  *
  * @param conf
  * @param serializer 持久化时使用的序列化器
  */
private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
  with Logging {

  /**
    * ZooKeeperPersistenceEngine在ZooKeeper上的工作目录，
    * 是Spark基于ZooKeeper进行热备的根节点的子节点master_status。
    * 热备的根节点可通过spark.deploy.ZooKeeper.dir属性配置，默认为spark。
    */
  private val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/master_status"

  // 连接Zookeeper的客户端
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)


  override def persist(name: String, obj: Object): Unit = {
    // 调用了serializeIntoFile()方法
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    // 删除对应的Zookeeper节点
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(WORKING_DIR).asScala
      // 过滤出ZooKeeper的master_status节点下文件名匹配前缀的所有子节点
      .filter(_.startsWith(prefix))
      // 进行反序列化操作
      .flatMap(deserializeFromFile[T])
  }

  override def close() {
    // 关闭Zookeeper连接
    zk.close()
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    // 对数据进行序列化
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    // 将数据保存到Zookeeper的master_status节点下创建的持久化子节点中
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
    }
  }
}
