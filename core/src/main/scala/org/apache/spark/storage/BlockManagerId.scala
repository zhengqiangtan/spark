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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * This class represent an unique identifier for a BlockManager.
 *
 * The first 2 constructors of this class are made private to ensure that BlockManagerId objects
 * can be created only using the apply method in the companion object. This allows de-duplication
 * of ID objects. Also, constructor parameters are private to ensure that parameters cannot be
 * modified from outside this class.
 *
  * @param executorId_ 当前BlockManager所在的实例的ID。
  *                    如果实例是Driver，那么ID为字符串"driver"，
  *                    否则由Master负责给各个Executor分配，ID格式为app-日期格式字符串-数字。
  * @param host_ BlockManager所在的主机地址
  * @param port_ BlockManager中BlockTransferService对外服务的端口
  * @param topologyInfo_ 拓扑信息
  */
@DeveloperApi
class BlockManagerId private (
    //
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int,
    private var topologyInfo_ : Option[String])
  extends Externalizable {

  // 重载构造方法，仅用于反序列化
  private def this() = this(null, null, 0, None)  // For deserialization only

  // 获取executorId_
  def executorId: String = executorId_

  // 用于检查host_和port_
  if (null != host_) {
    Utils.checkHost(host_, "Expected hostname")
    assert (port_ > 0)
  }

  // 拼接host_和port_为host_:port_
  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }

  // 获取host_
  def host: String = host_

  // 获取port_
  def port: Int = port_

  // 获取拓扑信息
  def topologyInfo: Option[String] = topologyInfo_

  // 判断当前BlockManagerId所标识的BlockManager是否处于Driver上
  def isDriver: Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER || // "diver"
      executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER // "<driver>"
  }

  // 将BlockManagerId的所有信息序列化后写到外部二进制流中。
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeBoolean(topologyInfo_.isDefined)
    // we only write topologyInfo if we have it
    topologyInfo.foreach(out.writeUTF(_))
  }

  // 从外部二进制流中读取BlockManagerId的所有信息。
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    val isTopologyInfoAvailable = in.readBoolean()
    topologyInfo_ = if (isTopologyInfoAvailable) Option(in.readUTF()) else None
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString: String = s"BlockManagerId($executorId, $host, $port, $topologyInfo)"

  override def hashCode: Int =
    ((executorId.hashCode * 41 + host.hashCode) * 41 + port) * 41 + topologyInfo.hashCode

  // 判断是否相等，需要Executor ID、host、port和拓扑信息都相等才算相等
  override def equals(that: Any): Boolean = that match {
    case id: BlockManagerId =>
      executorId == id.executorId &&
        port == id.port &&
        host == id.host &&
        topologyInfo == id.topologyInfo
    case _ =>
      false
  }
}


private[spark] object BlockManagerId {

  /**
   * Returns a [[org.apache.spark.storage.BlockManagerId]] for the given configuration.
    *
    * 该方法会从将创建的BlockManagerId存入blockManagerIdCache缓存字典中
   *
   * @param execId ID of the executor.
   * @param host Host name of the block manager.
   * @param port Port of the block manager.
   * @param topologyInfo topology information for the blockmanager, if available
   *                     This can be network topology information for use while choosing peers
   *                     while replicating data blocks. More information available here:
   *                     [[org.apache.spark.storage.TopologyMapper]]
   * @return A new [[org.apache.spark.storage.BlockManagerId]].
   */
  def apply(
      execId: String,
      host: String,
      port: Int,
      topologyInfo: Option[String] = None): BlockManagerId =
    // 如果blockManagerIdCache中存在就直接返回，否则创建然后存入blockManagerIdCache
    getCachedBlockManagerId(new BlockManagerId(execId, host, port, topologyInfo))

  def apply(in: ObjectInput): BlockManagerId = {
    // 从流中反序列化得到BlockManagerId
    val obj = new BlockManagerId()
    obj.readExternal(in)
    // 如果blockManagerIdCache中存在就直接返回，否则创建然后存入blockManagerIdCache
    getCachedBlockManagerId(obj)
  }

  // 用于缓存BlockManagerId
  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    // 该操作会存入缓存，但如果已存在存入会失败
    blockManagerIdCache.putIfAbsent(id, id)
    // 获取缓存的BlockManagerId
    blockManagerIdCache.get(id)
  }
}
