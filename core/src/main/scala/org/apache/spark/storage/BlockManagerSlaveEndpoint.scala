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

import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{MapOutputTracker, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An RpcEndpoint to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
  *
  * 每个Executor或Driver的SparkEnv中都有属于自己的BlockManagerSlaveEndpoint，
  * 分别由各自的SparkEnv负责创建和注册到各自的RpcEnv中。
  * Driver或Executor都存在各自的BlockManagerSlaveEndpoint，
  * 并由各自BlockManager的slaveEndpoint属性持有各自BlockManagerSlaveEndpoint的RpcEndpointRef。
  * BlockManagerSlaveEndpoint将接收BlockManagerMasterEndpoint下发的命令。
  * 例如，删除Block、获取Block状态、获取匹配的BlockId等。
  *
  * BlockManagerSlaveEndpoint用于接收BlockManagerMasterEndpoint的命令并执行相应的操作。
  *
  * @param rpcEnv 所属的RpcEnv
  * @param blockManager 所属对应的BlockManager
  * @param mapOutputTracker MapOutputTracker实例，用于Map任务输出跟踪，后面会讲解
  */
private[storage]
class BlockManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends ThreadSafeRpcEndpoint with Logging {

  // 异步线程池
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-slave-async-thread-pool")

  // 隐式对象，包装了asyncThreadPool
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  // Operations that involve removing blocks may be slow and should be done asynchronously
  // 用于接收BlockManagerMasterEndpoint的命令并执行相应的操作
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RemoveBlock(blockId) =>
      // 异步执行
      doAsync[Boolean]("removing block " + blockId, context) {
        blockManager.removeBlock(blockId)
        true
      }

    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, context) {
        blockManager.removeRdd(rddId)
      }

    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, context) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }

    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, context) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }

    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))

    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))

    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())
  }

  // 用于异步处理消息
  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T) {
    // 创建Future
    val future = Future {
      logDebug(actionMessage)
      // 执行具体操作
      body
    }
    // 执行成功时调用
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
      // 回复Response
      context.reply(response)
      logDebug("Sent response: " + response + " to " + context.senderAddress)
    }
    // 执行失败时调用
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
      // 回复失败出现的异常
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
