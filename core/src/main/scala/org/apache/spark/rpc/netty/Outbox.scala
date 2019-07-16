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

package org.apache.spark.rpc.netty

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

private[netty] sealed trait OutboxMessage {

  // 发送消息
  def sendWith(client: TransportClient): Unit

  // 发送失败的情况
  def onFailure(e: Throwable): Unit

}

// 不需要回复的消息
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    // 直接使用Transport的send()方法发送
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logWarning(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}

// 需要回复的RPC消息
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    // 记录TransportClient
    this.client = client
    // 使用TransportClient发送，记录返回的Request ID，注意最后的callback参数
    this.requestId = client.sendRpc(content, this)
  }

  // 超时
  def onTimeout(): Unit = {
    require(client != null, "TransportClient has not yet been set.")
    // 从Transport中根据Request ID移除对应的消息
    client.removeRpcRequest(requestId)
  }

  // 发送失败的处理
  override def onFailure(e: Throwable): Unit = {
    // 交给回调方法
    _onFailure(e)
  }

  // 发送成功的处理
  override def onSuccess(response: ByteBuffer): Unit = {
    // 交给回调方法
    _onSuccess(client, response)
  }

}


/**
  * @param nettyEnv 当前Outbox所在节点上的NettyRpcEnv。
  * @param address Outbox所对应的远端NettyRpcEnv的地址。
  */
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  // 向其他远端NettyRpcEnv上的所有RpcEndpoint发送的消息列表。
  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  // 当前Outbox内的TransportClient。消息的发送都依赖于此传输客户端。
  @GuardedBy("this")
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
    * 指向当前Outbox内连接任务的Future引用。
    * 如果当前没有连接任务，则connectFuture为null。
   */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  // 当前Outbox是否停止的状态。
  @GuardedBy("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
    * 表示当前Outbox内是否正有线程在处理messages列表中消息。
   */
  @GuardedBy("this")
  private var draining = false

  /**
   * Send a message. If there is no active connection, cache it and launch a new connection. If
   * [[Outbox]] is stopped, the sender will be notified with a [[SparkException]].
    * 发送消息
   */
  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) { // 如果Outbox已停止则丢弃休息
        true
      } else {
        // 否则放入messages链表
        messages.add(message)
        false
      }
    }
    if (dropped) { // 丢弃消息需要调用message发送失败的回调
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      // 调用drainOutbox()方法处理messages中的消息
      drainOutbox()
    }
  }

  /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized { // 加锁
      // 当前Outbox已停止，直接返回
      if (stopped) {
        return
      }
      // 正在连接远端服务，直接返回
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      // TransportClient为空，说明还未连接远端服务。
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        // 需要调用launchConnectTask方法运行连接远端服务的任务，然后返回。
        launchConnectTask()
        return
      }
      // 正有线程在处理（即发送）messages列表中的消息，则返回。
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      // 取出消息，如果取出为空，则返回
      message = messages.poll()
      if (message == null) {
        return
      }
      // 走到这里说明有可以处理的消息，将draining置为true
      draining = true
    }
    while (true) { // 循环处理messages列表中的消息
      try {
        val _client = synchronized { client }
        if (_client != null) {
          // 调用OutboxMessage的sendWith方法发送消息
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        // 不断从messages列表中取出消息
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  // 连接远程服务
  private def launchConnectTask(): Unit = {
    // 向NettyRpcEnv的clientConnectionExecutor提交一个连接任务
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          // 根据远端NettyRpcEnv的TransportServer的地址创建TransportClient对象
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            // 记录TransportClient
            client = _client
            if (stopped) { // 如果Outbox停止了，则关闭TransportClient
              // 这里的关闭仅仅是将client置为null，并没有真正关闭TransportClient，方便后面复用
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        // 调用drainOutbox()处理messages中的消息
        drainOutbox()
      }
    })
  }

  /**
   * Stop [[Inbox]] and notify the waiting messages with the cause.
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
   * [[SparkException]].
    *
    * 停止Outbox
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true // 更新标识
      if (connectFuture != null) {
        // 取消连接任务
        connectFuture.cancel(true)
      }
      // 关闭对应的TransportClient
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    // 逐个处理messages链表中的消息，调用它们的onFailure()方法，传递SparkException异常
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
