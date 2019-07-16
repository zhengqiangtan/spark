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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {

  // RPC最大重新连接次数。可以使用spark.rpc.numRetries属性进行配置，默认为3次。
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  // RPC每次重新连接需要等待的毫秒数。可以使用spark.rpc.retry.wait属性进行配置，默认值为3秒。
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  /**
    * RPC的ask操作的默认超时时间。
    * 可以使用spark.rpc.askTimeout或者spark.network.timeout属性进行配置，默认值为120秒。
    * spark.rpc.askTimeout属性的优先级更高。
    */
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * return the address for the [[RpcEndpointRef]]
    * 返回当前RpcEndpointRef对应RpcEndpoint的RPC地址（RpcAddress）。
   */
  def address: RpcAddress

  // 返回当前RpcEndpointRef对应RpcEndpoint的名称。
  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
    * 发送单向异步的消息。
    * 所谓“单向”就是发送完后就会忘记此次发送，不会有任何状态要记录，也不会期望得到服务端的回复。
    * send采用了at-most-once的投递规则。
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
    *
    * 发送消息并在指定超时时间内等待响应。
    * 该方法只会发送一次，不会重试。
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
    *
    * 以默认的超时时间作为timeout参数，调用ask[T:ClassTag](message:Any,timeout:RpcTimeout)方法。
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
   * method retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
   * retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
    *
    * 发送同步的请求，此类请求将会被RpcEndpoint接收，并在指定的超时时间内等待返回类型为T的处理结果。
    * 当此方法抛出SparkException时，将会进行请求重试，直到超过了默认的重试次数为止。
    * 由于此类方法会重试，因此要求服务端对消息的处理是幂等的。
    * 此方法也采用了at-least-once的投递规则。
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    // TODO: Consider removing multiple attempts
    // 尝试次数
    var attempts = 0
    // 用于记录异常
    var lastException: Exception = null
    while (attempts < maxRetries) { // 尝试次数小于最大可尝试次数
      // 尝试次数自增
      attempts += 1
      try {
        // 调用ask方法
        val future = ask[T](message, timeout)
        // 超时等待以获取结果
        val result = timeout.awaitResult(future)
        // 获取结果为空，抛出异常，会被捕获并记录在lastException中
        if (result == null) {
          throw new SparkException("RpcEndpoint returned null")
        }

        // 获取结果不为空，返回
        return result
      } catch {
        // 除了中断异常会向外抛出，其他异常都会被记录到lastException中
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }

      // 尝试次数小于最大可尝试次数，说明还可以尝试，需要等待一段时间后再进行
      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

}
