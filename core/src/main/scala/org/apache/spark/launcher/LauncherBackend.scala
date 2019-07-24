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

package org.apache.spark.launcher

import java.net.{InetAddress, Socket}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A class that can be used to talk to a launcher server. Users should extend this class to
 * provide implementation for the abstract methods.
 *
 * See `LauncherServer` for an explanation of how launcher communication works.
  *
  * 该类是SchedulerBackend与LauncherServer通信的组件
 */
private[spark] abstract class LauncherBackend {

  // 读取与LauncherServer建立的Socket连接上的消息的线程
  private var clientThread: Thread = _
  // BackendConnection实例
  private var connection: BackendConnection = _

  /**
    * LauncherBackend的最后一次状态，枚举类型Spark-AppHandle.State，共有：
    * - 未知（UNKNOWN）
    * - 已连接（CONNE-CTED）
    * - 已提交（SUBMITTED）
    * - 运行中（RUNNING）
    * - 已完成（FINISHED）
    * - 已失败（FAILED）
    * - 已被杀（KILLED）
    * - 丢失（LOST）
    */
  private var lastState: SparkAppHandle.State = _

  // clientThread是否与LauncherServer已经建立了Socket连接的状态
  @volatile private var _isConnected = false

  // 用于和LauncherServer建立连接
  def connect(): Unit = {
    // 系统环境变量_SPARK_LAUNCHER_PORT和_SPARK_LAUNCHER_SECRET，分别代表Socket端口和密钥
    val port = sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT).map(_.toInt)
    val secret = sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET)
    if (port != None && secret != None) {
      // 构造Socket
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      // 构造BackendConnection对象
      connection = new BackendConnection(s)
      // 发送Hello消息，携带了密钥和Spark版本信息
      connection.send(new Hello(secret.get, SPARK_VERSION))
      // 创建BackendConnection线程并启动
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      clientThread.start()
      // 标识运行状态
      _isConnected = true
    }
  }

  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } finally {
        if (clientThread != null) {
          clientThread.join()
        }
      }
    }
  }

  // 向LauncherServer发送SetAppId消息
  def setAppId(appId: String): Unit = {
    if (connection != null) {
      connection.send(new SetAppId(appId))
    }
  }

  // 向LauncherServer发送SetState消息
  def setState(state: SparkAppHandle.State): Unit = {
    if (connection != null && lastState != state) {
      connection.send(new SetState(state))
      lastState = state
    }
  }

  /** Return whether the launcher handle is still connected to this backend.
    * 判断clientThread是否与LauncherServer已经建立了Socket连接的状态
    **/
  def isConnected(): Boolean = _isConnected

  /**
   * Implementations should provide this method, which should try to stop the application
   * as gracefully as possible.
    *
    * LauncherBackend定义的处理LauncherServer的停止消息的抽象方法
   */
  protected def onStopRequest(): Unit

  /**
   * Callback for when the launcher handle disconnects from this backend.
    *
    * 用于在关闭Socket客户端与LauncherServer的Socket服务端建立的连接时，进行一些额外的处理
   */
  protected def onDisconnected() : Unit = { }

  // 用于启动一个调用onStopRequest()方法的线程
  private def fireStopRequest(): Unit = {
    val thread = LauncherBackend.threadFactory.newThread(new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        onStopRequest()
      }
    })
    thread.start()
  }

  // 用于保持与LauncherServer的Socket连接，并通过此Socket连接收发消息
  private class BackendConnection(s: Socket) extends LauncherConnection(s) {

    // 用于处理LauncherServer发送的消息
    override protected def handle(m: Message): Unit = m match {
      case _: Stop => // 处理Stop类型的方法
        // 调用LauncherBackend的fireStopRequest()方法停止Executor
        fireStopRequest()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

    // 重写了close()方法
    override def close(): Unit = {
      try {
        // 调用父类的close()方法
        super.close()
      } finally {
        // 调用外部类LauncherBackend的onDisconnected()方法
        onDisconnected()
        _isConnected = false
      }
    }

  }

}

private object LauncherBackend {

  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}
