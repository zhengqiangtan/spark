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

import java.io._
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance}
import org.apache.spark.util.{ThreadUtils, Utils}

private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager) extends RpcEnv(conf) with Logging {

  // 创建TransportConf
  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    // 对SparkConf进行克隆，并设置spark.rpc.io.numConnectionsPerPeer为1，用于指定对等节点间的连接数
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc", // 模块名为rpc
    conf.getInt("spark.rpc.io.threads", 0)) // 设置Netty传输线程数

  // 消息调度器
  private val dispatcher: Dispatcher = new Dispatcher(this)

  // 创建流管理器
  private val streamManager = new NettyStreamManager(this)

  // 创建TransportContext
  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  // 创建TransportClientBootstrap
  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new SaslClientBootstrap(transportConf, "", securityManager,
        securityManager.isSaslEncryptionEnabled()))
    } else {
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }

  // 创建TransportClientFactory工厂，用于常规的发送请求和接收响应
  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
   * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
   *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
    *
    * 该TransportClientFactory用于文件下载
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _

  // 用于处理请求超时的调度器，即单线程的ScheduledThreadPoolExecutor线程池
  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  /**
    * 用于异步处理TransportClientFactory.createClient()方法调用的线程池。
    * 线程池的大小默认为64，可以使用spark.rpc.connect.threads属性进行配置。
    */
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  // TransportServer
  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
    * RpcAddress与Outbox的映射关系的缓存。
    * 每次向远端发送请求时，此请求消息首先放入此远端地址对应的Outbox，然后使用线程异步发送。
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  // 创建TransportServer
  def startServer(bindAddress: String, port: Int): Unit = {
    // 先创建TransportServerBootstrap列表
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new SaslServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    // 创建TransportServer
    server = transportContext.createServer(bindAddress, port, bootstraps)
    // 向Dispatcher注册RpcEndpointVerifier，注册名为endpoint-verifier。
    dispatcher.registerRpcEndpoint(RpcEndpointVerifier.NAME, // endpoint-verifier
      new RpcEndpointVerifier(this, dispatcher)) // RpcEndpointVerifier用于校验指定名称的RpcEndpoint是否存在。
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  // 设置RpcEndpoint
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    // 使用Dispatcher注册RpcEndpoint
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    // 得到RpcEndpointAddress对象
    val addr = RpcEndpointAddress(uri)
    // 构建NettyRpcEndpointRef
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    // 获取远端NettyRpcEnv的RpcEndpointVerifier
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    // 使用ask方法进行询问，向远端NettyRpcEnv的RpcEndpointVerifier发送RpcEndpointVerifier.CheckExistence消息
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        // 能够查询到
        Future.successful(endpointRef)
      } else {
        // 没有查询到
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    // 停止Dispatcher，对对应的RpcEndpoint取消注册
    dispatcher.stop(endpointRef)
  }

  // 向远端RpcEndpoint发送消息
  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) { // 消息对应的TransportClient存在，直接发送
      message.sendWith(receiver.client)
    } else { // TransportClient不存在
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        // 获取远端RpcEndpoint对应的Outbox
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) { // Outbox为空
          // 新建一个Outbox并添加到outboxes字典中
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) { // 如果当前RpcEndpoint处于停止状态
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        // 则从outboxes移除对应的的消息接受者的Outbox
        outboxes.remove(receiver.address)
        // 停止该Outbox
        targetOutbox.stop()
      } else {
        // 否则使用Outbox的send()方法发送消息
        targetOutbox.send(message)
      }
    }
  }

  // 发送消息
  private[netty] def send(message: RequestMessage): Unit = {
    // 获取消息的接收者地址
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // 接收者地址与当前NettyRpcEnv的地址相同，说明处理请求的RpcEndpoint位于本地的NettyRpcEnv中
      // Message to a local RPC endpoint.
      try {
        // 由Dispatcher的postOneWayMessage()方法发送
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logWarning(e.getMessage)
      }
    } else {
      // 请求消息的接收者的地址与当前NettyRpcEnv的地址不同
      // Message to a remote RPC endpoint.
      // 将message序列化，封装为OneWayOutboxMessage类型的消息，调用postToOutbox()方法将消息投递出去。
      postToOutbox(message.receiver, OneWayOutboxMessage(serialize(message)))
    }
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    // 创建TransportClient
    clientFactory.createClient(address.host, address.port)
  }

  // 询问方法
  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()

    // 获取消息的接收者地址
    val remoteAddr = message.receiver.address

    // 失败回调
    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        logWarning(s"Ignored failure: $e")
      }
    }

    // 成功回调
    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        // 接收者地址与当前NettyRpcEnv的地址相同，说明处理请求的RpcEndpoint位于本地的NettyRpcEnv中
        // 构造Promise对象
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        // 调用Dispatcher的postLocalMessage()方法投递消息
        dispatcher.postLocalMessage(message, p)
      } else {
        // 接收者地址与当前NettyRpcEnv的地址不同，说明处理请求的RpcEndpoint位于其他节点的NettyRpcEnv中
        // 序列化消息数据，构造为RpcOutboxMessage对象，传递的失败和成功的回调都定义在上面
        val rpcMessage = RpcOutboxMessage(serialize(message),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        // 投递消息
        postToOutbox(message.receiver, rpcMessage)
        // 超时
        promise.future.onFailure {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      // 使用timeoutScheduler设置一个定时器，用于超时处理。此定时器在等待指定的超时时间后将抛出TimeoutException异常。
      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)

      // 请求如果在超时时间内处理完毕，则会调用timeoutScheduler的cancel方法取消timeoutCancelable超时定时器。
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  // 根据RpcEndpoint获取对应的RpcEndpointRef
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  override def fileServer: RpcEnvFileServer = streamManager

  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    try {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    } catch {
      case e: Exception =>
        pipe.sink().close()
        source.close()
        throw e
    }

    source
  }

  private def downloadClient(host: String, port: Int): TransportClient = {
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
        val module = "files"
        val prefix = "spark.rpc.io."

        // 克隆一份SparkConf
        val clone = conf.clone()

        // Copy any RPC configuration that is not overridden in the spark.files namespace.
        // 检查file模块一些有关RPC的配置是否缺失了，如果是将设置到克隆的SparkConf中
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }

        // 文件下载的IO线程数
        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        // 创建TransportConf
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        // 创建TransportContext
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        // 创建用于文件下载的TransportClientFactory
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
    }
    fileDownloadFactory.createClient(host, port)
  }

  private class FileDownloadChannel(source: ReadableByteChannel) extends ReadableByteChannel {

    @volatile private var error: Throwable = _

    def setError(e: Throwable): Unit = {
      error = e
      source.close()
    }

    override def read(dst: ByteBuffer): Int = {
      Try(source.read(dst)) match {
        case Success(bytesRead) => bytesRead
        case Failure(readErr) =>
          if (error != null) {
            throw error
          } else {
            throw readErr
          }
      }
    }

    override def close(): Unit = source.close()

    override def isOpen(): Boolean = source.isOpen()

  }

  private class FileDownloadCallback(
      sink: WritableByteChannel,
      source: FileDownloadChannel,
      client: TransportClient) extends StreamCallback {

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      while (buf.remaining() > 0) {
        sink.write(buf)
      }
    }

    override def onComplete(streamId: String): Unit = {
      sink.close()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      logDebug(s"Error downloading stream $streamId.", cause)
      source.setError(cause)
      sink.close()
    }

  }
}

private[netty] object NettyRpcEnv extends Logging {
  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  // 创建NettyRpcEnv
  def create(config: RpcEnvConfig): RpcEnv = {
    // 创建序列化器
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    // 通过序列化器、监听地址、安全管理器等构造NettyRpcEnv
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager)
    if (!config.clientMode) { // 如果是Driver
      // 定义启动RpcEnv的偏函数，该偏函数中会创建TransportServer
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        // 调用NettyRpcEnv的startServer()，这里会创建TransportServer
        nettyEnv.startServer(config.bindAddress, actualPort)
        // 返回NettyRpcEnv和端口
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        // 在指定端口启动NettyRpcEnv
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    // 返回NettyRpcEnv
    nettyEnv
  }
}

/**
 * The NettyRpcEnv version of RpcEndpointRef.
 *
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 *
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 *
 * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
 * a client connection, since the process hosting the endpoint is not listening for incoming
 * connections. These refs should not be shared with 3rd parties, since they will not be able to
 * send messages to the endpoint.
 *
 * @param conf Spark configuration.
 * @param endpointAddress The address where the endpoint is listening.
 * @param nettyEnv The RpcEnv associated with this ref.
 */
private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv)
  extends RpcEndpointRef(conf) with Serializable with Logging {

  // 类型为TransportClient。NettyRpcEndpointRef将利用此TransportClient向远端的RpcEndpoint发送请求。
  @transient @volatile var client: TransportClient = _

  // 远端RpcEndpoint的地址RpcEndpointAddress。
  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  // 远端RpcEndpoint的名称。
  private val _name = endpointAddress.name

  override def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = _name

  // 首先将message封装为RequestMessage，然后调用NettyRpcEnv的ask方法。
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(RequestMessage(nettyEnv.address, this, message), timeout)
  }

  // 首先将message封装为RequestMessage，然后调用NettyRpcEnv的send方法。
  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${_address})"

  def toURI: URI = new URI(_address.toString)

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()
}

/**
 * The message that is sent from the sender to the receiver.
 */
private[netty] case class RequestMessage(
    senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any)

/**
 * A response that indicates some failure happens in the receiver side.
 */
private[netty] case class RpcFailure(e: Throwable)

/**
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  // 用于跟踪远程客户端的RpcEnv地址的字典
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  /**
    * @param client A channel client which enables the handler to make requests back to the sender
    *               of this RPC. This will always be the exact same object for a particular channel.
    *               发送消息的TransportClient
    * @param message The serialized bytes of the RPC. 消息序列化后的数据
    * @param callback Callback which should be invoked exactly once upon success or failure of the RPC
    *                 用于对请求处理结束后进行回调，无论处理结果是成功还是失败，该回调都会被调用一次
    */
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    // 转换消息数据为RequestMessage对象
    val messageToDispatch = internalReceive(client, message)
    // 将消息投递到Dispatcher中对应的Inbox中
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  // 无需回复的消息
  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  // 转换消息数据为RequestMessage对象
  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    // 获取发送消息的TransportClient的InetSocketAddress，并构造为远端Client地址对象RpcAddress
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    // 反序列化消息为RequestMessage对象
    val requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
    // 检查远端Server地址是否为空
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      // 远端Server地址为空，重新构造一个RequestMessage对象，远端Server地址是TransportClient的Socket地址
      RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else { // 远端Server地址不为空
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      /**
        * 将远端节点的地址信息存入remoteAddresses字典
        *   - 键为保存远端节点Client的host和port的RpcAddress实例
        *   - 值为保存远端节点server的host和port的RpcAddress实例
        */
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        // put后返回为null即表示成功，说明这是首次与该地址通信，则向Dispatcher投递RemoteProcessConnected事件
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      // 投递RemoteProcessConnectionError事件，包含的是远端Client地址
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        // 投递RemoteProcessConnectionError事件，包含的是远端Server地址
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  // Channel激活时调用
  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    // 投递RemoteProcessConnected消息，包含的是远端Client地址
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  // Channel断开连接时调用；在当远端Client与本当前RPC环境断开连接时，还需要断开当前RPC环境到远端Server的连接
  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      // 移除对应的Outbox
      nettyEnv.removeOutbox(clientAddr)
      // 投递RemoteProcessDisconnected消息，包含的是远端Client地址
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      // 从remoteAddresses中移除
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        // 投递RemoteProcessDisconnected消息，包含的是远端Server地址
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}
