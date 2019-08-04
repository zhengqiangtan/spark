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

package org.apache.spark.network.client;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * An interceptor that is registered with the frame decoder to feed stream data to a
 * callback.
 */
class StreamInterceptor implements TransportFrameDecoder.Interceptor {

  private final TransportResponseHandler handler;
  private final String streamId;
  private final long byteCount;
  private final StreamCallback callback;
  private long bytesRead;

  StreamInterceptor(
      TransportResponseHandler handler,
      String streamId,
      long byteCount,
      StreamCallback callback) {
    this.handler = handler;
    this.streamId = streamId;
    this.byteCount = byteCount;
    this.callback = callback;
    this.bytesRead = 0;
  }

  @Override
  public void exceptionCaught(Throwable cause) throws Exception {
    handler.deactivateStream();
    callback.onFailure(streamId, cause);
  }

  @Override
  public void channelInactive() throws Exception {
    handler.deactivateStream();
    callback.onFailure(streamId, new ClosedChannelException());
  }

  @Override
  public boolean handle(ByteBuf buf) throws Exception {
    // 本次要读取的数据字节数
    int toRead = (int) Math.min(buf.readableBytes(), byteCount - bytesRead);
    // 构造buf的分片并转换为NIO的ByteBuffer，该操作会从readerIndex开始，创建长度为toRead的分片
    ByteBuffer nioBuffer = buf.readSlice(toRead).nioBuffer();

    // 获取可读字节数
    int available = nioBuffer.remaining();
    // 将数据传给StreamCallback回调对象
    callback.onData(streamId, nioBuffer);
    // 累计已读字节数
    bytesRead += available;

    // 判断读了多少
    if (bytesRead > byteCount) { // 已读字节大于响应消息指定的总字节数
      // 构造异常
      RuntimeException re = new IllegalStateException(String.format(
        "Read too many bytes? Expected %d, but read %d.", byteCount, bytesRead));
      // 将异常交给StreamCallback回调对象
      callback.onFailure(streamId, re);
      // 关闭流
      handler.deactivateStream();
      throw re;
    } else if (bytesRead == byteCount) { // 已读字节等于响应消息指定的总字节数
      // 即已经读完了，关闭流
      handler.deactivateStream();
      // 调用StreamCallback回调对象的方法告知读完了
      callback.onComplete(streamId);
    }

    // 返回值表示是否读完
    return bytesRead != byteCount;
  }

}
