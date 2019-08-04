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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 *
 * 在将消息放入管道前，先对消息内容进行编码，防止管道另一端读取时丢包和解析错误。
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private static final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    // 用于存放消息体
    Object body = null;
    // 用于记录消息体长度
    long bodyLength = 0;
    // 用于记录消息体是否包含在消息的同一帧中
    boolean isBodyInFrame = false;

    // If the message has a body, take it out to enable zero-copy transfer for the payload.
    if (in.body() != null) { // 在消息体不为null时
      try {
        // 读消息体大小
        bodyLength = in.body().size();
        // 读消息体，返回值为io.netty.buffer.ByteBuf和io.netty.channel.FileRegion其中一种。
        body = in.body().convertToNetty();
        // 读消息体是否包含在消息的同一帧中的标记
        isBodyInFrame = in.isBodyInFrame();
      } catch (Exception e) { // 遇见异常
        // 释放消息体
        in.body().release();
        if (in instanceof AbstractResponseMessage) {
          AbstractResponseMessage resp = (AbstractResponseMessage) in;
          // Re-encode this message as a failure response.
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error(String.format("Error processing %s for client %s",
            in, ctx.channel().remoteAddress()), e);
          encode(ctx, resp.createFailureResponse(error), out);
        } else {
          throw e;
        }
        return;
      }
    }

    // 读取消息类型
    Message.Type msgType = in.type();
    logger.trace(">>> Encode Message.Type " + msgType.name());
    // All messages have the frame length, message type, and message itself. The frame length
    // may optionally include the length of the body data, depending on what message is being
    // sent.
    /**
     * 计算消息头长度：表示帧大小的8字节 + 消息类型编码后的长度 + 消息编码后的长度
     */
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
    // 计算帧大小
    long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
    // 存放消息头的ByteBuf
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    // 写入帧大小
    header.writeLong(frameLength);
    // 写入消息类型
    msgType.encode(header);
    /**
     * 写入消息体相关信息，这个方法在每种消息的实现是不一样的。例如：
     * OneWayMessage只写入了消息体大小，
     * RpcRequest消息写入了Request ID和消息体大小，
     * StreamRequest消息写入了Stream ID。
     */
    in.encode(header);

    // 检查消息头是否合法
    assert header.writableBytes() == 0;

    if (body != null) {
      // We transfer ownership of the reference on in.body() to MessageWithHeader.
      // This reference will be freed when MessageWithHeader.deallocate() is called.
      // 消息体不为空，构建一个MessageWithHeader对象，保存了消息体、消息头、消息体大小、消息头大小
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      // 消息体为空，只保存消息头
      out.add(header);
    }
  }

}
