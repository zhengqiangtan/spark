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

package org.apache.spark.network.sasl;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Bootstraps a {@link TransportClient} by performing SASL authentication on the connection. The
 * server should be setup with a {@link SaslRpcHandler} with matching keys for the given appId.
 */
public class SaslClientBootstrap implements TransportClientBootstrap {
  private static final Logger logger = LoggerFactory.getLogger(SaslClientBootstrap.class);

  private final boolean encrypt;
  private final TransportConf conf;
  private final String appId;
  private final SecretKeyHolder secretKeyHolder;

  public SaslClientBootstrap(TransportConf conf, String appId, SecretKeyHolder secretKeyHolder) {
    this(conf, appId, secretKeyHolder, false);
  }

  public SaslClientBootstrap(
      TransportConf conf,
      String appId,
      SecretKeyHolder secretKeyHolder,
      boolean encrypt) {
    this.conf = conf;
    this.appId = appId;
    this.secretKeyHolder = secretKeyHolder;
    this.encrypt = encrypt;
  }

  /**
   * Performs SASL authentication by sending a token, and then proceeding with the SASL
   * challenge-response tokens until we either successfully authenticate or throw an exception
   * due to mismatch.
   */
  @Override
  public void doBootstrap(TransportClient client, Channel channel) {
    SparkSaslClient saslClient = new SparkSaslClient(appId, secretKeyHolder, encrypt);
    try {
      // 构造装载Token的载荷
      byte[] payload = saslClient.firstToken();

      while (!saslClient.isComplete()) { // 认证还未完成
        // 构造SaslMessage消息
        SaslMessage msg = new SaslMessage(appId, payload);
        // 将SaslMessage写入到ByteBuf
        ByteBuf buf = Unpooled.buffer(msg.encodedLength() + (int) msg.body().size());
        msg.encode(buf);
        buf.writeBytes(msg.body().nioByteBuffer());

        // 使用TransportClient以同步方式发送RpcRequest
        ByteBuffer response = client.sendRpcSync(buf.nioBuffer(), conf.saslRTTimeoutMs());
        // 解析响应数据
        payload = saslClient.response(JavaUtils.bufferToArray(response));
      }

      // 走到这里，说明SASL认证完成了，将appId设置到TransportClient中
      client.setClientId(appId);

      if (encrypt) { // 如果开启了传输加密
        if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslClient.getNegotiatedProperty(Sasl.QOP))) {
          throw new RuntimeException(
            new SaslException("Encryption requests by negotiated non-encrypted connection."));
        }

        // 为Channel的处理器链头部添加EncryptionHandler加密处理器、DecryptionHandler解密处理器和TransportFrameDecoder帧解码器
        SaslEncryption.addToChannel(channel, saslClient, conf.maxSaslEncryptedBlockSize());
        // 将SaslClient置空
        saslClient = null;
        logger.debug("Channel {} configured for SASL encryption.", client);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      if (saslClient != null) {
        try {
          // Once authentication is complete, the server will trust all remaining communication.
          // 认证完成后，服务端会对剩余所有的请求授信
          saslClient.dispose();
        } catch (RuntimeException e) {
          logger.error("Error while disposing SASL client", e);
        }
      }
    }
  }

}
