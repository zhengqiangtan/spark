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

import io.netty.buffer.ByteBuf;

/**
 * Interface for an object which can be encoded into a ByteBuf. Multiple Encodable objects are
 * stored in a single, pre-allocated ByteBuf, so Encodables must also provide their length.
 *
 * Encodable objects should provide a static "decode(ByteBuf)" method which is invoked by
 * {@link MessageDecoder}. During decoding, if the object uses the ByteBuf as its data (rather than
 * just copying data from it), then you must retain() the ByteBuf.
 *
 * Additionally, when adding a new Encodable Message, add it to {@link Message.Type}.
 *
 * 实现该接口的类可以转换到一个ByteBuf中
 */
public interface Encodable {
  /**
   * Number of bytes of the encoded form of this object.
   * 编码的字节数长度
   **/
  int encodedLength();

  /**
   * Serializes this object by writing into the given ByteBuf.
   * This method must write exactly encodedLength() bytes.
   * 编码方法，会将当前对象存入到buf中
   */
  void encode(ByteBuf buf);
}
