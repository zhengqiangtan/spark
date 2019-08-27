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

package org.apache.spark.network.buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import io.netty.channel.DefaultFileRegion;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 */
public final class FileSegmentManagedBuffer extends ManagedBuffer {
  // TransportConf配置对象
  private final TransportConf conf;
  // 所需要读取的文件
  private final File file;
  // 所要读取文件的偏移量
  private final long offset;
  // 所要读取文件的长度
  private final long length;

  public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
    this.conf = conf;
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long size() {
    return length;
  }

  // 以NIO ByteBuffer方式读取
  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    try {
      // 构造RAF并得到其FileChannel
      channel = new RandomAccessFile(file, "r").getChannel();
      // Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
      // 判断读取长度是否小于spark.storage.memoryMapThreshold参数指定的长度
      if (length < conf.memoryMapBytes()) { // 由spark.storage.memoryMapThreshold配置参数决定
        // 分配对应大小的ByteBuffer
        ByteBuffer buf = ByteBuffer.allocate((int) length);
        // 定位到偏移量
        channel.position(offset);
        // 进行读取
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) { // 读取到-1说明读完了
            throw new IOException(String.format("Reached EOF before filling buffer\n" +
                            "offset=%s\nfile=%s\nbuf.remaining=%s",
                    offset, file.getAbsoluteFile(), buf.remaining()));
          }
        }
        // 切换buffer为写模式并返回
        buf.flip();
        return buf;
      } else {
        // 否则返回对应的MappedByteBuffer映射缓冲对象，并没有实际的读取
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
      }
    } catch (IOException e) {
      try {
        if (channel != null) {
          long size = channel.size();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                  e);
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException("Error in opening " + this, e);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  // 以文件流方式读取
  @Override
  public InputStream createInputStream() throws IOException {
    FileInputStream is = null;
    try {
      // 根据文件创建FileInputStream
      is = new FileInputStream(file);
      // 定位到偏移量
      ByteStreams.skipFully(is, offset);
      // 返回LimitedInputStream流对象
      return new LimitedInputStream(is, length);
    } catch (IOException e) {
      try {
        if (is != null) {
          long size = file.length();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
              e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(is);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(is);
      throw e;
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  // 将缓冲区的数据转换为Netty的对象，用来将数据写到外部
  @Override
  public Object convertToNetty() throws IOException {
    if (conf.lazyFileDescriptor()) { // 以懒加载的方式初始化FileDescriptor
      // 传入的是File对象，并没有打开文件
      return new DefaultFileRegion(file, offset, length);
    } else { // 不以懒加载的方式初始化FileDescriptor
      FileChannel fileChannel = new FileInputStream(file).getChannel();
      // 传入的是FileChannel对象，此时文件已经被打开了
      return new DefaultFileRegion(fileChannel, offset, length);
    }
  }

  public File getFile() { return file; }

  public long getOffset() { return offset; }

  public long getLength() { return length; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("file", file)
      .add("offset", offset)
      .add("length", length)
      .toString();
  }
}
