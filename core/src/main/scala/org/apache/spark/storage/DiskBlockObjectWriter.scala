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

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 *
  * @param file 要写入的文件。
  * @param serializerManager
  * @param serializerInstance
  * @param bufferSize 缓冲大小。
  * @param syncWrites 是否同步写。
  * @param writeMetrics 对Shuffle中间结果写入到磁盘的度量与统计。
  * @param blockId 块的唯一身份标识BlockId。
  */
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  // 是否已经初始化
  private var initialized = false
  // 是否已经打开流
  private var streamOpen = false
  // 是否已经关闭
  private var hasBeenClosed = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   *           ^          ^     ^
   *           |          |    channel.position()
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
    *
    * 提交的文件位置
   */
  private var committedPosition = file.length()
  // 报告给度量系统的文件位置
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
    *
    * 已写的记录数
   */
  private var numRecordsWritten = 0

  // 主要是创建各种流
  private def initialize(): Unit = {
    fos = new FileOutputStream(file, true)
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  // 打开要写入文件的各种输出流及管道
  def open(): DiskBlockObjectWriter = {
    // 检查状态
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      // 初始化
      initialize()
      initialized = true
    }

    // 对Block的输出流进行压缩与加密。
    bs = serializerManager.wrapStream(blockId, mcs)
    // 对压缩流进行序列化
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      mcs.manualClose()
      channel = null
      mcs = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      streamOpen = false
      hasBeenClosed = true
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * 将输出流中的数据写入到磁盘。
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      // 刷新并关闭流
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      if (syncWrites) { // 同步写出
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        // 进行同步
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      // 记录写出数据后FileChannel当前的position
      val pos = channel.position()
      // 得到本次写出数据的FileSegment对象
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      // 记录position
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      // 向度量系统记录写出的数据字节数
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      // 返回FileSegment对象
      fileSegment
    } else {
      // 流关闭了，返回长度为0的FileSegment对象
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }

      val truncateStream = new FileOutputStream(file, true)
      try {
        truncateStream.getChannel.truncate(committedPosition)
        file
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
        file
    }
  }

  /**
   * Writes a key-value pair.
    * 向输出流中写入键值对。
   */
  def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
    *
    * 对写入的记录数进行统计和度量。
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
