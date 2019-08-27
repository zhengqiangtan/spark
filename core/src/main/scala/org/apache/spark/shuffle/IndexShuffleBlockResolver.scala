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

package org.apache.spark.shuffle

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 * 特质ShuffleBlockResolver定义了对Shuffle Block进行解析的规范，
 * 包括获取Shuffle数据文件、获取Shuffle索引文件、删除指定的Shuffle数据文件和索引文件、生成Shuffle索引文件、获取Shuffle块的数据等。
 *
 * ShuffleBlockResolver目前只有IndexShuffleBlockResolver这唯一的实现类。
 * IndexShuffleBlockResolver用于创建和维护Shuffle Block与物理文件位置之间的映射关系。
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  /**
   * 即与Shuffle相关的TransportConf。
   * 用于对Shuffle客户端传输线程数（spark.shuffle.io.clientThreads属性）和
   * Shuffle服务端传输线程数（spark.shuffle.io.serverThreads属性）进行读取。
   */
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  // 用于获取Shuffle数据文件
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  // 用于获取Shuffle索引文件
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   *
   * 用于删除Shuffle过程中包含指定map任务输出数据的Shuffle数据文件和索引文件
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    // 获取指定Shuffle中指定map任务输出的数据文件
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) { // 删除数据文件
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    // 获取指定Shuffle中指定map任务输出的索引文件
    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) { // 删除索引文件
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   *
   * 检查给定的索引文件和数据文件是否能互相匹配，
   * 如果能匹配则返回数据文件的每个分区的长度组成的数组，否则返回null
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    /**
     * 检查索引文件长度，其长度需要是 (数据块数量 + 1) * 8，
     * 索引值为Long型，占8个字节，
     * 索引文件头的8个字节为标记值，即Long型整数0，不算索引值
     */
    if (index.length() != (blocks + 1) * 8) {
      return null
    }
    // 创建数据块数量大小的数组
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    // 得到索引文件的输入流
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      // 读取第一个Long型整数
      var offset = in.readLong()
      // 第一个Long型整数标记值必须为0，如果不为0，则直接返回null
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        // 读取Long型偏移量
        val off = in.readLong()
        // 记录对应的数据块长度
        lengths(i) = off - offset
        // offset更新
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    // 数据文件的长度，等于lengths数组所有元素之和，表示校验成功
    if (data.length() == lengths.sum) {
      // 返回lengths数组
      lengths
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   *
   * 用于将每个Block的偏移量写入索引文件
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    // 获取指定Shuffle中指定map任务输出的索引文件
    val indexFile = getIndexFile(shuffleId, mapId)
    // 根据索引文件获取临时索引文件的路径
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 构建临时文件的输出流
      val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
      Utils.tryWithSafeFinally {
        // We take in lengths of each block, need to convert it to offsets.
        // 写入临时索引文件的第一个标记值，即Long型整数0
        var offset = 0L
        out.writeLong(offset)
        // 遍历每个数据块的长度，并作为偏移量写入临时索引文件
        for (length <- lengths) {
          // 在原有offset上加上数据块的长度
          offset += length
          // 写入临时文件
          out.writeLong(offset)
        }
      } {
        out.close()
      }

      // 获取指定Shuffle中指定map任务输出的数据文件
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized { // 主要该步骤是加锁的
        /**
         * 检查indexFile文件、dataFile文件及数据块的长度是否，
         * 注意，这里传入的不是临时索引文件indexTmp
         * 这个操作是为了检查可能已经存在的索引文件与数据文件是否匹配，
         * 如果检查发现时匹配的，则说明有其他的TaskAttempt已经完成了该Map任务的写出，
         * 那么此时产生的临时索引文件就无用了。
         * 注意，由于当前代码块是使用synchronized同步的，因此不用担心并发问题，
         * 同一个时间点只会有一个TaskAttempt成功写出数据。
         */
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) { // 如果匹配
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          // 将checkIndexAndDataFile()方法记录的数据文件的每个分区的长度组成的数组复制到lengths数组中
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            // 将临时索引文件删除
            dataTmp.delete()
          }
          // 将临时索引文件删除
          indexTmp.delete()
        } else { // 如果不匹配，说明当前还没有其他的TaskAttempt进行索引文件的写入，本次操作产生的临时索引文件可以用
          // 将indexFile和dataFile删除
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          // 临时的索引文件和数据文件作为正式的索引文件和数据文件
          // 将indexTmp重命名为indexFile
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          // 将dataTmp重命名为dataFile
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      // 如果临时索引文件还存在，一定要将其删除
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  // 用于获取指定的ShuffleBlockId对应的数据
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    // 获取指定map任务输出的索引文件
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)
    // 读取索引文件的输入流
    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      /**
       * 跳过与当前Reduce任务无关的字节，可见，
       * 在索引文件中是按照Reduce任务ID的顺序记录每个Reduce对应的数据块的索引数据的。
       */
      ByteStreams.skipFully(in, blockId.reduceId * 8)
      // 读取偏移量
      val offset = in.readLong()
      // 读取下一个偏移量
      val nextOffset = in.readLong()
      // 构造并返回FileSegmentManagedBuffer
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        // 读取的起始偏移量为offset
        offset,
        // 读取长度为nextOffset - offset
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
