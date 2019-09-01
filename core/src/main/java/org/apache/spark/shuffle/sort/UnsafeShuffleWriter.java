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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

/**
 * 底层使用ShuffleExternalSorter作为外部排序器，
 * UnsafeShuffleWriter不具备SortShuffleWriter的聚合功能。
 * UnsafeShuffleWriter将使用Tungsten的内存作为缓存，以提高写入磁盘的性能。
 *
 * SortShuffleWriter底层的PartitionedPairBuffer使用的是JVM的内存，
 * 而UnsafeShuffleWriter使用的则是Tungsten（既有可能是JVM内存，也有可能是操作系统内存）。
 */
@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  // 默认的排序缓冲区初始化大小
  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;

  private final BlockManager blockManager;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  // 对Shuffle写入（也就是map任务输出到磁盘）的度量，即ShuffleWrite Metrics。
  private final ShuffleWriteMetrics writeMetrics;
  // Shuffle的唯一标识
  private final int shuffleId;
  // Map任务的身份标识
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  // 是否采用NIO的从文件流到文件流的复制方式，可以通过spark.file.transferTo属性配置，默认为true
  private final boolean transferToEnabled;
  // 初始化时的排序缓冲大小，可以通过spark.shuffle.sort.initialBufferSize属性配置，默认为4096
  private final int initialSortBufferSize;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  // 使用内存的峰值（单位为字节）
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  // ByteArrayOutputStream的子类，提供了暴露ByteArrayOutputStream内部存储数据的字节数组buf的getBuf()方法。
  private MyByteArrayOutputStream serBuffer;
  // 将serBuffer包装为SerializationStream后的对象。
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   *
   * 标记UnsafeShuffleWriter是否正在停止中。
   */
  private boolean stopping = false;

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) throws IOException {
    /**
     * 由于UnsafeShuffleWriter是SerializedShuffleHandle对应的序列化模式下的ShuffleWriter，
     * 所以需要检查分区器的分区总数，不可大于16777216
     */
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      // 分区总数大于16777216，抛异常
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    // 用于操作索引文件与数据文件
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    // 序列化器
    this.serializer = dep.serializer().newInstance();
    // 分区器
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    // 是否允许transferTo操作，默认允许
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    // 初始状态的排序缓冲区大小，默认为4096字节，即4K
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
                                                  DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    // 调用open()方法
    open();
  }

  // 更新内存使用峰值
  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      // 使用ShuffleExternalSorter的getPeakMemoryUsedBytes()获取
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   * 获取内存使用峰值
   */
  public long getPeakMemoryUsedBytes() {
    // 先更新
    updatePeakMemoryUsed();
    // 再获取
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   * 写出方法，该方法仅用于测试
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  // 将map任务的输出结果写到磁盘
  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      // 迭代输入的每条记录
      while (records.hasNext()) {
        // 将记录插入排序器
        insertRecordIntoSorter(records.next());
      }
      // 将map任务输出的数据持久化到磁盘
      closeAndWriteOutput();
      success = true;
    } finally {
      // 写出完成后，需要使用ShuffleExternalSorter进行资源清理
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() throws IOException {
    assert (sorter == null);
    // 创建外部Shuffle排序器
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      // 初始化缓冲大小
      initialSortBufferSize,
      // 分区总数
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    // 序列化缓冲
    serBuffer = new MyByteArrayOutputStream(1024 * 1024);
    // 包装为序列化流对象
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  // 将map任务输出的数据持久化到磁盘
  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    // 更新使用内存的峰值
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    // 关闭ShuffleExternalSorter，获得溢出文件信息的数组
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    // 将ShuffleExternalSorter置为null
    sorter = null;
    final long[] partitionLengths;
    // 获取正式的输出数据文件
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 创建临时文件
    final File tmp = Utils.tempFileWith(output);
    try {
      try {
        // 合并所有溢出文件到正式的输出数据文件，返回的是每个分区的数据长度
        partitionLengths = mergeSpills(spills, tmp);
      } finally {
        // 因为合并成了一个文件，因此删除溢写的文件
        for (SpillInfo spill : spills) {
          if (spill.file.exists() && ! spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }

      // 根据partitionLengths数组创建索引文件
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    // 构造并返回MapStatus对象
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  // 将记录插入到排序器
  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    // 计算记录的分区ID
    final int partitionId = partitioner.getPartition(key);
    // 重置serBuffer
    serBuffer.reset();
    // 将记录写入到serOutputStream中进行序列化
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    // 得到序列化后的数据大小
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    // 将serBuffer底层的序列化字节数组插入到Tungsten的内存中
    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  // 强制ShuffleExternalSorter溢写
  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
    // 是否开启了解压缩，由spark.shuffle.compress参数配置，默认为true
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    // 压缩编解码器
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // 是否开启了快速合并，由spark.shuffle.unsafe.fastMergeEnabled参数配置，默认为true
    final boolean fastMergeEnabled =
      sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
    /**
     * 检查是否支持快速合并：
     * 1. 没有开启压缩，则可以快速合并；
     * 2. 开启了压缩，但需要压缩编解码器支持对级联序列化流进行解压缩。
     *    支持该功能的压缩编解码有Snappy、LZ4、LZF三种。
     */
    final boolean fastMergeIsSupported = !compressionEnabled ||
      CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 是否开启了数据加解密
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    try {
      if (spills.length == 0) { // 没有溢写文件
        // 创建空文件
        new FileOutputStream(outputFile).close(); // Create an empty file
        // 返回分区总数大小的空的long数组
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) { // 有一个溢写文件
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        // 直接将该文件重命名为outputFile文件
        Files.move(spills[0].file, outputFile);
        // 返回分区的数据长度数组
        return spills[0].partitionLengths;
      } else { // 有多个溢写文件
        // 用于存放每个分区对应的输出数据的长度的数组
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        if (fastMergeEnabled && fastMergeIsSupported) { // 开启并支持快速合并
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) { // 开启了NIO复制方式，且未开启加解密
            logger.debug("Using transferTo-based fast merge");
            // 使用mergeSpillsWithTransferTo()方法的transferTo-based fast方式进行合并
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            // 使用mergeSpillsWithFileStream()方法的fileStream-based fast方式进行合并，压缩编解码器传null
            logger.debug("Using fileStream-based fast merge");
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          // 使用mergeSpillsWithFileStream()方法的slow方式进行合并，传递了压缩编解码器
          logger.debug("Using slow merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        // 记录度量信息
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is slower than the NIO-based merge,
   * {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[], File)}, so it's only used in
   * cases where the IO compression codec does not support concatenation of compressed data, when
   * encryption is enabled, or when users have explicitly disabled use of {@code transferTo} in
   * order to work around kernel bugs.
   *
   * 使用文件流的形式合并溢写文件，这种方式比NIO TransferTo的方式要慢。
   * 满足以下三种情况之一会使用该方式：
   * 1. 需要支持加解密功能。
   * 2. 压缩编解码器不支持对级联序列化流进行解压缩。
   * 3. 当开发者自行禁用了transferTo的功能。
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      SpillInfo[] spills,
      File outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);
    // 分区总数
    final int numPartitions = partitioner.numPartitions();
    // 创建存储分区数据长度的数组
    final long[] partitionLengths = new long[numPartitions];
    // 创建保存溢写文件输入流的数组
    final InputStream[] spillInputStreams = new FileInputStream[spills.length];

    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    // 创建合并输出流，该流对FileOutputStream进行了包装，提供了字节计数功能
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(
      new FileOutputStream(outputFile));

    boolean threwException = true;
    try {
      // 遍历所有的溢写文件信息对象SpillInfo，为每个溢写文件创建FileInputStream
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new FileInputStream(spills[i].file);
      }

      // 遍历分区号
      for (int partition = 0; partition < numPartitions; partition++) {
        // 从mergedFileOutputStream中获取当前已合并的字节数
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() calls, so that we can close the higher
        // level streams to make sure all data is really flushed and internal state is cleaned.
        /**
         * 再次进行包装，得到针对当前分区号的输出流
         * 1. TimeTrackingOutputStream的包装提供了写出时时间记录功能。
         * 2. CloseShieldOutputStream的包装了close()方法，屏蔽了对被包装流的关闭操作。
         */
        OutputStream partitionOutput = new CloseShieldOutputStream(
          new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        // 加解密包装
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        // 解压缩包装
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        /**
         * 遍历溢写文件信息对象SpillInfo
         * 由于每个溢写文件中包含了多个分区的数据，因此需要遍历每个溢写文件，
         * 并得到每个溢写文件中记录的对应分区的数据
         */
        for (int i = 0; i < spills.length; i++) {
          // 获取每个溢写文件中记录的对应分区（即外层for循环中循环到的partition分区）的数据大小
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) { // 分区溢写数据大于0
            // 将当前溢写文件的输入流，根据对应分区的数据大小包装为LimitedInputStream流
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
              partitionLengthInSpill, false);
            try {
              // 加解密包装
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                partitionInputStream);
              // 解压缩包装
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              // 将数据拷贝到partitionOutput
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              // 关闭LimitedInputStream流，但不会关闭底层被包装的流
              partitionInputStream.close();
            }
          }
        }
        // 刷新数据
        partitionOutput.flush();
        partitionOutput.close();
        // 记录分区数据长度
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      // 关闭溢写文件的输入流
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      // 关闭合并文件的输出流
      Closeables.close(mergedFileOutputStream, threwException);
    }
    // 返回存放了每个分区的数据长度的数组
    return partitionLengths;
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * 使用NIO TransferTo来合并溢写文件每个分区的数据。
   * 只有在压缩编解码及序列化器支持对级联序列化流进行解压缩时才可以使用。
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    // 溢写文件数需大于等于2
    assert (spills.length >= 2);
    // 获取总分区数
    final int numPartitions = partitioner.numPartitions();
    // 创建存储分区数据大小的数组
    final long[] partitionLengths = new long[numPartitions];
    // 创建FileChannel数组，用于存放每个溢写文件的FileChannel对象
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    // 创建存放每个溢写文件的FileChannel的position的数组
    final long[] spillInputChannelPositions = new long[spills.length];
    // 合并输出文件的FileChannel
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      // 获取每个溢写文件的FileChannel，存放到spillInputChannels数组
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      // 获取合并输出文件的FileChannel
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      // 传输总字节数
      long bytesWrittenToMergedFile = 0;
      // 遍历分区编号
      for (int partition = 0; partition < numPartitions; partition++) {
        // 遍历溢写文件的SpillInfo对象
        for (int i = 0; i < spills.length; i++) {
          // 获取溢写文件信息SpillInfo对象中记录的对应分区（即partition分区）的数据长度
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          // 需要TransferTo的字节数
          long bytesToTransfer = partitionLengthInSpill;
          // 获取对应溢写文件的FileChannel
          final FileChannel spillInputChannel = spillInputChannels[i];
          // 开始时间
          final long writeStartTime = System.nanoTime();
          // 当还有需要TransferTo的数据
          while (bytesToTransfer > 0) {
            // 使用FileChannel的transferTo()方法将数据从溢写文件的FileChannel传输到合并文件的FileChannel，
            final long actualBytesTransferred = spillInputChannel.transferTo(
              spillInputChannelPositions[i],
              bytesToTransfer,
              mergedFileOutputChannel);
            // 更新该溢写文件的position记录
            spillInputChannelPositions[i] += actualBytesTransferred;
            // 更新剩余字节数记录
            bytesToTransfer -= actualBytesTransferred;
          }
          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          // 一个溢写文件传输完成，更新传输字节计数
          bytesWrittenToMergedFile += partitionLengthInSpill;
          // 更新partition分区本次从溢写文件传输的字节数
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      // 检查合并输出文件的position是否等于合并的总字节数
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      // 检查每个溢写文件的FileChannel的position是否与文件的大小相同
      for (int i = 0; i < spills.length; i++) {
        // 检查每个溢写文件的FileChannel的position是否与文件的大小相同
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        // 关闭溢写文件的FileChannel
        Closeables.close(spillInputChannels[i], threwException);
      }
      // 关闭合并文件的FileChannel
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    // 返回存放了每个分区的数据长度的数组
    return partitionLengths;
  }

  // 关闭操作，会返回MapStatus对象
  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      // 度量记录内存使用峰值
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) { // 已关闭
        // 返回空
        return Option.apply(null);
      } else {
        // 标记stopping为true
        stopping = true;
        if (success) { // Shuffle输出是否成功
          // 如果成功，但mapStatus为null，则抛出异常
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          // 否则返回mapStatus
          return Option.apply(mapStatus);
        } else {
          // 输出不成功，返回null
          return Option.apply(null);
        }
      }
    } finally {
      // 关闭ShuffleExternalSorter
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
