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
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 *
 * ShuffleExternalSorter是专门用于对Shuffle数据进行排序的外部排序器，用于将map任务的输出存储到Tungsten中；
 * 在记录超过限制时，将数据溢出到磁盘。
 * 与ExternalSorter不同，ShuffleExternalSorter本身并没有实现数据的持久化功能，
 * 具体的持久化将由ShuffleExternalSorter的调用者UnsafeShuffleWriter来实现。
 */
final class ShuffleExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  // 磁盘写缓冲大小，即1M
  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  // 分区数量
  private final int numPartitions;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  // Task任务上下文
  private final TaskContext taskContext;

  // 对Shuffle写入（也就是map任务输出到磁盘）的度量，即Shuffle WriteMetrics。
  private final ShuffleWriteMetrics writeMetrics;

  /**
   * Force this sorter to spill when there are this many elements in memory. The default value is
   * 1024 * 1024 * 1024, which allows the maximum size of the pointer array to be 8G.
   *
   * 磁盘溢出的元素数量。
   * 可通过spark.shuffle.spill.numElementsForceSpillThreshold属性进行配置，默认为1G，10亿条。
   */
  private final long numElementsForSpillThreshold;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter
   * 创建的DiskBlockObjectWriter内部的文件缓冲大小。
   * 可通过spark.shuffle.file.buffer属性进行配置，默认是32KB。
   */
  private final int fileBufferSizeBytes;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   *
   * 已经分配的Page（即MemoryBlock）列表
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  // 溢出文件的元数据信息的列表
  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes.
   * 内存中数据结构大小的峰值（单位为字节）。
   */
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  // 用于在内存中对插入的记录进行排序。
  @Nullable private ShuffleInMemorySorter inMemSorter;
  // 当前的Page（即MemoryBlock）。
  @Nullable private MemoryBlock currentPage = null;
  // Page的光标。实际为用于向Tungsten写入数据时的地址信息。
  private long pageCursor = -1;

  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetrics writeMetrics) {
    // Tungsten内存模式是通过MemoryManager获取的
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    // 文件缓冲区大小，默认为32M
    this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    // 溢写阈值，默认为1G，即，当数据条数超过该值时，会溢写到磁盘
    this.numElementsForSpillThreshold =
      conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", 1024 * 1024 * 1024);
    this.writeMetrics = writeMetrics;
    // 内存排序器，spark.shuffle.sort.useRadixSort参数用于指定是否使用基数排序，默认为true，否则使用普通的TimSort排序
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
    // 更新内存使用峰值记录
    this.peakMemoryUsedBytes = getMemoryUsage();
  }

  /**
   * Sorts the in-memory records and writes the sorted records to an on-disk file.
   * This method does not free the sort data structures.
   *
   * 将内存中的索引记录进行排序，然后根据排序后的索引值顺序取得Map任务输出的数据，顺序写入磁盘文件。
   *
   * @param isLastFile if true, this indicates that we're writing the final output file and that the
   *                   bytes written should be counted towards shuffle spill metrics rather than
   *                   shuffle write metrics.
   *                   如果为true表示写的是最后一个输出文件
   */
  private void writeSortedFile(boolean isLastFile) throws IOException {

    // 度量相关
    final ShuffleWriteMetrics writeMetricsToUse;

    if (isLastFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      // 最后一次写文件操作，需要计算本次的写出数据作为Shuffle操作的数据量
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // This call performs the actual sort.
    // 获取基于内存的Shuffle排序迭代器
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array. This array does not need to be large enough to hold a single
    // record;
    // 磁盘写缓冲，1MB
    final byte[] writeBuffer = new byte[DISK_WRITE_BUFFER_SIZE];

    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    // 在磁盘上创建唯一的TempShuffleBlockId和对应的文件，作为溢出操作的数据存储文件。
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    // 获取文件对象
    final File file = spilledFileInfo._2();
    // 获取对应的BlockId
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    // 将分区数、文件对象和BlockId封装为SpillInfo溢写信息对象
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    // 获取DiskBlockObjectWriter
    final DiskBlockObjectWriter writer =
      blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);

    int currentPartition = -1;
    // 如果还有经过排序的索引记录
    while (sortedRecords.hasNext()) {
      // 取出下一条索引记录
      sortedRecords.loadNext();
      // 获取索引中记录的分区号
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      // 分区号需要大于currentPartition，以递增写入
      assert (partition >= currentPartition);
      // 检查分区号是否与当前的相同
      if (partition != currentPartition) {
        // Switch to the new partition
        // 不相同，且currentPartition不为-1，则需要转换为新分区
        if (currentPartition != -1) {
          /**
           * 先将当前的缓冲流中的数据写出到磁盘，返回的是当前写出操作的FileSegment
           * FileSegment对象包含了当前写出数据的文件、偏移量和长度
           */
          final FileSegment fileSegment = writer.commitAndGet();
          // 记录当前分区的数据长度
          spillInfo.partitionLengths[currentPartition] = fileSegment.length();
        }
        // 更新currentPartition为下一个分区号
        currentPartition = partition;
      }

      // 获取当前记录的PackedRecordPointer指针中的Map任务输出数据存储的页号和偏移量
      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      // 从TaskMemoryManager中获取Map任务输出数据在内存中对应的页和偏移量
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      // 读取4字节的Int值，作为dataRemaining
      int dataRemaining = Platform.getInt(recordPage, recordOffsetInPage);
      // 跳过4字节的Int值，即跳过dataRemaining的长度
      long recordReadPosition = recordOffsetInPage + 4; // skip over record length
      // 当还有剩余数据时
      while (dataRemaining > 0) {
        // 计算需要进行transfer的数据，大小不能超过DISK_WRITE_BUFFER_SIZE（1MB）
        final int toTransfer = Math.min(DISK_WRITE_BUFFER_SIZE, dataRemaining);
        // 将数据拷贝到writeBuffer
        Platform.copyMemory(
          recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        // 将writeBuffer的数据通过DiskBlockObjectWriter先写出到缓冲区
        writer.write(writeBuffer, 0, toTransfer);
        // 更新偏移量
        recordReadPosition += toTransfer;
        // 更新剩余数据量
        dataRemaining -= toTransfer;
      }
      // 对写入的记录数进行统计和度量
      writer.recordWritten();
    }

    // Map任务输出数据已写完，将缓冲区中的数据写出到磁盘
    final FileSegment committedSegment = writer.commitAndGet();
    // 关闭写出器
    writer.close();
    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    // 检查currentPartition是否为-1，如果不为-1，说明是有数据写出的
    if (currentPartition != -1) {
      // 更新最后一个写出分区的数据长度
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      // 将spillInfo记录到spills数组
      spills.add(spillInfo);
    }

    if (!isLastFile) {  // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
      // 更新度量数据
      writeMetrics.incRecordsWritten(writeMetricsToUse.recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.bytesWritten());
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   *
   * 当内存紧张时将数据溢出到磁盘。
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spills.size(),
      spills.size() > 1 ? " times" : " time");

    /**
     * 将内存中的记录进行排序后输出到磁盘。这里用到的排序方式有两种：
     * 1. 对分区ID进行比较的排序；
     * 2. 默认采用了基数排序（Radix Sort）。
     * 3. 如果没有开启基数排序则使用TimSort排序。
     */
    writeSortedFile(false);
    // 将所使用的Page（即MemoryBlock）全部释放
    final long spillSize = freeMemory();
    // 重置ShuffleMemorySorter底层的长整型数组，便于下次排序
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    // 更新任务度量信息
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    // 返回溢出的数据大小
    return spillSize;
  }

  // 获取内存使用情况
  private long getMemoryUsage() {
    long totalPageSize = 0;
    // 遍历所有的MemoryBlock统计它们的页大小
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    // 需要加上内存排序器使用的内存大小
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  // 更新内存使用的峰值
  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    // 只有当计算出的内存使用大小大于当前峰值时才会更新
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   * 获取内存使用峰值
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  // 释放内存
  private long freeMemory() {
    // 更新内存使用峰值
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    // 遍历所有的MemoryBlock
    for (MemoryBlock block : allocatedPages) {
      // 累计释放的内存大小
      memoryFreed += block.size();
      // 使用freeBlock()方法释放
      freePage(block);
    }
    // 清空保存MemoryBlock的列表
    allocatedPages.clear();
    // 清空当前使用的MemoryBlock页
    currentPage = null;
    // 清空页游标
    pageCursor = 0;
    // 返回释放的内存总大小
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    // 清空内存
    freeMemory();
    if (inMemSorter != null) {
      // 释放内存排序器占用的内存
      inMemSorter.free();
      inMemSorter = null;
    }
    // 删除所有的溢写文件
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   *
   * 检查内存排序器是否有足够的空间以插入记录，如果空间不足则进行扩容。
   * 如果无法进行扩容，则将数据溢写到磁盘。
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    // 没有剩余空间了
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      // 获取内存排序器当前使用的内存大小
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        /**
         * 尝试进行扩容，扩容为原来的两倍，溢写操作隐藏在该步骤中。
         * 在TaskMemoryManager申请内存无法满足时，会调用MemoryConsumer的spill()方法尝试溢写。
         */
        array = allocateArray(used / 8 * 2);
      } catch (OutOfMemoryError e) { // 产生异常说明内存不足，无法满足申请
        // should have trigger spilling
        /**
         * 申请内存时虽然出现OutOfMemoryError错误，但由于进行了溢写操作，
         * 因此当前的MemoryConsumer有可能也发生了溢写，在当前MemoryConsumer出现溢写后，
         * 会重置调用内存排序器的reset()方法重置，此操作会刷新可使用的内存大小，
         * 如果内存排序器此时还是没有可用的内存，则抛出异常。
         * 详见 {@link #spill(long, MemoryConsumer) } 方法
         */
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      // 检查是否进行了溢写
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        // 进行了溢写，释放扩容时申请的空间
        freeArray(array);
      } else {
        // 没有进行溢写，将申请的扩容空间设置为内存排序器的内存空间
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    /**
     * 1. 当前页currentPage为null，则需要申请第一个MemoryBlock作为当前页；
     * 2. 当前页currentPage不为null，但当前页的游标加上需要增加的偏移量大于当前页的总大小
     *    说明当前页无法满足需要的空间，则申请一个新的页。
     */
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      // 申请新的MemoryBlock作为currentPage，大小为required
      currentPage = allocatePage(required);
      // 将游标pageCursor指向currentPage的baseOffset
      pageCursor = currentPage.getBaseOffset();
      // 将新申请的页放入allocatedPages进行管理
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the shuffle sorter.
   *
   * map任务在执行结束后会将数据写入磁盘，等待reduce任务获取。
   * 在写入磁盘之前，Spark可能会对map任务的输出在内存中进行一些排序和聚合。
   * insertRecord()方法是这一过程的入口
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // for tests
    assert(inMemSorter != null);
    // 判断ShuffleInMemorySorter中的记录数大于等于numElementsForSpillThreshold，10亿条
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      // 如果是，则将数据溢出到磁盘
      spill();
    }

    /**
     * 检查是否有足够的空间将额外的记录插入到排序指针数组中，如果需要额外的空间，则增加数组的容量；
     * 如果无法获取所需的空间，则内存中的数据将被溢出到磁盘。
     */
    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int required = length + 4; // 4字节用于在内存中存储数据的长度

    // 检查是否有足够的空间，如果需要额外的空间，则申请分配新的Page
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    // 返回页号和相对于内存块起始地址的偏移量（64位长整型）。
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);

    // 向Page所代表的内存块的起始地址写入数据的长度
    Platform.putInt(base, pageCursor, length);
    pageCursor += 4;

    // 将记录数据拷贝到Page中
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;

    /**
     * 将记录的元数据信息存储到内部用的长整型数组中，以便于排序。
     * 其中高24位存储分区ID，中间13位存储页号，低27位存储偏移量。
     */
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * 关闭排序器，这个操作会触发对排序器中缓存的数据进行排序并将它们溢写到磁盘。
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    try {
      if (inMemSorter != null) {
        // Do not count the final file towards the spill count.
        // 传入的isLastFile为true
        writeSortedFile(true);
        // 释放内存
        freeMemory();
        // 释放排序器内存
        inMemSorter.free();
        inMemSorter = null;
      }
      // 将spills数组重新构建为一个新的SpillInfo数组并返回
      return spills.toArray(new SpillInfo[spills.size()]);
    } catch (IOException e) {
      // 出现异常则清理资源并抛出异常
      cleanupResources();
      throw e;
    }
  }

}
