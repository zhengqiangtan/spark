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

package org.apache.spark.memory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * 2^32 * 8 bytes, which is
 * approximately 35 terabytes of memory.
 */
public class TaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /** The number of bits used to address the page table.
   * 用于寻址Page表的位数。静态常量PAGE_NUMBER_BITS的值为13。
   * 在64位的长整型中将使用高位的13位存储页号。
   **/
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of bits used to encode offsets in data pages.
   * 用于保存编码后的偏移量的位数。静态常量OFFSET_BITS的值为51。
   * 在64位的长整型中将使用低位的51位存储偏移量。
   */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** The number of entries in the page table.
   * Page表中的Page数量。静态常量PAGE_TABLE_SIZE的值为8192，
   * 实际是将1向左位移13（即PAGE_NUMBER_BITS）位所得的值。
   */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
   * maximum page size is limited by the maximum amount of data that can be stored in a long[]
   * array, which is (2^32 - 1) * 8 bytes (or 16 gigabytes). Therefore, we cap this at 16 gigabytes.
   *
   * 最大的Page大小。静态常量MAXIMUM_PAGE_SIZE_BYTES的值为17179869176，即（2^32-1）× 8。
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** Bit mask for the lower 51 bits of a long.
   * 长整型的低51位的位掩码。静态常量MASK_LONG_LOWER_51_BITS的值为2251799813685247，
   * 即十六进制0x7FFFFFFFFFFFFL，
   * 二进制0000 0000 0000 0111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
   */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   *
   * Page表。pageTable实际为Page（即MemoryBlock）的数组，数组长度为PAGE_TABLE_SIZE。
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   *
   * 用于跟踪空闲Page的BitSet
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private final MemoryManager memoryManager;

  // TaskMemoryManager所管理TaskAttempt的身份标识
  private final long taskAttemptId;

  /**
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   *
   * Tungsten的内存模式。TaskMemoryManager的getTungstenMemoryMode()方法专门用于返回tungstenMemoryMode的值。
   */
  final MemoryMode tungstenMemoryMode;

  /**
   * Tracks spillable memory consumers.
   *
   * 用于跟踪可溢出的内存消费者
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * The amount of memory that is acquired but not used.
   *
   * TaskAttempt已经获得但是并未使用的内存大小
   */
  private volatile long acquiredButNotUsed = 0L;

  /**
   * Construct a new TaskMemoryManager.
   */
  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
   * spill() of consumers to release more memory.
   *
   * 为内存消费者获得指定大小（单位为字节）的内存。
   * 当Task没有足够的内存时，将调用MemoryConsumer的spill方法释放内存。
   *
   * @return number of bytes successfully granted (<= N).
   */
  public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
    // 检查申请大小和发出申请的消费者
    assert(required >= 0);
    assert(consumer != null);
    // 获取申请的内存模式
    MemoryMode mode = consumer.getMode();
    // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
    // memory here, then it may not make sense to spill since that would only end up freeing
    // off-heap memory. This is subject to change, though, so it may be risky to make this
    // optimization now in case we forget to undo it late when making changes.
    synchronized (this) {
      // 为当前的TaskAttempt按照指定的存储模式获取指定大小的内存
      long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

      // Try to release memory from other consumers first, then we can reduce the frequency of
      // spilling, avoid to have too many spilled files.
      if (got < required) { // 逻辑上已经获得的内存未达到期望的内存大小
        // Call spill() on other consumers to release memory
        // 遍历consumers中与指定内存模式相同且已经使用了内存的MemoryConsumer，尝试溢写以空闲一部分内存
        for (MemoryConsumer c: consumers) {
          // 前置条件：不能是当前发出申请的消费者，且已使用内存要大于0，同时内存模式与申请的一样
          if (c != consumer && c.getUsed() > 0 && c.getMode() == mode) {
            try {
              // 调用MemoryConsumer的spill()方法尝试溢出数据到磁盘，以释放内存，为当前的TaskAttempt腾出内存
              long released = c.spill(required - got, consumer);
              if (released > 0) { // MemoryConsumer释放了内存空间
                logger.debug("Task {} released {} from {} for {}", taskAttemptId,
                  Utils.bytesToString(released), c, consumer);
                // 溢写成功，再次尝试为当前MemoryConsumer从MemoryManager申请不够的那部分内存
                got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
                if (got >= required) { // 已经获得的内存达到期望的内存大小，则直接结束溢出操作
                  break;
                }
              }
            } catch (IOException e) {
              logger.error("error while calling spill() on " + c, e);
              throw new OutOfMemoryError("error while calling spill() on " + c + " : "
                + e.getMessage());
            }
          }
        }
      }

      // call spill() on itself
     /**
      * 走到这里，说明已经对其他MemoryConsumer尝试了溢写操作了，
      * 如果申请的内存还不够，那么只能让当前申请内存的MemoryConsumer尝试溢写以空闲一部分内存了
      */
      if (got < required) { // 已经获得的内存还未达到期望的内存大小
        try {
          // 让当前申请内存的MemoryConsumer尝试将数据溢出到磁盘以释放内存
          long released = consumer.spill(required - got, consumer);
          if (released > 0) { // 获取内存的最后尝试
            logger.debug("Task {} released {} from itself ({})", taskAttemptId,
              Utils.bytesToString(released), consumer);
            // 做最后的尝试
            got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
          }
        } catch (IOException e) {
          logger.error("error while calling spill() on " + consumer, e);
          throw new OutOfMemoryError("error while calling spill() on " + consumer + " : "
            + e.getMessage());
        }
      }
      // 将当前申请获得内存的MemoryConsumer添加到consumers中
      consumers.add(consumer);
      logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got), consumer);
      // 返回最终获得的内存大小
      return got;
    }
  }

  /**
   * Release N bytes of execution memory for a MemoryConsumer.
   *
   * 为内存消费者释放指定大小（单位为字节）的内存。
   */
  public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
    logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size), consumer);
    memoryManager.releaseExecutionMemory(size, taskAttemptId, consumer.getMode());
  }

  /**
   * Dump the memory usage of all consumers.
   *
   * 用于将TaskAttempt、各个MemoryConsumer及MemoryManager管理的执行内存和存储内存的使用情况打印到日志。
   */
  public void showMemoryUsage() {
    logger.info("Memory used in task " + taskAttemptId);
    synchronized (this) {
      long memoryAccountedForByConsumers = 0;
      for (MemoryConsumer c: consumers) {
        long totalMemUsage = c.getUsed();
        memoryAccountedForByConsumers += totalMemUsage;
        if (totalMemUsage > 0) {
          logger.info("Acquired by " + c + ": " + Utils.bytesToString(totalMemUsage));
        }
      }
      long memoryNotAccountedFor =
        memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
      logger.info(
        "{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, taskAttemptId);
      logger.info(
        "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
        memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
    }
  }

  /**
   * Return the page size in bytes.
   *
   * 用于获得Page的大小（单位为字节）。其实际为MemoryManager的pageSizeBytes属性。
   */
  public long pageSizeBytes() {
    return memoryManager.pageSizeBytes();
  }

  /**
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of Tungsten memory that will be shared between operators.
   *
   * Returns `null` if there was not enough memory to allocate the page. May return a page that
   * contains fewer bytes than requested, so callers should verify the size of returned pages.
   *
   * 用于给MemoryConsumer分配指定大小（单位为字节）的MemoryBlock。
   *
   * TaskMemoryManager在分配Page时，
   * 首先从指定内存模式对应的ExecutionMemoryPool中申请获得逻辑内存，
   * 然后会选择内存模式对应的MemoryAllocator申请获得物理内存。
   */
  public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
    // 校验参数
    assert(consumer != null);
    assert(consumer.getMode() == tungstenMemoryMode);
    // 请求获得的页大小不能超出限制17179869176，即（2^32-1）× 8
    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException(
        "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
    }

    // 获取逻辑内存
    long acquired = acquireExecutionMemory(size, consumer);
    if (acquired <= 0) {
      // 如果获取到的内存大小小于等于零，那么返回null
      return null;
    }

    // 页控制
    final int pageNumber;
    synchronized (this) {
      // 获得还未分配的页号
      pageNumber = allocatedPages.nextClearBit(0);
      // 页号不能大于总页数
      if (pageNumber >= PAGE_TABLE_SIZE) {
        // 释放申请的逻辑内存
        releaseExecutionMemory(acquired, consumer);
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      // 将此页号记为已分配
      allocatedPages.set(pageNumber);
    }
    MemoryBlock page = null;
    try {
      // 获取Tungsten采用的内存分配器，分配指定大小的MemoryBlock
      page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
    } catch (OutOfMemoryError e) {
      /**
       * 说明物理内存大小小于MemoryManager认为自己管理的逻辑内存大小，
       * 此时需要更新acquiredButNotUsed，从allocatedPages中清除此页号并再次调用allocatePage()方法。
       */
      logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
      // there is no enough memory actually, it means the actual free memory is smaller than
      // MemoryManager thought, we should keep the acquired memory.
      synchronized (this) {
        // 更新acquiredButNotUsed
        acquiredButNotUsed += acquired;
        // 从allocatedPages中清除此页号
        allocatedPages.clear(pageNumber);
      }
      // this could trigger spilling to free some pages.
      // 再次调用allocatePage()方法
      return allocatePage(size, consumer);
    }
    // 给MemoryBlock指定页号
    page.pageNumber = pageNumber;
    // 将页号（pageNumber）与MemoryBlock之间的对应关系放入pageTable中
    pageTable[pageNumber] = page;
    if (logger.isTraceEnabled()) {
      logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
    }

    // 返回MemoryBlock
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
   * 用于释放给MemoryConsumer分配的MemoryBlock。
   * TaskMemoryManager在释放Page时，首先使用内存模式对应的MemoryAllocator释放物理内存，
   * 然后从指定内存模式对应的ExecutionMemoryPool中释放逻辑内存。
   * freePage()与allocatePage()操作的顺序正好相反。
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {
    assert (page.pageNumber != -1) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert(allocatedPages.get(page.pageNumber));
    // 清理pageTable中指定页号对应的MemoryBlock
    pageTable[page.pageNumber] = null;
    synchronized (this) {
      // 清空allocatedPages对MemoryBlock的页号的跟踪
      allocatedPages.clear(page.pageNumber);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }
    // 获取MemoryBlock的页大小
    long pageSize = page.size();
    // 释放MemoryBlock
    memoryManager.tungstenMemoryAllocator().free(page);
    // 释放MemoryManager管理的逻辑内存
    releaseExecutionMemory(pageSize, consumer);
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   *
   * 用于根据给定的Page（即MemoryBlock）和Page中偏移量的地址，返回页号和相对于内存块起始地址的偏移量（64位长整型）。
   *
   * @param page a data page allocated by {@link TaskMemoryManager#allocatePage}/
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) { // Tungsten的内存模式是堆外内存
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      /**
       * 此时的参数offsetInPage是操作系统内存的绝对地址，
       * offsetInPage与MemoryBlock的起始地址之差就是相对于起始地址的偏移量
       */
      offsetInPage -= page.getBaseOffset();
    }
    // 通过位运算将页号存储到64位长整型的高13位中，并将偏移量存储到64位长整型的低51位中，返回生成的64位的长整型。
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }

  // 获取页号相对于内存块起始地址的偏移量
  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }

  // 用于解码页号，将64位的长整型右移51位（只剩下页号），然后转换为整型以获得Page的页号。
  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    // 右移51位
    return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
  }

  // 解码偏移量，用于将64位的长整型与51位的掩码按位进行与运算，以获得在Page中的偏移量。
  private static long decodeOffset(long pagePlusOffsetAddress) {
    // 与上MASK_LONG_LOWER_51_BITS掩码，即取pagePlusOffsetAddress的低51位
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

    /**
     * Get the page associated with an address encoded by
     * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
     * <p>
     * 用于获取Page，通过64位的长整型，获取Page在内存中的对象。此方法在Tungsten采用堆内存模式时有效，否则返回null。
     */
    public Object getPage(long pagePlusOffsetAddress) {
        if (tungstenMemoryMode == MemoryMode.ON_HEAP) { // Tungsten的内存模式是堆内存
            // 获得Page的页号
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            // 从pageTable中取出MemoryBlock
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            assert (page.getBaseObject() != null);
            // 返回MemoryBlock的obj
            return page.getBaseObject();
        } else { // Tungsten的内存模式是堆外内存
            // 由于使用操作系统内存时不需要在JVM堆上创建对象，因此直接返回null
            return null;
        }
    }

  /**
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   *
   * 用于通过64位的长整型，获取在Page中的偏移量。
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    // 获得在Page中的偏移量
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) { // Tungsten的内存模式是堆内存
      // 返回在Page中的偏移量
      return offsetInPage;
    } else { // Tungsten的内存模式是堆外内存
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
      // 获得Page的页号
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      // 从pageTable中获得与页号对应的MemoryBlock
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      // 返回Page在操作系统内存中的偏移量
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   *
   * 用于清空所有Page和内存
   */
  public long cleanUpAllAllocatedMemory() {
    synchronized (this) {
      // 遍历所有的内存消费者，进行日志记录
      for (MemoryConsumer c: consumers) {
        if (c != null && c.getUsed() > 0) {
          // In case of failed task, it's normal to see leaked memory
          logger.debug("unreleased " + Utils.bytesToString(c.getUsed()) + " memory from " + c);
        }
      }
      // 清空consumers数组
      consumers.clear();

      // 遍历pageTable管理的所有MemoryBlock
      for (MemoryBlock page : pageTable) {
        if (page != null) {
          logger.debug("unreleased page: " + page + " in task " + taskAttemptId);
          // 使用具体的Tungsten MemoryAllocator进行释放
          memoryManager.tungstenMemoryAllocator().free(page);
        }
      }
      // 将pageTable填充为空数组
      Arrays.fill(pageTable, null);
    }

    // release the memory that is not used by any consumer (acquired for pages in tungsten mode).
    // 释放未被使用的Tungsten内存
    memoryManager.releaseExecutionMemory(acquiredButNotUsed, taskAttemptId, tungstenMemoryMode);

    // 使用当前TaskAttempt占用的所有执行内存
    return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
  }

  /**
   * Returns the memory consumption, in bytes, for the current task.
   *
   * 用于获取TaskAttempt消费的所有内存的大小
   */
  public long getMemoryConsumptionForThisTask() {
    return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
  }

  /**
   * Returns Tungsten memory mode
   *
   * 用于返回tungstenMemoryMode的值
   */
  public MemoryMode getTungstenMemoryMode() {
    return tungstenMemoryMode;
  }
}
