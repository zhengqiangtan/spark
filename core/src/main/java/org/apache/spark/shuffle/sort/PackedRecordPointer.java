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

/**
 * Wrapper around an 8-byte word that holds a 24-bit partition number and 40-bit record pointer.
 * <p>
 * Within the long, the data is laid out as follows:
 * <pre>
 *   [24 bit partition number][13 bit memory page number][27 bit offset in page]
 * </pre>
 * This implies that the maximum addressable page size is 2^27 bits = 128 megabytes, assuming that
 * our offsets in pages are not 8-byte-word-aligned. Since we have 2^13 pages (based off the
 * 13-bit page numbers assigned by {@link org.apache.spark.memory.TaskMemoryManager}), this
 * implies that we can address 2^13 * 128 megabytes = 1 terabyte of RAM per task.
 * <p>
 * Assuming word-alignment would allow for a 1 gigabyte maximum page size, but we leave this
 * optimization to future work as it will require more careful design to ensure that addresses are
 * properly aligned (e.g. by padding records).
 *
 * 使用64 bit的long型变量记录Record信息，分为三个部分：
 * [24 bit 分区号][13 bit 内存页号][27 bit 内存偏移量]
 * 我们知道在TaskMemoryManager中，使用13位表示内存页号，使用51位表示偏移量；
 * 此处内存页号是足量的，但内存偏移量只有27位，即可寻址的最大偏移量的大小为2^27 bit，即128 MB,
 * 因此，一个Task的可用内存 = 总页数 * 页的最大大小 = 2^13 * 2^27 = 2^40 = 1 TB字节。
 */
final class PackedRecordPointer {

  // 最大页大小
  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;  // 128 megabytes

  /**
   * The maximum partition identifier that can be encoded. Note that partition ids start from 0.
   * 最大分区号
   */
  static final int MAXIMUM_PARTITION_ID = (1 << 24) - 1;  // 16777215

  /**
   * The index of the first byte of the partition id, counting from the least significant byte.
   * 从低位向高位计算，分区号的起始字节索引，即第40位开始是分区号
   */
  static final int PARTITION_ID_START_BYTE_INDEX = 5;

  /**
   * The index of the last byte of the partition id, counting from the least significant byte.
   * 从低位向高位计算，分区号的终止字节索引，即第56位是分区号的最后一个字节的索引
   */
  static final int PARTITION_ID_END_BYTE_INDEX = 7;

  /** Bit mask for the lower 40 bits of a long.
   * 低40位掩码，可以取得Page Number和Offset
   */
  private static final long MASK_LONG_LOWER_40_BITS = (1L << 40) - 1;

  /** Bit mask for the upper 24 bits of a long
   * 高24位掩码，可以取得Partition ID
   */
  private static final long MASK_LONG_UPPER_24_BITS = ~MASK_LONG_LOWER_40_BITS;

  /** Bit mask for the lower 27 bits of a long.
   * 低27位掩码，可以取得offset
   */
  private static final long MASK_LONG_LOWER_27_BITS = (1L << 27) - 1;

  /** Bit mask for the lower 51 bits of a long.
   * 低51位掩码
   */
  private static final long MASK_LONG_LOWER_51_BITS = (1L << 51) - 1;

  /** Bit mask for the upper 13 bits of a long
   * 高13位掩码
   */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Pack a record address and partition id into a single word.
   *
   *
   *
   * @param recordPointer a record pointer encoded by TaskMemoryManager.
   *                      由TaskMemoryManager提供的Record指针记录，
   *                      高13位是页号，低51位是偏移量
   * @param partitionId a shuffle partition id (maximum value of 2^24).
   *                    分区号
   * @return a packed pointer that can be decoded using the {@link PackedRecordPointer} class.
   *          拼接为 分区号（24位）页号（13位）偏移量（27位） 的long型整数
   */
  public static long packPointer(long recordPointer, int partitionId) {
    // 检查分区号，不可超过 (1 << 24) - 1
    assert (partitionId <= MAXIMUM_PARTITION_ID);
    // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
    // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
    // 取高13位，即页号
    final long pageNumber = (recordPointer & MASK_LONG_UPPER_13_BITS) >>> 24;
    /**
     * 取低27位，获取offset
     * 将页号和偏移量进行相或，组成低40位
     */
    final long compressedAddress = pageNumber | (recordPointer & MASK_LONG_LOWER_27_BITS);
    // 将分区号右移40位，然后与页号和偏移量的40位进行相与，取得64位long型值
    return (((long) partitionId) << 40) | compressedAddress;
  }

  private long packedRecordPointer;

  // 设置packedRecordPointer
  public void set(long packedRecordPointer) {
    this.packedRecordPointer = packedRecordPointer;
  }

  // 获取分区号
  public int getPartitionId() {
    // 即取packedRecordPointer的高24位，然后右移去掉低40位即可
    return (int) ((packedRecordPointer & MASK_LONG_UPPER_24_BITS) >>> 40);
  }

  // 获取TaskMemoryManager需要的页号和偏移量
  public long getRecordPointer() {
    // 左移24位，去掉分区号，然后取高13位即是页号
    final long pageNumber = (packedRecordPointer << 24) & MASK_LONG_UPPER_13_BITS;
    // 直接取低27位即是偏移量
    final long offsetInPage = packedRecordPointer & MASK_LONG_LOWER_27_BITS;
    // 将高13位和低27位组合成64位的long型整数
    return pageNumber | offsetInPage;
  }

}
