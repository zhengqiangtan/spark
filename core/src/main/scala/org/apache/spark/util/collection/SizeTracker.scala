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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   *
   * 采样增长的速率。例如，速率为2时，分别对在1，2，4，8...位置上的元素进行采样。
   * SAMPLE_GROWTH_RATE的值固定为1.1。
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /** Samples taken since last resetSamples(). Only the last two are kept for extrapolation.
   * 样本队列，最后两个样本将被用于估算。
   */
  private val samples = new mutable.Queue[Sample]

  /** The average number of bytes per update between our last two samples.
   * 平均每次更新的字节数。
   */
  private var bytesPerUpdate: Double = _

  /** Total number of insertions and updates into the map since the last resetSamples().
   * 更新操作（包括插入和更新）的总次数。
   */
  private var numUpdates: Long = _

  /** The value of 'numUpdates' at which we will take our next sample.
   * 下次采样时，numUpdates的值，即numUpdates的值增长到与nextSampleNum相同时，才会再次采样。
   */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   *
   * 用于重置SizeTracker采集的样本
   */
  protected def resetSamples(): Unit = {
    // 将numUpdates设置为1
    numUpdates = 1
    // 将nextSampleNum设置为1
    nextSampleNum = 1
    // 清空samples中的样本
    samples.clear()
    // 调用takeSample()方法采集样本
    takeSample()
  }

  /**
   * Callback to be invoked after every update.
   *
   * 用于向集合中更新了元素之后进行回调，以触发对集合的采样。
   */
  protected def afterUpdate(): Unit = {
    // 更新numUpdates
    numUpdates += 1
    if (nextSampleNum == numUpdates) { // 如果nextSampleNum与numUpdates相等
      // 调用takeSample()方法采样
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   *
   * 用于采集样本
   */
  private def takeSample(): Unit = {
    // 调用SizeEstimator的estimate()方法估算集合的大小，并将估算的大小和numUpdates作为样本放入队列samples中。
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    // 仅适用最后的两个样本进行推断
    if (samples.size > 2) { // 保留样本队列的最后两个样本
      // 队首出队，扔掉一个无用样本
      samples.dequeue()
    }
    // 将队列翻转后进行匹配
    val bytesDelta = samples.toList.reverse match {
        // 分别是 倒数第1个，倒数第2个和剩下的
      case latest :: previous :: tail =>
        // (倒数第1个样本记录的大小 - 倒数第2个样本记录的大小) / (倒数第1个样本的采样编号 - 倒数第2个的采样编号)
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    // 得到根据采样计算的每次更新字节数速率，最小为0
    bytesPerUpdate = math.max(0, bytesDelta)
    // 机选下次采样的采样号
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   *
   * 用于估算集合的当前大小（单位为字节）
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    // 使用当前的numUpdates与上次采样的numUpdates之间的差值，乘以bytesPerUpdate作为估计要增加的大小。
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    // 将上次采样时的集合大小与extrapolatedDelta相加作为估算的集合大小
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
