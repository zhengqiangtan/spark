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

package org.apache.spark.util

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
 *
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  // 事件队列，双端队列
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  // 标记当前事件循环是否停止
  private val stopped = new AtomicBoolean(false)

  // 事件处理线程
  private val eventThread = new Thread(name) {
    // 设置为守护线程
    setDaemon(true)

    // 主要的run()方法
    override def run(): Unit = {
      try {
        while (!stopped.get) {
          // 从事件队列中取出事件
          val event = eventQueue.take()
          try {
            // 交给onReceive()方法处理
            onReceive(event)
          } catch { // 异常处理
            case NonFatal(e) => // 非致命异常
              try {
                // 回调给onError()方法处理
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch { // 中断等其他异常
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  // 启动当前事件循环
  def start(): Unit = {
    // 判断是否已被停止，被停止的事件循环无法被启动
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    // 调用onStart()方法通知事件循环启动了，onStart()方法由子类实现
    onStart()
    // 启动事件处理线程
    eventThread.start()
  }

  // 停止当前事件循环
  def stop(): Unit = {
    // CAS方式修改stopped为true，标识事件循环被停止
    if (stopped.compareAndSet(false, true)) {
      // 中断事件处理线程
      eventThread.interrupt()
      // 标识是否调用了onStop()方法
      var onStopCalled = false
      try {
        // 对事件处理线程进行join，等待它完成
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        // 标记onStopCalled并调用onStop()方法通知事件循环停止了，onStop()方法由子类实现
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // 如果join过程中出现中断异常，则直接调用onStop()方法
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
   * Put the event into the event queue. The event thread will process it later.
    * 投递事件，会放入eventQueue事件队列
   */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  /**
   * Return if the event thread has already been started but not yet stopped.
    *
    * 判断事件循环是否处于激活状态
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * Invoked when `start()` is called but before the event thread starts.
    * 表示事件循环启动了，需子类实现
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
    * 表示事件循环停止了，需子类实现
   */
  protected def onStop(): Unit = {}

  /**
   * Invoked in the event thread when polling events from the event queue.
   *
   * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
   * and cannot process events in time. If you want to call some blocking actions, run them in
   * another thread.
    *
    * 表示收到事件，需子类实现
   */
  protected def onReceive(event: E): Unit

  /**
   * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
   * will be ignored.
    *
    * 表示在处理事件时出现异常，需子类实现
   */
  protected def onError(e: Throwable): Unit

}
