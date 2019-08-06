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

package org.apache.spark.scheduler

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.DynamicVariable

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
  * 采用异步线程将SparkListenerEvent类型的事件投递到SparkListener类型的监听器。
 */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {

  self =>

  import LiveListenerBus._

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  // 事件队列容量，通过spark.scheduler.listenerbus.eventqueue.size参数指定，默认为1000
  private lazy val EVENT_QUEUE_CAPACITY = validateAndGetQueueSize()

  /**
    * SparkListenerEvent事件的阻塞队列，
    * 队列大小可以通过Spark属性spark.scheduler.listenerbus.eventqueue.size进行配置，
    * 默认为10000（Spark早期版本中属于静态属性，固定为10000）。
    */
  private lazy val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)

  // 从配置信息获取队列容量值，会设置给EVENT_QUEUE_CAPACITY字段
  private def validateAndGetQueueSize(): Int = {
    // 通过spark.scheduler.listenerbus.eventqueue.size获取队列容量，默认为1000
    val queueSize = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_SIZE)
    if (queueSize <= 0) {
      throw new SparkException("spark.scheduler.listenerbus.eventqueue.size must be > 0!")
    }
    queueSize
  }

  // Indicate if `start()` is called
  // 标记LiveListenerBus的启动状态的AtomicBoolean类型的变量。
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  // 标记LiveListenerBus的停止状态的AtomicBoolean类型的变量。
  private val stopped = new AtomicBoolean(false)

  /**
    * A counter for dropped events. It will be reset every time we log it.
    * 使用AtomicLong类型对删除的事件进行计数，
    * 每当日志打印了droppedEventsCounter后，会将droppedEventsCounter重置为0。
    **/
  private val droppedEventsCounter = new AtomicLong(0L)

  /**
    * When `droppedEventsCounter` was logged last time in milliseconds.
    * 用于记录最后一次日志打印droppedEventsCounter的时间戳。
    **/
  @volatile private var lastReportTimestamp = 0L

  // Indicate if we are processing some event
  // Guarded by `self`
  // 用来标记当前正有事件被listenerThread线程处理。
  private var processingEvent = false

  // 用于标记是否由于eventQueue已满，导致新的事件被删除。
  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  // 用于当有新的事件到来时释放信号量，当对事件进行处理时获取信号量。
  private val eventLock = new Semaphore(0)

  // 处理事件的线程
  private val listenerThread = new Thread(name) {
    // 设置为守护线程
    setDaemon(true)

    // 主要的运行方法
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {

        // 无限循环
        while (true) {
          // 获取信号量
          eventLock.acquire()
          self.synchronized {
            // 标记正在处理事件
            processingEvent = true
          }
          try {
            // 从eventQueue中获取事件，并判断事件是否为空
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            // 对事件进行处理
            postToAll(event)
          } finally {
            self.synchronized {
              // 标记当前没有事件被处理
              processingEvent = false
            }
          }
        }
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   *
   */
  def start(): Unit = {
    // CAS方式标记开始运行了
    if (started.compareAndSet(false, true)) {
      // 启动listenerThread线程开始处理事件
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  // 向eventQueue队列放入事件
  def post(event: SparkListenerEvent): Unit = {
    // 判断事件总线运行状态
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    // 放入事件
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) { // 放入成功，释放信号量
      eventLock.release()
    } else { // 放入失败，可能是因为事件队列满了
      // 丢弃事件
      onDropEvent(event)
      // 递增丢弃事件的计数器
      droppedEventsCounter.incrementAndGet()
    }

    // 丢弃事件数大于0，需要周期性记录日志
    val droppedEvents = droppedEventsCounter.get
    if (droppedEvents > 0) {
      // Don't log too frequently
      // 距离上一次报告时间已经超过60秒
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        // 重置丢弃事件计数器为0
        if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
          // 更新报告时间
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          // 记录警告日志
          logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
            new java.util.Date(prevLastReportTimestamp))
        }
      }
    }
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
    *
    * 等待一段时间直到当前事件总线中没有事件了，此处的"没有事件"需满足两个条件：
    * 1. 事件队列为空；
    * 2. 没有正在处理的事件。
    *
    * 如果直到超时一直无法满足，会抛出超时异常
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    // 计算应该终止的时间
    val finishTime = System.currentTimeMillis + timeoutMillis
    // 如果事件队列不为空，或者还有正在处理的事件，就循环判断
    while (!queueIsEmpty) {
      // 如果当前时间超过了应该终止的时间，就抛出超时异常
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case.
       * 休眠一段时间
       **/
      Thread.sleep(10)
    }
  }

  /**
   * For testing only. Return whether the listener daemon thread is still alive.
   * Exposed for testing.
    *
    * 判断处理线程是否存活
   */
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
   * Return whether the event queue is empty.
   *
   * The use of synchronized here guarantees that all events that once belonged to this queue
   * have already been processed by all attached listeners, if this returns true.
    *
    * 事件队列是否为空，且没有正在处理的事件
   */
  private def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty && !processingEvent }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
    *
    * 停止事件总线
   */
  def stop(): Unit = {
    // 先检查状态
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    // CAS方式标记已经停止
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      // 释放信号量
      eventLock.release()

      /**
        * join处理线程直到它处理完正在处理的事件，
        * 因为listenerThread在判断stopped标记为true后会直接结束，
        * 所以往后的事件不会被处理
        */
      listenerThread.join()
    } else {
      // Keep quiet
    }
  }

  /**
   * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
   * notified with the dropped events.
   *
   * Note: `onDropEvent` can be called in any thread.
    *
    * 丢弃事件
   */
  def onDropEvent(event: SparkListenerEvent): Unit = {
    // 这里的执行表示事件是因为队列满了才无法放入导致失败，从而将logDroppedEvent修改为true
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }
}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** The thread name of Spark listener bus */
  val name = "SparkListenerBus"
}

