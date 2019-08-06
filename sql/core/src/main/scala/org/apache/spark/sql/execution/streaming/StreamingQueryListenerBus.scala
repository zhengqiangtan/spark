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

package org.apache.spark.sql.execution.streaming

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[StreamingQueryListener]]s. This one will send received
 * [[StreamingQueryListener.Event]]s to the Spark listener bus. It also registers itself with
 * Spark listener bus, so that it can receive [[StreamingQueryListener.Event]]s and dispatch them
 * to StreamingQueryListeners.
 *
 * Note that each bus and its registered listeners are associated with a single SparkSession
 * and StreamingQueryManager. So this bus will dispatch events to registered listeners for only
 * those queries that were started in the associated SparkSession.
  * 用于将StreamingQueryListener.Event类型的事件投递到StreamingQueryListener类型的监听器，
  * 此外还会将StreamingQueryListener.Event类型的事件交给SparkListenerBus。
  *
  * StreamingQueryListenerBus自己也是一个SparkListener监听器，
  * 它会将自己添加到sparkListenerBus中，并且实现了onOtherEvent()方法
 */
class StreamingQueryListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingQueryListener, StreamingQueryListener.Event] {

  import StreamingQueryListener._

  sparkListenerBus.addListener(this)

  /**
   * RunIds of active queries whose events are supposed to be forwarded by this ListenerBus
   * to registered `StreamingQueryListeners`.
   *
   * Note 1: We need to track runIds instead of ids because the runId is unique for every started
   * query, even it its a restart. So even if a query is restarted, this bus will identify them
   * separately and correctly account for the restart.
   *
   * Note 2: This list needs to be maintained separately from the
   * `StreamingQueryManager.activeQueries` because a terminated query is cleared from
   * `StreamingQueryManager.activeQueries` as soon as it is stopped, but the this ListenerBus
   * must clear a query only after the termination event of that query has been posted.
    *
    * 用于记录需要被sparkListenerBus转发的事件的RunId
   */
  private val activeQueryRunIds = new mutable.HashSet[UUID]

  /**
   * Post a StreamingQueryListener event to the added StreamingQueryListeners.
   * Note that only the QueryStarted event is posted to the listener synchronously. Other events
   * are dispatched to Spark listener bus. This method is guaranteed to be called by queries in
   * the same SparkSession as this listener.
    *
    * 投递事件的方法
   */
  def post(event: StreamingQueryListener.Event) {
    event match {
      case s: QueryStartedEvent => // 对于QueryStartedEvent单独处理
        // 记录Query事件的runId
        activeQueryRunIds.synchronized { activeQueryRunIds += s.runId }
        // 会将投递的事件也投递给sparkListenerBus
        sparkListenerBus.post(s)
        // post to local listeners to trigger callbacks
        // 然后将事件通知给自己所维护的所有监听器
        postToAll(s)
      case _ =>
        // 其它事件直接投递给sparkListenerBus
        sparkListenerBus.post(event)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingQueryListener.Event =>
        // SPARK-18144: we broadcast QueryStartedEvent to all listeners attached to this bus
        // synchronously and the ones attached to LiveListenerBus asynchronously. Therefore,
        // we need to ignore QueryStartedEvent if this method is called within SparkListenerBus
        // thread
        /**
          * 当收到投递给sparkListenerBus的StreamingQueryListener.Event时，
          * 当LiveListenerBus事件总线的处理线程还没有工作，或投递的事件不是QueryStartedEvent，
          * 就讲事件通知给自己所维护的所有监听器
          */
        if (!LiveListenerBus.withinListenerThread.value || !e.isInstanceOf[QueryStartedEvent]) {
          postToAll(e)
        }
      case _ =>
    }
  }

  /**
   * Dispatch events to registered StreamingQueryListeners. Only the events associated queries
   * started in the same SparkSession as this ListenerBus will be dispatched to the listeners.
   */
  override protected def doPostEvent(
      listener: StreamingQueryListener,
      event: StreamingQueryListener.Event): Unit = {

    // 该方法用于判断activeQueryRunIds是否包含指定的runId
    def shouldReport(runId: UUID): Boolean = {
      activeQueryRunIds.synchronized { activeQueryRunIds.contains(runId) }
    }

    event match {
      case queryStarted: QueryStartedEvent =>
        // 如果activeQueryRunIds中包含该Query事件的runId
        if (shouldReport(queryStarted.runId)) {
          // 则执行监听器的onQueryStarted()方法
          listener.onQueryStarted(queryStarted)
        }
      case queryProgress: QueryProgressEvent =>
        // 如果activeQueryRunIds中包含该Query事件的runId
        if (shouldReport(queryProgress.progress.runId)) {
          // 则执行监听器的onQueryProgress()方法
          listener.onQueryProgress(queryProgress)
        }
      case queryTerminated: QueryTerminatedEvent =>
        // 如果activeQueryRunIds中包含该Query事件的runId
        if (shouldReport(queryTerminated.runId)) {
          // 则执行监听器的onQueryTerminated()方法
          listener.onQueryTerminated(queryTerminated)
          // 因为Query事件终止了，需要将该事件的runId从activeQueryRunIds移除
          activeQueryRunIds.synchronized { activeQueryRunIds -= queryTerminated.runId }
        }
      case _ =>
    }
  }
}
