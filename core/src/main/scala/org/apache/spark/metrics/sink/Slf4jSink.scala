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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, Slf4jReporter}

import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

private[spark] class Slf4jSink(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink {
  val SLF4J_DEFAULT_PERIOD = 10
  val SLF4J_DEFAULT_UNIT = "SECONDS"

  val SLF4J_KEY_PERIOD = "period"
  val SLF4J_KEY_UNIT = "unit"

  // 获取数据的时间周期
  val pollPeriod = Option(property.getProperty(SLF4J_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => SLF4J_DEFAULT_PERIOD
  }

  // 获取数据的时间周期的单位
  val pollUnit: TimeUnit = Option(property.getProperty(SLF4J_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(SLF4J_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 实例化Slf4jReporter对象
  val reporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  override def start() {
    // 调用Slf4jReporter的相关方法
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    // 调用Slf4jReporter的相关方法
    reporter.stop()
  }

  override def report() {
    // 调用Slf4jReporter的相关方法
    reporter.report()
  }
}

