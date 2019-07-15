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

package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())

  // 注册应用状态度量，包括WAITING，RUNNING，FINISHED，FAILED，KILLED，UNKNOWN
  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    // application的state字段
    override def getValue: String = application.state.toString
  })

  // 注册运行持续时长度量
  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    // application的duration字段
    override def getValue: Long = application.duration
  })

  // 注册授权的内核数度量
  metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
    // application的coresGranted字段
    override def getValue: Int = application.coresGranted
  })

}
