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

import org.apache.spark.annotation.Private;

@Private
public enum MemoryMode {
  ON_HEAP, // 堆内存；并不能与JVM中的Java堆直接画等号，它只是JVM堆内存的一部分。
  OFF_HEAP // 堆外内存；是Spark使用sun.misc.Unsafe的API直接在工作节点的系统内存中开辟的空间。
}
