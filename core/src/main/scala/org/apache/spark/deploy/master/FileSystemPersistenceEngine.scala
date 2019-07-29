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

import java.io._

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.Utils


/**
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
  *
  * 基于文件系统的持久化引擎。
  * 对于ApplicationInfo、WorkerInfo及DriverInfo，FileSystemPersistenceEngine会
  * 将它们的数据存储到磁盘上的单个文件夹中，当要移除它们时，这些磁盘文件将被删除。
  *
  * 由于不同的Master往往不在同一个机器节点上，
  * 因此在使用FileSystemPersistenceEngine时，底层的文件系统应该是分布式的。
 *
 * @param dir Directory to store files. Created if non-existent (but not recursively).
  *            持久化的根目录
 * @param serializer Used to serialize our objects.
  *                   持久化使用的序列化器
 */
private[master] class FileSystemPersistenceEngine(
    val dir: String,
    val serializer: Serializer)
  extends PersistenceEngine with Logging {

  new File(dir).mkdir()

  override def persist(name: String, obj: Object): Unit = {
    // 调用了serializeIntoFile()方法
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  override def unpersist(name: String): Unit = {
    // 获取文件对应的File对象
    val f = new File(dir + File.separator + name)
    // 尝试删除文件
    if (!f.delete()) {
      logWarning(s"Error deleting ${f.getPath()}")
    }
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    // 过滤出持久化根目录下，文件名匹配前缀的所有文件
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    // 对每个文件的数据反序列化
    files.map(deserializeFromFile[T])
  }

  private def serializeIntoFile(file: File, value: AnyRef) {
    // 创建文件
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    // 打开输出流
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      // 包装为序列化流
      out = serializer.newInstance().serializeStream(fileOut)
      // 写出数据到文件
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    // 文件输入流
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      // 反序列化
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
