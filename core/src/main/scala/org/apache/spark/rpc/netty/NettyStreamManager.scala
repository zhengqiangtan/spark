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
package org.apache.spark.rpc.netty

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc.RpcEnvFileServer
import org.apache.spark.util.Utils

/**
 * StreamManager implementation for serving files from a NettyRpcEnv.
 *
 * Three kinds of resources can be registered in this manager, all backed by actual files:
 *
 * - "/files": a flat list of files; used as the backend for [[SparkContext.addFile]].
 * - "/jars": a flat list of files; used as the backend for [[SparkContext.addJar]].
 * - arbitrary directories; all files under the directory become available through the manager,
 *   respecting the directory's hierarchy.
 *
 * Only streaming (openStream) is supported.
 */
private[netty] class NettyStreamManager(rpcEnv: NettyRpcEnv)
  extends StreamManager with RpcEnvFileServer {

  // 提供了对下面三类文件的下载支持
  // 普通文件
  private val files = new ConcurrentHashMap[String, File]()
  // jar文件
  private val jars = new ConcurrentHashMap[String, File]()
  // 目录
  private val dirs = new ConcurrentHashMap[String, File]()

  // 对获取文件块不支持
  override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    throw new UnsupportedOperationException()
  }

  // 打开文件流
  override def openStream(streamId: String): ManagedBuffer = {
    // 移除前面的"/"，然后以"/"切分为两份，前一份为文件类型，后一份为文件名
    val Array(ftype, fname) = streamId.stripPrefix("/").split("/", 2)
    val file = ftype match {
      case "files" => files.get(fname) // 普通文件，直接返回
      case "jars" => jars.get(fname) // jar文件，直接返回
      case other => // 其他，说明是目录
        val dir = dirs.get(ftype)
        require(dir != null, s"Invalid stream URI: $ftype not found.")
        // 构造File返回
        new File(dir, fname)
    }

    // 为文件构建FileSegmentManagedBuffer流
    if (file != null && file.isFile()) {
      new FileSegmentManagedBuffer(rpcEnv.transportConf, file, 0, file.length())
    } else {
      null
    }
  }

  // 添加普通文件
  override def addFile(file: File): String = {
    // 直接添加，如果已经存在相同的文件则会抛出IllegalArgumentException异常
    val existingPath = files.putIfAbsent(file.getName, file)
    require(existingPath == null || existingPath == file,
      s"File ${file.getName} was already registered with a different path " +
        s"(old path = $existingPath, new path = $file")
    s"${rpcEnv.address.toSparkURL}/files/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

  // 添加Jar文件
  override def addJar(file: File): String = {
    // 直接添加，如果已经存在相同的文件则会抛出IllegalArgumentException异常
    val existingPath = jars.putIfAbsent(file.getName, file)
    require(existingPath == null || existingPath == file,
      s"File ${file.getName} was already registered with a different path " +
        s"(old path = $existingPath, new path = $file")
    s"${rpcEnv.address.toSparkURL}/jars/${Utils.encodeFileNameToURIRawPath(file.getName())}"
  }

  // 添加目录
  override def addDirectory(baseUri: String, path: File): String = {
    // 验证目录的URI，不能是/files或/jars，否则抛出IllegalArgumentException异常
    val fixedBaseUri = validateDirectoryUri(baseUri)
    // 去掉前面的"/"后再添加，如果已经存在会抛出IllegalArgumentException异常
    require(dirs.putIfAbsent(fixedBaseUri.stripPrefix("/"), path) == null,
      s"URI '$fixedBaseUri' already registered.")
    s"${rpcEnv.address.toSparkURL}$fixedBaseUri"
  }

}
