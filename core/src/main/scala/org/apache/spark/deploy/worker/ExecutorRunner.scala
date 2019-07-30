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

package org.apache.spark.deploy.worker

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 */
private[deploy] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: RpcEndpointRef,
    val workerId: String,
    val host: String,
    val webUiPort: Int,
    val publicAddress: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    conf: SparkConf,
    val appLocalDirs: Seq[String],
    @volatile var state: ExecutorState.Value)
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // Timeout to wait for when trying to terminate an executor.
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  private var shutdownHook: AnyRef = null

  private[worker] def start() {
    // 创建workerThread线程
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      // 线程主要调用了fetchAndRunExecutor()方法
      override def run() { fetchAndRunExecutor() }
    }
    // 启动workerThread线程
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    // JVM关闭钩子
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.RUNNING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Worker shutting down")) }
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   *
   * @param message the exception message which caused the executor's death
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
    }
    try {
      worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
    } catch {
      case e: IllegalStateException => logWarning(e.getMessage(), e)
    }
  }

  /** Stop this executor runner, including killing the process it launched */
  private[worker] def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us
    * 将ApplicationDescription的command属性（即Command）的arguments属性中每个参数中的变量替换为指定的值。
    * 例如，将{WORKER_URL}替换为ExecutorRunner的workerUrl属性。
    **/
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      // 构造ProcessBuilder，进程命令将由ApplicationDescription的command属性决定。
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      // 为ProcessBuilder设置执行目录和环境变量
      builder.directory(executorDir)
      // 为ProcessBuilder设置环境变量
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        if (conf.getBoolean("spark.ui.reverseProxy", false)) {
          s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
        } else {
          s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
        }
      // 重定向进程的输出流与错误流为executorDir目录下的文件stdout与stderr
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      // 启动ProcessBuilder，生成进程
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40)

      // Redirect its stdout and stderr to files
      // 重定向进程的输出流与错误流
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, StandardCharsets.UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      // 等待获取进程的退出状态
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      // 一旦收到退出状态，则向Worker发送ExecutorStateChanged消息。
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        // 用于杀死CoarseGrainedExecutorBackend进程后将向Worker发送ExecutorStateChanged消息
        killProcess(None)
      case e: Exception =>
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        // 用于杀死CoarseGrainedExecutorBackend进程后将向Worker发送ExecutorStateChanged消息
        killProcess(Some(e.toString))
    }
  }
}
