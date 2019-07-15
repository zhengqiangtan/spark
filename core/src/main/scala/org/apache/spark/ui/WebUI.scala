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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager, // SparkEnv中创建的安全管理器SecurityManager
    val sslOptions: SSLOptions, // 使用SecurityManager获取spark.ssl.ui属性指定的WebUI的SSL（Secure Sockets Layer，安全套接层）选项。
    port: Int, // WebUI对外服务的端口，可以使用spark.ui.port属性进行配置。
    conf: SparkConf,
    basePath: String = "", // WebUI的基本路径，默认为空字符串。
    name: String = "") // WebUI的名称，Spark UI的name为SparkUI。
  extends Logging {

  // WebUITab的缓冲数组。
  protected val tabs = ArrayBuffer[WebUITab]()
  // ServletContextHandler的缓冲数组。ServletContextHandler是Jetty提供的API，负责对ServletContext进行处理。
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  /**
    * WebUIPage与ServletContextHandler缓冲数组之间的映射关系。
    * 由于WebUIPage的两个方法render和renderJson分别需要由一个对应的ServletContextHandler处理，
    * 所以一个WebUIPage对应两个ServletContextHandler。
    */
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  // 用于缓存ServerInfo，即WebUI的Jetty服务器信息。
  protected var serverInfo: Option[ServerInfo] = None
  /**
    * 当前WebUI的Jetty服务的主机名。
    * 优先采用系统环境变量SPARK_PUBLIC_DNS指定的主机名，否则采用spark.driver.host属性指定的host，
    * 在没有前两个配置的时候，将默认使用工具类Utils的localHostName方法返回的主机名。
    */
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  // 过滤了$符号的当前类的简单名称。
  private val className = Utils.getFormattedClassName(this)

  // 获取basePath
  def getBasePath: String = basePath
  // 获取tabs中的所有WebUITab，并以Scala的序列返回。
  def getTabs: Seq[WebUITab] = tabs.toSeq
  // 获取handlers中的所有ServletContextHandler，并以Scala的序列返回。
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq
  // 获取securityManager。
  def getSecurityManager: SecurityManager = securityManager

  /**
    * Attach a tab to this UI, along with all of its attached pages.
    * 添加WebUITab到当前WebUI中
    **/
  def attachTab(tab: WebUITab) {
    // 向tabs中添加WebUITab，然后给WebUITab中的每个WebUIPage施加attachPage方法
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  // 从当前WebUI中移除指定WebUITab
  def detachTab(tab: WebUITab) {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  // 从当前WebUI中移除指定WebUIPage
  def detachPage(page: WebUIPage) {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /**
    * Attach a page to this UI.
    * 添加WebUIPage到当前WebUI中
    * */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    // 创建renderHandler和renderJsonHandler
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)
    // 将上面两个Handler添加到handlers缓存数组和Jetty服务器中
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    // 将上面两个Handler更新到pageToHandlers中
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
  }

  /**
    * Attach a handler to this UI.
    * 给handlers缓存数组中添加ServletContextHandler，
    * 并且将此ServletContextHandler通过ServerInfo的addHandler方法添加到Jetty服务器中。
    **/
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach { info =>
      info.rootHandler.addHandler(handler)
      if (!handler.isStarted) {
        handler.start()
      }
    }
  }

  /**
    * Detach a handler from this UI.
    * 从handlers缓存数组中移除ServletContextHandler，
    * 并且将此ServletContextHandler通过ServerInfo的removeHandler方法从Jetty服务器中移除。
    * */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach { info =>
      info.rootHandler.removeHandler(handler)
      if (handler.isStarted) {
        handler.stop()
      }
    }
  }

  /**
   * Add a handler for static content.
   *
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /**
   * Remove a static content handler.
   *
   * @param path Path in UI to unmount.
   */
  def removeStaticHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
    * Initialize all components of the server.
    * 用于初始化WebUI服务中的所有组件
    **/
  def initialize(): Unit

  /**
    * Bind to the HTTP server behind this web interface.
    * 启动与WebUI绑定的Jetty服务
    * */
  def bind() {
    assert(!serverInfo.isDefined, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /**
    * Return the url of web interface. Only valid after bind().
    * 获取WebUI的Web界面的URL
    * */
  def webUrl: String = s"http://$publicHostName:$boundPort"

  /**
    * Return the actual port to which this server is bound. Only valid after bind().
    * 获取WebUI的Jetty服务的端口
    * */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /**
    * Stop the server behind this web interface. Only valid after bind().
    * 停止WebUI，即停止WebUI底层的Jetty服务。
    * */
  def stop() {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.get.stop()
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
 *
  * @param parent 上一级节点
  * @param prefix 当前WebUITab的前缀，prefix将与上级节点的路径一起构成当前WebUITab的访问路径。
  */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  // 当前WebUITab所包含的WebUIPage的缓冲数组。
  val pages = ArrayBuffer[WebUIPage]()
  // 当前WebUITab的名称。
  val name = prefix.capitalize

  /**
    * Attach a page to this tab. This prepends the page's prefix with the tab's own prefix.
    * 首先将当前WebUITab的前缀与WebUIPage的前缀拼接，作为WebUIPage的访问路径，然后向pages中添加WebUIPage。
    **/
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /**
    * Get a list of header tabs from the parent UI.
    * 获取父亲WebUI中的所有WebUITab。
    * 此方法实际通过调用父亲WebUI的getTabs方法实现。
    **/
  def headerTabs: Seq[WebUITab] = parent.getTabs

  // 获取父亲WebUI的基本路径。此方法实际通过调用父亲WebUI的getBasePath方法实现。
  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  // 渲染页面
  def render(request: HttpServletRequest): Seq[Node]
  // 生成JSON
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
