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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging

// 借助ZooKeeper实现的领导选举代理
private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  /**
    * ZooKeeperLeaderElectionAgent在ZooKeeper上的工作目录，
    * 是Spark基于ZooKeeper进行热备的根节点的子节点leader_election。
    * 根节点可通过spark.deploy.ZooKeeper.dir属性配置，默认为spark。
    */
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  // 连接ZooKeeper的客户端
  private var zk: CuratorFramework = _
  // 使用ZooKeeper进行领导选举的客户端
  private var leaderLatch: LeaderLatch = _
  // 领导选举的状态，包括有领导（LEADER）和无领导（NOT_LEADER）
  private var status = LeadershipStatus.NOT_LEADER

  // 启动基于Zookeeper的领导选举代理
  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    // 创建Zookeeper客户端
    zk = SparkCuratorUtil.newClient(conf)

    /**
      * 创建LeaderLatch，LeaderLatch是Curator提供的进行主节点选举的API。
      * LeaderLatch实际是通过在Master上创建领导选举的Znode节点，
      * 并对Znode节点添加监视点来发现Master是否成功竞选的。
      */
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    // 将ZooKeeperLeaderElectionAgent自己作为回调监听器
    leaderLatch.addListener(this)
    // 启动选举
    leaderLatch.start()
  }

  // 停止基于Zookeeper的领导选举代理
  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  // 告知ZooKeeperLeaderElectionAgent所属的Master节点被选为Leader
  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      // 更新领导关系
      updateLeadershipStatus(true)
    }
  }

  // 告知ZooKeeperLeaderElectionAgent所属的Master节点没有被选为Leader
  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      // 更新领导关系
      updateLeadershipStatus(false)
    }
  }

  // 更新领导关系状态
  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) { // 选举为领导
      // 将其状态设置为LEADER
      status = LeadershipStatus.LEADER
      // 调用masterInstance的electedLeader方法将Master节点设置为Leader
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) { // 撤销领导职务
      // 将其状态设置为NOT_LEADER
      status = LeadershipStatus.NOT_LEADER
      // 调用masterInstance的revokedLeadership方法撤销Master节点的领导关系
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
