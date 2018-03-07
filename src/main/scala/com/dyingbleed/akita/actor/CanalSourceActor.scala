package com.dyingbleed.akita.actor

import java.net.InetSocketAddress
import java.util.List

import akka.actor.{Actor, ActorLogging, Timers}
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import com.dyingbleed.akita.actor.CanalSourceActor.{StartTick, Tick, TickKey}
import com.dyingbleed.akita.utils.EntryUtils
import com.google.common.collect.Lists
import com.google.common.net.HostAndPort
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
  * Created by 李震 on 2017/12/23.
  */
class CanalSourceActor(
                        servers: String,
                        destination: String,
                        username: String,
                        password: String,
                        filter: String
                      ) extends Actor with Timers with ActorLogging {

  private var canalConnector: CanalConnector = _

  override def preStart(): Unit = {
    if (servers.contains(",")) {
      // 集群模式
      val hosts: List[InetSocketAddress] = Lists.newLinkedList()
      for (server <- StringUtils.split(this.servers, ",")) {
        val hostAndPort = HostAndPort.fromString(server)
        hosts.add(new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPortOrDefault(11111)))
      }
      this.canalConnector = CanalConnectors.newClusterConnector(hosts, destination, username, password)
    } else {
      // 单机模式
      val hostAndPort: HostAndPort = HostAndPort.fromString(this.servers)
      this.canalConnector = CanalConnectors.newSingleConnector(
        new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPortOrDefault(11111)),
        destination,
        username,
        password
      )
    }

    log.info("连接 Canal {}", servers)
    this.canalConnector.connect()
    this.canalConnector.subscribe(filter)
    this.canalConnector.rollback()
    log.info("连接 Canal 成功")

    timers.startSingleTimer(TickKey, StartTick, 1.second)
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info("restart")
  }

  override def postStop(): Unit = {
    this.canalConnector.disconnect()
    this.canalConnector = null
  }

  override def receive: Receive = {
    case StartTick => {
      timers.startPeriodicTimer(TickKey, Tick, 1.second)
    }
    case Tick => {
      fetch(this.canalConnector)
    }
  }

  private def fetch(canalConnector: CanalConnector): Unit = {
    var messageId = -1l

    val message = canalConnector.getWithoutAck(1000)
    messageId = message.getId
    if (messageId != -1 && message.getEntries.size > 0) {
      log.info("收到 Canal Server 消息 {}", messageId)
      for (entry <- message.getEntries) {
        if (entry.getEntryType == EntryType.TRANSACTIONBEGIN || entry.getEntryType == EntryType.TRANSACTIONEND
        ) {
          // do nothing
        } else {
          val tableName = entry.getHeader.getTableName

          for (json <- EntryUtils.toJSON(entry)) {
            context.actorSelection("../kafka") ! (tableName, json)
          }
        }
      }
      canalConnector.ack(messageId)
    }
  }
}

object CanalSourceActor {

  private case object TickKey

  private case object StartTick

  private case object Tick

}
