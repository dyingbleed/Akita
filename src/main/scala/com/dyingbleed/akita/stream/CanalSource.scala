package com.dyingbleed.akita.stream

import java.net.InetSocketAddress
import java.util

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import com.dyingbleed.akita.utils.EntryUtils
import com.google.common.collect.Lists
import com.google.common.net.HostAndPort
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by 李震 on 2018/5/5.
  */
class CanalSource(
                   servers: String,
                   destination: String,
                   username: String,
                   password: String,
                   filter: String
                 ) extends GraphStage[SourceShape[String]] {

  val out: Outlet[String] = Outlet("canal.out")

  override def shape: SourceShape[String] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StageLogging {

      private var canalConnector: CanalConnector = _

      private val messageQueue = new mutable.Queue[String]()

      override def preStart(): Unit = {
        if (this.canalConnector == null) {
          this.canalConnector = if (servers.contains(",")) {
            // 集群模式
            val hosts: util.LinkedList[InetSocketAddress] = Lists.newLinkedList()
            for (server <- StringUtils.split(servers, ",")) {
              val hostAndPort = HostAndPort.fromString(server)
              hosts.add(new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPortOrDefault(11111)))
            }
            CanalConnectors.newClusterConnector(hosts, destination, username, password)
          } else {
            // 单机模式
            val hostAndPort: HostAndPort = HostAndPort.fromString(servers)
            CanalConnectors.newSingleConnector(
              new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPortOrDefault(11111)),
              destination,
              username,
              password
            )
          }

          log.info("连接 canal {}", servers)
          this.canalConnector.connect()
          this.canalConnector.subscribe(filter)
          this.canalConnector.rollback()
          log.info("连接 canal 成功")
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          while (messageQueue.isEmpty) {
            val message = canalConnector.getWithoutAck(100)
            val messageId = message.getId
            log.info("收到 canal 消息 {} {}", messageId, message.getEntries.size)
            if (messageId != -1 && message.getEntries.size > 0) {
              for (entry <- message.getEntries.asScala) {
                if (entry.getEntryType == EntryType.TRANSACTIONBEGIN || entry.getEntryType == EntryType.TRANSACTIONEND
                ) {
                  // do nothing
                } else {
                  for (json <- EntryUtils.toJSON(entry).asScala) {
                    messageQueue.enqueue(json)
                  }
                }
              }
              canalConnector.ack(messageId)
            }
          }

          val message = messageQueue.dequeue()
          push(out, message)
        }
      })

      override def postStop(): Unit = {
        this.canalConnector.disconnect()
        this.canalConnector = null
        log.info("canal 连接关闭")
      }
    }
  }

}
