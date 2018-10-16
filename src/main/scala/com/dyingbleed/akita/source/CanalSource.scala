package com.dyingbleed.akita.source

import java.net.InetSocketAddress
import java.util

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import com.google.common.collect.Lists
import com.google.common.net.HostAndPort
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Canal Source
  *
  * @param args Canal 参数
  *
  * Created by 李震 on 2018/5/5.
  */
class CanalSource(args: CanalArgs) extends GraphStage[SourceShape[CanalMessage]] {

  val out: Outlet[CanalMessage] = Outlet("canal.out")

  override def shape: SourceShape[CanalMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StageLogging {

      private var canalConnector: CanalConnector = _

      private val messageQueue = new mutable.Queue[CanalMessage]()

      private var messageBatchId = -1l

      override def preStart(): Unit = {
        if (this.canalConnector == null) {
          this.canalConnector = if (args.servers.contains(",")) {
            // 集群模式
            val hosts: util.LinkedList[InetSocketAddress] = Lists.newLinkedList()
            for (server <- StringUtils.split(args.servers, ",")) {
              val hostAndPort = HostAndPort.fromString(server)
              hosts.add(new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPort))
            }
            CanalConnectors.newClusterConnector(hosts, args.destination, args.username, args.password)
          } else {
            // 单机模式
            val hostAndPort: HostAndPort = HostAndPort.fromString(args.servers)
            CanalConnectors.newSingleConnector(
              new InetSocketAddress(hostAndPort.getHostText, hostAndPort.getPort()),
              args.destination,
              args.username,
              args.password
            )
          }

          log.info("连接 canal")
          this.canalConnector.connect()
          this.canalConnector.subscribe(args.filter)
          this.canalConnector.rollback()
          log.info("连接 canal 成功")
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          /*
           * 1. 如果队列为空，从 canal 获取批量数据（最多 100 条），入队
           * */
          while (messageQueue.isEmpty) {
            val messageBatch = canalConnector.getWithoutAck(100)
            messageBatchId = messageBatch.getId
            log.debug("收到 canal 消息 {}", messageBatchId)
            if (messageBatchId != -1) {
              for (entry <- messageBatch.getEntries.asScala) {
                if (entry.getEntryType == EntryType.TRANSACTIONBEGIN || entry.getEntryType == EntryType.TRANSACTIONEND
                ) {
                  // do nothing
                } else {
                  for (rows <- CanalUtils.toJSON(entry)) {
                    messageQueue.enqueue(rows)
                  }
                }
              }
            }
          }

          /*
           * 2. 出队，推送到出口
           * */
          val message = messageQueue.dequeue()
          push(out, message)

          /*
           * 3. 如果队列为空，即上次获取批量数据处理完毕，向 canal 确认
           * */
          if (messageQueue.isEmpty && messageBatchId != -1) canalConnector.ack(messageBatchId)
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
