package com.dyingbleed.akita.sink

import java.nio.charset.Charset
import scala.collection.JavaConverters._

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.dyingbleed.akita.source.CanalMessage
import org.apache.flume.api.{RpcClient, RpcClientFactory}
import org.apache.flume.event.EventBuilder

/**
  * Created by 李震 on 2018/7/4.
  */
class FlumeSink(args: FlumeArgs) extends GraphStage[SinkShape[CanalMessage]] {

  val in: Inlet[CanalMessage] = Inlet("flume.in")

  override def shape: SinkShape[CanalMessage] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StageLogging {
      
      private var client: RpcClient = null

      override def preStart(): Unit = {
        if (this.client == null) {
          log.info("连接 fllume")
          this.client = RpcClientFactory.getDefaultInstance(args.host, args.port)
          log.info("连接 flume 成功")

          pull(in)
        }
      }
      
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val message = grab(in)
          val header = Map(
            "table_name" -> message.tableName,
            "event" -> message.event.name,
            "schema" -> message.schema
          )
          val event = EventBuilder.withBody(message.row, Charset.forName("UTF-8"), header.asJava)
          client.append(event)
          pull(in)
        }
      })

      override def postStop(): Unit = {
        this.client.close()
        this.client = null
        log.info("flume 连接关闭")
      }
    }
  }

}
