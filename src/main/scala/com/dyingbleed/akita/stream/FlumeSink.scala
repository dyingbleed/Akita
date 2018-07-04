package com.dyingbleed.akita.stream

import java.nio.charset.Charset

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}
import akka.stream.{Attributes, Inlet, SinkShape}
import org.apache.flume.api.{RpcClient, RpcClientFactory}
import org.apache.flume.event.EventBuilder

/**
  * Created by 李震 on 2018/7/4.
  */
class FlumeSink(args: FlumeArgs) extends GraphStage[SinkShape[String]] {

  val in: Inlet[String] = Inlet("flume.in")

  override def shape: SinkShape[String] = SinkShape(in)

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
          val event = EventBuilder.withBody(message, Charset.forName("UTF-8"))
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
