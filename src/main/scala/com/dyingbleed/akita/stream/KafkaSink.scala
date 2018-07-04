package com.dyingbleed.akita.stream

import java.util.Properties

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Kafka Sink
  *
  * @param args Kafka 参数
  *
  * Created by 李震 on 2018/5/5.
  */
class KafkaSink(args: KafkaArgs) extends GraphStage[SinkShape[String]] {

  val in: Inlet[String] = Inlet("kafka.in")

  override def shape: SinkShape[String] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with StageLogging {

      private var kafkaProducer: KafkaProducer[String, String] = null

      override def preStart(): Unit = {
        if (this.kafkaProducer == null) {
          val properties = new Properties
          properties.put("bootstrap.servers", args.servers)
          properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

          log.info("连接 kafka {}", args.servers)
          this.kafkaProducer = new KafkaProducer[String, String](properties)
          log.info("连接 kafka 成功")

          pull(in)
        }
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val message = grab(in)
          val record = new ProducerRecord[String, String](args.topic, message)
          kafkaProducer.send(record)
          pull(in)
        }
      })

      override def postStop(): Unit = {
        this.kafkaProducer.close()
        this.kafkaProducer = null
        log.info("kafka 连接关闭")
      }
    }
  }

}
