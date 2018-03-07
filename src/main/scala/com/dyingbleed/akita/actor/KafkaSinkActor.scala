package com.dyingbleed.akita.actor

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by 李震 on 2017/12/23.
  */
class KafkaSinkActor(servers: String, topic: String) extends Actor with ActorLogging {

  private var kafkaProducer: KafkaProducer[String, String] = _

  override def preStart(): Unit = {
    val properties = new Properties
    properties.put("bootstrap.servers", this.servers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    this.kafkaProducer = new KafkaProducer[String, String](properties)
  }

  override def postStop(): Unit = {
    this.kafkaProducer.close()
    this.kafkaProducer = null
  }

  override def receive: Receive = {
    case message: (String, String) => {
      val record = new ProducerRecord[String, String](topic, message._1, message._2)
      this.kafkaProducer.send(record)
    }
  }


}
