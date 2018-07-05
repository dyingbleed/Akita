package com.dyingbleed.akita.sink

import java.util.Properties

import com.google.common.base.Preconditions._

/**
  * Created by 李震 on 2018/7/4.
  */
class KafkaArgs(properties: Properties) {

  checkNotNull(properties)

  checkNotNull(properties.getProperty(KafkaArgs.KAFKA_SERVERS))
  checkNotNull(properties.getProperty(KafkaArgs.KAFKA_TOPIC))

  val servers = properties.getProperty(KafkaArgs.KAFKA_SERVERS)
  val topic = properties.getProperty(KafkaArgs.KAFKA_TOPIC)

}

object KafkaArgs {

  val KAFKA_SERVERS = "kafka.servers"
  val KAFKA_TOPIC = "kafka.topic"

  def apply(properties: Properties): KafkaArgs = new KafkaArgs(properties)

}
