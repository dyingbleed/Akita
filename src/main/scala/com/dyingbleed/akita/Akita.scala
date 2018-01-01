package com.dyingbleed.akita

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.dyingbleed.akita.actor.{CanalSourceActor, KafkaSinkActor}

/**
  * Created by 李震 on 2017/12/23.
  */
class Akita(properties: Properties) extends Runnable {

  private var isRunner = true

  override def run(): Unit = {
    val actorSystem = ActorSystem.create()

    actorSystem.actorOf(Props(
      classOf[KafkaSinkActor],
      properties.getProperty("kafka.servers"),
      properties.getProperty("kafka.topic")
    ), "kafka")
    actorSystem.actorOf(Props(
      classOf[CanalSourceActor],
      properties.getProperty("canal.servers"),
      properties.getProperty("canal.destination", "akita"),
      properties.getProperty("canal.username", ""),
      properties.getProperty("canal.password", ""),
      properties.getProperty("canal.filter", ".*\\..*")

    ), "canal")

    Runtime.getRuntime.addShutdownHook(new Thread() {

      override def run(): Unit = {
        isRunner = false
      }

    })
    // 主循环
    while (this.isRunner) {
      Thread.sleep(1000)
    }

    actorSystem.terminate()
  }

}

object Akita {

  def apply(properties: Properties): Akita = new Akita(properties)

}