package com.dyingbleed.akita

import java.util.Properties

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer}
import akka.stream.scaladsl.{RestartSink, RestartSource, Sink, Source}
import com.dyingbleed.akita.stream.{CanalSource, KafkaSink}

/**
  * Created by 李震 on 2017/12/23.
  */
class Akita(properties: Properties) extends Runnable {

  override def run(): Unit = {

    implicit val system: ActorSystem = ActorSystem("Akita")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        system.terminate()
      }
    })

    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val source: Source[String, NotUsed] = RestartSource.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Source.fromGraph(new CanalSource(
          properties.getProperty("canal.servers"),
          properties.getProperty("canal.destination", "akita"),
          properties.getProperty("canal.username", ""),
          properties.getProperty("canal.password", ""),
          properties.getProperty("canal.filter", ".*\\..*")
        ))
      }
    }

    val sink: Sink[String, NotUsed] = RestartSink.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Sink.fromGraph(new KafkaSink(
          properties.getProperty("kafka.servers"),
          properties.getProperty("kafka.topic")
        ))
      }
    }

    source.runWith(sink)

  }

}

object Akita {

  def apply(properties: Properties): Akita = new Akita(properties)

}