package com.dyingbleed.akita

import java.util.Properties

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RestartSink, RestartSource, Sink, Source}
import com.dyingbleed.akita.AkitaSinkType.{flume, kafka}
import com.dyingbleed.akita.stream._

/**
  * Created by 李震 on 2017/12/23.
  */
class Akita(properties: Properties, sinkType: AkitaSinkType) extends Runnable {

  override def run(): Unit = {

    implicit val system: ActorSystem = ActorSystem("Akita")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        system.terminate()
      }
    })

    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val source: Source[String, NotUsed] = canalSource
    val sink: Sink[String, NotUsed] = sinkType match {
      case kafka => kafkaSink
      case flume => flumeSink
    }
    source.runWith(sink)

  }

  private def canalSource: Source[String, NotUsed] = {
    RestartSource.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Source.fromGraph(new CanalSource(CanalArgs(properties)))
      }
    }
  }

  private def kafkaSink: Sink[String, NotUsed] = {
    RestartSink.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Sink.fromGraph(new KafkaSink(KafkaArgs.apply(properties)))
      }
    }
  }

  private def flumeSink: Sink[String, NotUsed] = {
    RestartSink.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Sink.fromGraph(new FlumeSink(FlumeArgs(properties)))
      }
    }
  }

}

object Akita {

  def apply(properties: Properties, sinkType: AkitaSinkType): Akita = new Akita(properties, sinkType)

}