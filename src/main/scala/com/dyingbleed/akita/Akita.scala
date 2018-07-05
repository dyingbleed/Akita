package com.dyingbleed.akita

import java.util.Properties

import scala.concurrent.duration._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RestartSink, RestartSource, Sink, Source}
import com.dyingbleed.akita.sink.{FlumeArgs, FlumeSink, KafkaArgs, KafkaSink}
import com.dyingbleed.akita.source.{CanalArgs, CanalMessage, CanalSource}

/**
  * Created by 李震 on 2017/12/23.
  */
class Akita(properties: Properties, sinkType: Symbol) extends Runnable {

  override def run(): Unit = {

    implicit val system: ActorSystem = ActorSystem("Akita")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        system.terminate()
      }
    })

    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val source: Source[CanalMessage, NotUsed] = canalSource
    val sink: Sink[CanalMessage, NotUsed] = sinkType match {
      case 'kafka => kafkaSink
      case 'flume => flumeSink
    }
    source.runWith(sink)

  }

  private def canalSource: Source[CanalMessage, NotUsed] = {
    RestartSource.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Source.fromGraph(new CanalSource(CanalArgs(properties)))
      }
    }
  }

  private def kafkaSink: Sink[CanalMessage, NotUsed] = {
    RestartSink.withBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 5.seconds,
      randomFactor = 0.2
    ) { () => {
        Sink.fromGraph(new KafkaSink(KafkaArgs.apply(properties)))
      }
    }
  }

  private def flumeSink: Sink[CanalMessage, NotUsed] = {
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

  def apply(properties: Properties, sinkType: Symbol): Akita = new Akita(properties, sinkType)

}