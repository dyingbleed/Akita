package com.dyingbleed.akita.sink

import java.util.Properties

import com.google.common.base.Preconditions.checkNotNull

/**
  * Created by 李震 on 2018/7/4.
  */
class FlumeArgs(properties: Properties) {

  checkNotNull(properties)

  checkNotNull(properties.getProperty(FlumeArgs.FLUME_HOST))
  checkNotNull(properties.getProperty(FlumeArgs.FLUME_PORT))

  val host = properties.getProperty(FlumeArgs.FLUME_HOST)
  val port = properties.getProperty(FlumeArgs.FLUME_PORT).toInt

}

object FlumeArgs {

  val FLUME_HOST = "flume.host"
  val FLUME_PORT = "flume.port"

  def apply(properties: Properties): FlumeArgs = new FlumeArgs(properties)

}
