package com.dyingbleed.akita.stream

import java.util.Properties

import com.google.common.base.Preconditions._

/**
  * Created by 李震 on 2018/7/4.
  */
class CanalArgs(properties: Properties) {

  checkNotNull(properties)

  checkNotNull(properties.getProperty(CanalArgs.CANAL_SERVERS))
  checkNotNull(properties.getProperty(CanalArgs.CANAL_DESTINATION))

  val servers = properties.getProperty(CanalArgs.CANAL_SERVERS)
  val destination = properties.getProperty(CanalArgs.CANAL_DESTINATION)
  val username = properties.getProperty(CanalArgs.CANAL_USERNAME, "")
  val password = properties.getProperty(CanalArgs.CANAL_PASSWORD, "")
  val filter = properties.getProperty(CanalArgs.CANAL_FILTER, ".*\\\\..*")

}

object CanalArgs {

  val CANAL_SERVERS = "canal.servers"
  val CANAL_DESTINATION = "canal.destination"
  val CANAL_USERNAME = "canal.username"
  val CANAL_PASSWORD = "canal.password"
  val CANAL_FILTER = "canal.filter"

  def apply(properties: Properties): CanalArgs = new CanalArgs(properties)

}
