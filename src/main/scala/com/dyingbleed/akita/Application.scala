package com.dyingbleed.akita

import java.io.FileInputStream
import java.util.Properties

import org.apache.commons.cli._
import org.apache.commons.io.IOUtils

/**
  * Created by 李震 on 2017/12/23.
  */
object Application extends App {

  val options = new Options

  options.addOption("p", true, "加载 Properties 文件")

  val parser = new DefaultParser
  val cmd = parser.parse(options, args)

  if (cmd.hasOption("p")) {
    val path = cmd.getOptionValue("p")
    // 加载 Properties
    val properties = loadProperties(path)
    // 初始化并启动 Akita
    Akita(properties).run()
  }
  else {
    val formatter = new HelpFormatter
    formatter.printHelp("-p", options)
  }

  private def loadProperties(path: String) = {
    val in = new FileInputStream(path)
    val properties = new Properties
    properties.load(in)
    IOUtils.closeQuietly(in)
    properties
  }

}
