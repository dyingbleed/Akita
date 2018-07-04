package com.dyingbleed.akita

import java.io.FileInputStream
import java.util.Properties

import org.apache.commons.cli._
import org.apache.commons.io.IOUtils

/**
  * Created by 李震 on 2017/12/23.
  */
object Application extends App {

  /*
   * 0. 参数定义
   * */
  val options = new Options
  options.addOption("c", "conf", true, "加载 Properties 配置文件")
  options.addOption("s", "sink", true, "Sink 类型，当前支持 kafka 和 flume")
  val cmd = (new DefaultParser).parse(options, args)


  /*
   * 1. Akita 参数
   * */
  var properties: Properties = null
  var sinkType: AkitaSinkType = null

  /*
   * 2. 加载配置文件
   * */
  if (cmd.hasOption("c")) {
    val path = cmd.getOptionValue("c")
    // 加载 Properties
    properties = loadProperties(path)
  } else {
    val formatter = new HelpFormatter
    formatter.printHelp("-c", options)
  }

  /*
   * 3. 设置 Sink 类型
   * */
  if (cmd.hasOption("s")) {
    try {
      sinkType = AkitaSinkType.valueOf(cmd.getOptionValue("s"))
    } catch {
      case e: IllegalArgumentException => {
        val formatter = new HelpFormatter
        formatter.printHelp("-s", options)
      }
    }
  } else {
    val formatter = new HelpFormatter
    formatter.printHelp("-s", options)
  }

  /*
   * 4. 启动 Akita
   * */
  Akita.apply(properties, sinkType).run()


  private def loadProperties(path: String) = {
    val in = new FileInputStream(path)
    val properties = new Properties
    properties.load(in)
    IOUtils.closeQuietly(in)
    properties
  }

}
