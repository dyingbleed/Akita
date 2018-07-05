package com.dyingbleed.akita.source

import java.math.BigDecimal
import java.sql.Types

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{Column, EventType, RowChange}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by 李震 on 2018/7/5.
  */
object CanalUtils {

  /**
    * CanalEntry 转 JSON
    *
    * 参考：<a href="https://github.com/alibaba/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/EntryProtocol.proto">EntryProtocol.proto</a>
    *
    * @param entry
    * @return
    */
  def toJSON(entry: CanalEntry.Entry): Seq[CanalMessage] = {
    val tableName = entry.getHeader.getTableName
    parseRowChange(RowChange.parseFrom(entry.getStoreValue)).map(msg => {
      msg.copy(tableName = tableName)
    })
  }

  private def parseRowChange(rowChange: CanalEntry.RowChange): Seq[CanalMessage] = {
    val rows = new mutable.ListBuffer[CanalMessage]

    for (rowData <- rowChange.getRowDatasList.toSeq) {
      rowChange.getEventType match {
        case EventType.INSERT => {
          val (schema, row) = parseColumns(rowData.getAfterColumnsList)
          rows += CanalMessage(null, 'insert, schema, row)
        }
        case EventType.UPDATE => {
          val (schema, row) = parseColumns(rowData.getAfterColumnsList)
          rows += CanalMessage(null, 'update, schema, row)
        }
        case EventType.DELETE => {
          val (schema, row) = parseColumns(rowData.getBeforeColumnsList)
          rows += CanalMessage(null, 'delete, schema, row)
        }
        case _ => {}
      }
    }

    rows
  }

  private def parseColumns(columns: Seq[Column]): (String, String) = {
    val schema = new JSONObject()
    val row = new JSONObject()

    for (column <- columns) {
      val value = column.getValue
      if (value != null && value.trim.nonEmpty) {
        column.getSqlType match {
          case Types.INTEGER | Types.SMALLINT | Types.TINYINT => {
            row.put(column.getName, int2Integer(value.toInt))
          }
          case Types.BIGINT => {
            row.put(column.getName, long2Long(value.toLong))
          }
          case Types.FLOAT => {
            row.put(column.getName, float2Float(value.toFloat))
          }
          case Types.DOUBLE => {
            row.put(column.getName, double2Double(value.toDouble))
          }
          case Types.DECIMAL => {
            row.put(column.getName, new BigDecimal(value))
          }
          case Types.VARCHAR | Types.CHAR | Types.BLOB => {
            row.put(column.getName, value)
          }
          case Types.DATE => {
            row.put(column.getName, value)
          }
          case Types.TIMESTAMP => {
            row.put(column.getName, value)
          }
          case _ => {
            row.put(column.getName, value)
          }
        }
      }
      schema.put(column.getName, column.getMysqlType)
    }

    (schema.toJSONString, row.toJSONString)
  }

}


