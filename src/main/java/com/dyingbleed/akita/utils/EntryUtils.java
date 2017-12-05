package com.dyingbleed.akita.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Created by 李震 on 2017/12/5.
 */
public final class EntryUtils {


    /**
     * CanalEntry 转 JSON
     *
     * 参考：<a href="https://github.com/alibaba/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/EntryProtocol.proto">EntryProtocol.proto</a>
     *
     * @param entry Canal Entry
     * @return JSON
     */
    public static JSONObject toJSON(Entry entry) throws InvalidProtocolBufferException {
        JSONObject jo = new JSONObject();

        // 协议头部信息
        Header header = entry.getHeader();
        jo.put("header", parseHeader(header));


        RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
        jo.put("storeValue", parseRowChange(rowChange));

        return jo;
    }

    private static JSONObject parseHeader(Header header) {
        JSONObject jo = new JSONObject();

        jo.put("tableName", header.getTableName());
        // TODO: 2017/12/5  暂时只解析表名

        return jo;
    }

    private static JSONObject parseRowChange(RowChange rowChange) {
        JSONObject jo = new JSONObject();

        EventType eventType = rowChange.getEventType();
        jo.put("eventType", eventType.toString());

        JSONArray ja = new JSONArray();
        for (RowData rowData: rowChange.getRowDatasList()) {
            if (eventType == EventType.INSERT) {
                for (Column column: rowData.getAfterColumnsList()) {
                    ja.add(parseColumn(column));
                }
            } else if (eventType == EventType.DELETE) {
                for (Column column: rowData.getBeforeColumnsList()) {
                    ja.add(parseColumn(column));
                }
            } else if (eventType == EventType.UPDATE) {
                for (Column column: rowData.getAfterColumnsList()) {
                    ja.add(parseColumn(column));
                }
            }
        }
        jo.put("rowDatas", ja);

        return jo;
    }

    private static JSONObject parseColumn(Column column) {
        JSONObject jo = new JSONObject();

        jo.put("index", column.getIndex());
        jo.put("name", column.getName());
        jo.put("value", column.getValue());
        jo.put("sqlType", column.getSqlType());

        return jo;
    }

}
