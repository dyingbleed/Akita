package com.dyingbleed.akita.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by 李震 on 2017/12/5.
 */
public final class EntryUtils {


    /**
     * CanalEntry 转 JSON
     *
     * 参考：<a href="https://github.com/alibaba/canal/blob/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/EntryProtocol.proto">EntryProtocol.proto</a>
     *
     * @param entry
     * @return
     */
    public static List<String> toJSON(Entry entry) throws InvalidProtocolBufferException {
        RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
        return parseRowChange(rowChange).stream().map(jo -> jo.toJSONString()).collect(Collectors.toList());
    }

    private static List<JSONObject> parseRowChange(RowChange rowChange) {
        List<JSONObject> r = Lists.newLinkedList();

        EventType eventType = rowChange.getEventType();

        for (RowData rowData: rowChange.getRowDatasList()) {
            JSONObject data = new JSONObject();
            JSONObject schema = new JSONObject();

            if (eventType == EventType.INSERT) {
                for (Column column: rowData.getAfterColumnsList()) {
                    data.put(column.getName(), column.getValue());
                    schema.put(column.getName(), column.getMysqlType());
                }
            } else if (eventType == EventType.DELETE) {
                for (Column column: rowData.getBeforeColumnsList()) {
                    data.put(column.getName(), column.getValue());
                    schema.put(column.getName(), column.getMysqlType());
                }
            } else if (eventType == EventType.UPDATE) {
                for (Column column: rowData.getAfterColumnsList()) {
                    data.put(column.getName(), column.getValue());
                    schema.put(column.getName(), column.getMysqlType());
                }
            }

            // 事件类型
            data.put("_EVENT", eventType.name());
            // 表结构
            data.put("_SCHEMA", schema);

            r.add(data);
        }

        return r;
    }

}
