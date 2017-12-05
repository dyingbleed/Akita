package com.dyingbleed.akita;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.dyingbleed.akita.sink.AkitaSink;
import com.dyingbleed.akita.sink.impl.KafkaSink;
import com.dyingbleed.akita.source.AkitaSource;
import com.dyingbleed.akita.source.impl.CanalSource;
import com.dyingbleed.akita.utils.EntryUtils;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Properties;

/**
 * Created by 李震 on 2017/12/4.
 */
public final class Akita implements Runnable {

    /*
     * Source
     * */

    private AkitaSource source;

    /*
     * Sink
     * */

    private AkitaSink sink;


    /**
     * 创建 Akita 实例
     *
     * @param properties
     * @return
     */
    public static Akita createInstance(Properties properties) {
        return new Akita(properties);
    }

    private Akita(Properties properties) {
        // 创建 Source
        this.source = createSource(properties);

        // 创建 Sink
        this.sink = createSink(properties);
    }

    private AkitaSource createSource(Properties properties) {
        String servers = properties.getProperty("canal.servers", AddressUtils.getHostIp().concat(":").concat("11111"));
        String destination = properties.getProperty("canal.destination", "akita");
        String username = properties.getProperty("canal.username", "");
        String password = properties.getProperty("canal.password", "");
        String filter = properties.getProperty("canal.filter", ".*\\..*");

        return new CanalSource(servers, destination, username, password, filter);
    }

    private AkitaSink createSink(Properties properties) {
        String servers = properties.getProperty("kafka.servers", AddressUtils.getHostIp().concat(":").concat("9092"));
        String topic = properties.getProperty("kafka.topic", "");

        return new KafkaSink(servers, topic);
    }


    @Override
    public void run() {
        /*
         * 初始化
         * */

        this.source.init();

        this.sink.init();

        /*
         * 主循环
         * */
        Boolean running = true;
        while (running) {
            this.source.pull(1000, (entry) -> {
                try {
                    JSONObject json = EntryUtils.toJSON(entry);
                    String key = json.getJSONObject("header").getString("tableName");
                    this.sink.push(key, json.toJSONString());
                } catch (Exception e) {
                    e.printStackTrace();
                    // TODO: 2017/12/5 处理失败，丢弃
                }
            });
        }

        /*
         * 销毁
         * */

        this.source.destroy();

        this.sink.destroy();
    }
}
