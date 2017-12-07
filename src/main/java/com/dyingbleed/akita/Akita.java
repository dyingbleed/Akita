package com.dyingbleed.akita;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.dyingbleed.akita.sink.AkitaSink;
import com.dyingbleed.akita.sink.impl.KafkaSink;
import com.dyingbleed.akita.source.AkitaSource;
import com.dyingbleed.akita.source.impl.CanalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by 李震 on 2017/12/4.
 */
public final class Akita implements Runnable {

    /*
     * 常量
     * */

    private static final Logger logger = LoggerFactory.getLogger(Akita.class);

    /*
     * Source
     * */

    private AkitaSource source;

    /*
     * Sink
     * */

    private AkitaSink sink;

    /*
     * 参数
     * */

    private volatile Boolean isRunning;

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
        logger.info("Akita 初始化");
        this.source.init();
        this.sink.init();

        /*
         * 主循环
         * */
        logger.info("Akita 启动");
        isRunning = true;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
        }));

        while (isRunning) {
            this.source.pull(1000, (key, value) -> {
                try {
                    this.sink.push(key, value);
                } catch (Exception e) {
                    logger.error("Akita 发送消息失败：{}", e);
                }
            });
        }

        /*
         * 销毁
         * */
        logger.info("Akita 退出");
        this.source.destroy();
        this.sink.destroy();
    }
}
