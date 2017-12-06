package com.dyingbleed.akita.sink.impl;

import com.dyingbleed.akita.sink.AkitaSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by 李震 on 2017/12/4.
 */
public class KafkaSink implements AkitaSink {

    /*
     * 常量
     * */

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    /*
     * Kafka 参数
     * */

    private String servers;

    private String topic;

    /*
     * Kafka Producer
     * */

    private KafkaProducer<String, String> kafkaProducer;


    public KafkaSink(String servers, String topic) {
        this.servers = servers;
        this.topic = topic;
    }

    @Override
    public void init() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.servers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.kafkaProducer = new KafkaProducer<>(properties);
        logger.info("创建 Kafka 生产者：{}", this.servers);
    }

    @Override
    public void push(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, key, value);
        this.kafkaProducer.send(record);
    }

    @Override
    public void destroy() {
        this.kafkaProducer.close();
        this.kafkaProducer = null;
    }
}
