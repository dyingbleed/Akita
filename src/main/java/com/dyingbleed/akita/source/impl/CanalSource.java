package com.dyingbleed.akita.source.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dyingbleed.akita.source.AkitaSource;
import com.dyingbleed.akita.source.AkitaSourceCallback;
import com.dyingbleed.akita.utils.EntryUtils;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by 李震 on 2017/12/4.
 */
public class CanalSource implements AkitaSource {

    /*
     * 常量
     * */

    private static final String SERVER_SEPERATOR = ",";

    // 连续异常重启次数
    private static final int RETRY_TIMES = 5;

    private static final Logger logger = LoggerFactory.getLogger(CanalSource.class);


    /*
     * Canal 参数
     * */

    private String canalServers;

    private String canalDestination;

    private String canalUsername;

    private String canalPassword;

    private String canalFilter;

    /*
     * Canal 连接
     * */

    private CanalConnector canalConnector;


    public CanalSource(String canalServers, String canalDestination, String canalUsername, String canalPassword, String canalFilter) {
        this.canalServers = canalServers;
        this.canalDestination = canalDestination;
        this.canalUsername = canalUsername;
        this.canalPassword = canalPassword;
        this.canalFilter = canalFilter;
    }

    @Override
    public void init() {
        CanalConnector canalConnector;

        if (this.canalServers.contains(SERVER_SEPERATOR)) {
            // 集群模式
            logger.info("使用 Canal Client 集群模式");
            List<InetSocketAddress> hosts = Lists.newLinkedList();
            for (String server: StringUtils.split(this.canalServers, SERVER_SEPERATOR)) {
                HostAndPort hostAndPort = HostAndPort.fromString(server);
                hosts.add(new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPortOrDefault(11111)));
            }
            canalConnector = CanalConnectors.newClusterConnector(
                    hosts,
                    canalDestination,
                    canalUsername,
                    canalPassword
            );
            logger.info("创建 Canal 连接：{}, {}, {}, {}", this.canalServers, this.canalDestination, this.canalUsername, this.canalPassword);
        } else {
            // 单机模式
            logger.info("使用 Canal Client 单机模式");
            HostAndPort hostAndPort = HostAndPort.fromString(this.canalServers);
            canalConnector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPortOrDefault(11111)),
                    canalDestination,
                    canalUsername,
                    canalPassword
            );
            logger.info("创建 Canal 连接：{}, {}, {}, {}", this.canalServers, this.canalDestination, this.canalUsername, this.canalPassword);
        }

        logger.info("开始连接 Canal Server");
        canalConnector.connect();
        logger.info("订阅 Canal Server {}", canalFilter);
        canalConnector.subscribe(canalFilter);
        logger.info("回滚 Canal Server");
        canalConnector.rollback();

        this.canalConnector = canalConnector;
    }

    @Override
    public void pull(int i, AkitaSourceCallback callback) {
        long messageId = -1;

        try {
            Message message = canalConnector.getWithoutAck(i);
            messageId = message.getId();

            if (messageId != -1 && message.getEntries().size() > 0) {
                logger.info("收到 Canal Server 消息 {}", messageId);
                for (Entry entry: message.getEntries()) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) continue;

                    String key = entry.getHeader().getTableName();
                    try {
                        for (String value: EntryUtils.toJSON(entry)) callback.processEntry(key, value);
                    } catch (Exception e) {
                        logger.error("CanalSource 处理 Entiry 失败", e);
                    }
                }
            }

            canalConnector.ack(messageId);
        } catch (CanalClientException e) {
            logger.error("CanalClient 异常，重启试试", e);
            this.destroy();
            this.init();
        } catch (Exception e) {
            logger.error("CanalSource 处理信息失败，数据回滚", e);
            if (messageId != -1) canalConnector.rollback(messageId);
        }
    }

    @Override
    public void destroy() {
        this.canalConnector.disconnect();
        this.canalConnector = null;
    }
}
