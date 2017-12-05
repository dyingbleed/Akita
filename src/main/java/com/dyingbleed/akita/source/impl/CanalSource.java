package com.dyingbleed.akita.source.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.dyingbleed.akita.source.AkitaSource;
import com.dyingbleed.akita.source.AkitaSourceCallback;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;

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
        } else {
            // 单机模式
            HostAndPort hostAndPort = HostAndPort.fromString(this.canalServers);
            canalConnector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPortOrDefault(11111)),
                    canalDestination,
                    canalUsername,
                    canalPassword
            );
        }

        canalConnector.connect();
        canalConnector.subscribe(canalFilter);
        canalConnector.rollback();

        this.canalConnector = canalConnector;
    }

    @Override
    public void pull(int i, AkitaSourceCallback callback) {
        Message message = canalConnector.getWithoutAck(i);
        long messageId = message.getId();

        if (messageId != -1 && message.getEntries().size() > 0) {
            for (Entry entry: message.getEntries()) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) continue;

                callback.processEntry(entry);
            }
        }

        canalConnector.ack(messageId);
    }

    @Override
    public void destroy() {
        this.canalConnector.disconnect();
        this.canalConnector = null;
    }
}
