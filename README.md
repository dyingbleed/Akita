# Akita 秋田犬
Ali Canal 数据集成服务

特性：

- 支持集成数据到 Kafka 和 Flume
- 服务自愈
- 基于背压（back prssure）的流量控制
- 数据 at least once 递交保证

## 构建

执行

```bash
mvn clean package
```

在 *target* 目录下生成 *Akita-release.tar.gz* 和 *Akita-release.zip* 文件

## 配置

编辑 *conf/akita.properties* 文件

配置说明：

- canal.servers  Canal 服务地址
- canal.destination  Canal 服务实例
- canal.username  Canal 用户名
- canal.password  Canal  密码
- canal.filter  Canal 表过滤
- kafka.servers  Kafka Broker 服务地址
- kafka.topic  Kafka 主题
- flume.host  Flume Thrift Server 主机地址 
- flume.port  Flume Thrift Server 主机端口号

## 启停服务

启动服务，集成数据到 Kafka

```bash
bin/start-akita.sh kafka
```

启动服务，集成数据到 Flume

```bash
bin/start-akita.sh flume
```

停止

```bash
bin/stop-akita.sh
```

## 消息

```json
{
  "name":"Tom",
  "sex":"m",
  "age": 18
}
```

 - _TABLENAME  表名
 - _EVENT  操作，INSERT UPDATE DELETE
 - _SCHEMA  表结构
