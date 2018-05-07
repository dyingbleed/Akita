# Akita 秋田犬
发布 Ali Canal 到 Apache Kafka 服务

特性：

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

## 启停服务

启动

```bash
bin/start-akita.sh
```

停止

```bash
bin/stop-akita.sh
```

## 消息

```json
{
  "_TABLENAME": "users",
  "_EVENT":"INSERT",
  "_SCHEMA":{
    "name":"varchar(32)",
    "sex":"char(1)",
    "birthday":"date"
  },
  "name":"Tom",
  "sex":"m",
  "birthday":"1997-12-06"
}
```

 - _TABLENAME  表名
 - _EVENT  操作，INSERT UPDATE DELETE
 - _SCHEMA  表结构
