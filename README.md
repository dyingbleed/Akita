# Akita 秋田犬
发布 Ali Canal 到 Apache Kafka 服务

## 构建

执行

```bash
mvn clean package
```

在 *target* 目录下生成 *Akita-release.tar.gz* 和 *Akita-release.zip* 文件

## 配置

编辑 *conf/akita.properties* 文件

## 启停服务

启动

```bash
bin/start-akita.sh
```

停止

```bash
bin/stop-akita.sh
```

消息

```json
{
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
