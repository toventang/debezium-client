# 说明

本项目基于 debezium 的 CDC（变更数据捕获）客户端，可支持文档型存储：ElasticSearch、MongoDB 等，关系型数据库：Postgresql、MySQL、MSSQL、Oracle、Cassandra 等。与阿里开源的 Canal 功能相同，但 debezium 支持更多的数据库。

如果你的系统需要备份数据库，或数据库主备复制，那么本项目非常适合。

如果你的系统需要 CQRS 架构，且允许数据最终一致性，那么你可以使用本项目。

如果你的系统要求必须达到完全一致性，那么你可能更适合采用 EventSoucing（事件溯源）设计模式，因为本项目的 Change Data Capture 设计模式只能做到准实时复制。

本项目基于 kafka 保证数据顺序一致性，如果你不需要集成 kafka，可以参看[嵌入式转接器](https://github.com/debezium/debezium/tree/master/debezium-embedded/src/main/java/io/debezium/embedded)

## 示例

在 [examples](./examples) 中已有 ElasticSearch 的示例，只需提供源数据（基于 Kafka）地址和目标数据（NOSQL、RDB）地址，即可自动捕获源数据的变更项，并写入目标数据存储。

1、安装 [docker](./examples/docker-compose.yaml)

```console
docker-compose up
```

2、向 debezium 写入配置

```console
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://192.168.50.199:8083/connectors/ -d @register-postgres.json
```

3、运行程序

```console
go run ./elasticsearch/main.go
```

## Debezium

有关订阅的 GroupID、Topic 或更多设置，可以查看[官方文档](https://debezium.io/documentation/reference/0.10/connectors/index.html)