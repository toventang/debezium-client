# 说明

本项目基于 debezium 的 CDC（变更数据捕获）客户端，可支持文档型存储：ElasticSearch、MongoDB 等，关系型数据库：Postgresql、MySQL、MSSQL、Oracle、Cassandra 等。与阿里开源的 Canal 功能相同，但 debezium 支持更多的数据库。

如果你的系统需要备份数据库，或数据库主备复制，那么本项目非常适合。

如果你的系统需要 CQRS 架构，且允许数据最终一致性，那么你可以使用本项目。

如果你的系统要求必须达到完全一致性，那么你可能更适合采用 EventSoucing（事件溯源）设计模式，因为本项目的 Change Data Capture 设计模式只能做到准实时复制。

本项目基于 kafka 保证数据顺序一致性，如果你不需要集成 kafka，可以参看[嵌入式转接器](https://github.com/debezium/debezium/tree/master/debezium-embedded/src/main/java/io/debezium/embedded)

## 示例

在 [examples](./examples) 中已有 ElasticSearch 的示例，只需提供源数据（基于 Kafka）地址和目标数据（NOSQL、RDB）地址，即可自动捕获源数据的变更项，并写入目标数据存储。

1、安装依赖的 docker 镜像 [docker](./docker-compose.yaml)

```console
docker-compose up
```

2、向 debezium connect 写入配置

```console
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://192.168.50.199:8083/connectors/ -d @register-postgres.json
```

3、编译源码，运行 client 的 docker 镜像，参数 OS 表示编译目标平台

```console
make OS=darwin build && make image
```

4、运行 debezium-client 的 docker 容器

```console
docker run --name debeclient -ti debeclient -KAFKA_ADDRESS=192.168.50.199:9092 \
          -KAFKA_GROUPID=cdc.catalogs.subscriber \
          -KAFKA_TOPICS=catalogdbs.public.catalogs,catalogdbs.public.templates \
          -DST_TYPE=elasticsearch -DST_ADDRESS=http://192.168.50.138:9200 -DST_TIMEOUT=5
          -FIELD_MAPPING={"public.catalogs":{"created_at":"createdAt","updated_at":"updatedAt"}
```

**参数说明：**
KAFKA_ADDRESS 是指 debezium 服务端监听数据更改后写入的 kafka 地址；
KAFKA_GROUPID 是指消费 kafka 消息的 groupid，必须保证 groupid 是唯一的；
KAFKA_TOPICS 是指要消费的 kafka topic，多个主题以 "," 分隔，必须与服务端一致。

DST_TYPE 是指目标数据库类型，暂时支持 Elasticsearch、Postgresql；
DST_ADDRESS 是指目标数据库地址；
DST_TIMEOUT 表示写入目标数据库的超时设置，默认 5 秒；
FIELD_MAPPING 是指将源数据库字段名更改为另一名称写入，以 json 格式表示“表和字段”的结构，格式为：{"表名":{"字段1":"字段1映射","字段2":"字段2映射",...},...}。

## Debezium

有关订阅的 GroupID、Topic 或更多设置，可以查看[官方文档](https://debezium.io/documentation/reference/0.10/connectors/index.html)