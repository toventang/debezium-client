# 说明

本项目基于 debezium 1.9 开发的 CDC（变更数据捕获）客户端。
debezium 支持以下数据源：MySQL、MongoDB、PostgreSQL、Oracle、SQL Server、DB2、Cassandra、Vitess。
本项目已内置以下目标数据库：ElasticSearch、Postgresql、MySQL；

## 使用前阅读

* 如果你的系统需要备份数据库，或数据库主备复制，那么本项目非常适合。
* 如果你的系统需要 CQRS 架构，且允许数据最终一致性，那么你可以使用本项目。

如果你不想使用 kafka，可以参看[嵌入式适配器](https://github.com/debezium/debezium/tree/main/debezium-embedded/src/main/java/io/debezium/embedded)

## 示例

1、安装依赖的 docker 镜像 [docker](./docker-compose.yaml)

```console
docker-compose up
```

2、向 debezium connect 写入配置

```console
# 删除名为 dbserver1-connector 的配置
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" http://10.211.55.7:8083/connectors/dbserver1-connector

# 写入配置 register-dbserver1.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://10.211.55.7:8083/connectors/ -d @example/register-dbserver1.json
```

3、编译源码，运行 client 的 docker 镜像，参数 OS 表示编译目标平台

```console
make OS=linux build && make image
```

4、运行 debezium-client 的 docker 容器

```console
docker run --name debeclient --rm debeclient
```

## Prometheus 指标

1. 消费成功的消息数量
2. 消费失败的消息数量
3. 每个消息的处理时间

## Debezium

有关订阅的 GroupID、Topic 或更多设置，可以查看[官方文档](https://debezium.io/documentation/reference/1.9/connectors/index.html)

# 规划功能

- [ ] 表结构同步
- [ ] 延时处理
- [ ] 批量处理
