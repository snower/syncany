# 数据库

数据库适配支持，提供数据输入输出源。

```json
{
  "databases": [
    {
      "name": "stdio",
      "driver": "textline"
    },
    {
      "name": "mysql_analysis",
      "driver": "mysql",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "root",
      "passwd": "123456",
      "db": "statistics",
      "charset":"utf8mb4"
    },
    {
      "name": "mongo_trading",
      "driver": "mongo",
      "host": "127.0.0.1",
      "port": 27017,
      "db": "trading"
    },
    {
      "name": "exports",
      "driver": "execl",
      "path": "./exports"
    },
    {
      "name": "json",
      "driver": "json",
      "path": "./json"
    }
  ]
}
```

## memory

从内存输入或者输出到内存暂存区，可用于形成级联处理。

配置参数：

- name 名称

## textline

文本文件行输出，可指定用csv或json格式化。

配置参数：

- name 名称
- path 文件保存目录  
- format 格式化方式（支持csv、json、richtable，默认制表符间隔）

注：表名可用&1 &2输出到控制台。

## mongo

MongoDB数据库支持。

配置参数：

- name 名称
- db 数据库名
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/mongodb/mongo-python-driver](https://github.com/mongodb/mongo-python-driver) 连接参数信息。

## mysql

MySQL数据库支持。

配置参数：

- name 名称
- db 数据库名
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/PyMySQL/PyMySQL](https://github.com/PyMySQL/PyMySQL) 连接参数信息。

virtual_views虚拟视图参数：

- name 视图名称
- name_match 视图名称正则匹配（可选）
- query 视图raw sql查询语句
- args 视图查询参数（数组，顺序需和sql语句中条件占位符号一致）

虚拟视图可在需要用原始sql查询语句查询数据库时使用，定义的虚拟视图可当作一个数据库下的标准表在输入指令中使用。

## postgresql

PostgreSQL数据库支持。

配置参数：

- name 名称
- db 数据库名
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/psycopg/psycopg2](https://github.com/psycopg/psycopg2) 连接参数信息。

## clickhouse

Clickhouse数据库支持。

配置参数：

- name 名称
- database 数据库名
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/mymarilyn/clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) 连接参数信息。

## influxdb

Influxdb数据库支持，可以用于时序数据分析。

配置参数：

- name 名称
- db 数据库名
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/influxdata/influxdb-python](https://github.com/influxdata/influxdb-python) 连接参数信息。

## elasticsearch

Elasticsearch数据库支持。

配置参数：

- name 名称
- virtual_views 虚拟视图配置信息

其它参数请查看 [https://github.com/elastic/elasticsearch-py](https://github.com/elastic/elasticsearch-py) 连接参数信息。

## execl

读写execl文件支持。

配置参数：

- name 名称
- path execl文件保存目录

## csv

读写csv文件支持。

配置参数：

- name 名称
- path csv文件保存目录

注：表名可用&1 &2输出到控制台。

## json

读写json文件支持。

配置参数：

- name 名称
- path json文件保存目录

注：表名可用&1 &2输出到控制台。

## beanstalk

Beanstalk队列支持。

配置参数：

- name 名称
- serialize  序列化方式（支持直接字符串、pickle、json、msgpack）
- ignore_serialize_error 是否忽略序列化错误（默认False）
- wait_timeout 等待超时时间（默认30秒）
- bulk_size 每批最大条数（默认500）

其它参数请查看 [https://github.com/menezes-/pystalkd](https://github.com/menezes-/pystalkd) 连接参数信息。

## redis

Redis数据库支持。

配置参数：

- name 名称
- serialize  序列化方式（支持直接字符串、pickle、json、msgpack）
- ignore_serialize_error 是否忽略序列化错误（默认False）
- expire_seconds 默认过期时间（默认1天）

其它参数请查看 [https://github.com/andymccurdy/redis-py](https://github.com/andymccurdy/redis-py) 连接参数信息。

