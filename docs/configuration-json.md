# 完整配置

```json
{
  "extends": ["extends.json"],
  "imports": {},
  "sources": {},
  "name": "demo",
  "input": "<<&.data.demo.json::_id",
  "output": ">>&.stdio.&1::site_id use I",
  "databases": [
    {
      "name": "stdio",
      "driver": "textline"
    },
    {
      "name": "data",
      "driver": "json",
      "path": "./examples/demo/data"
    }
  ],
  "logger": {},
  "querys": {
    "start_date": {">=": "2021-01-01"}
  },
  "defines": {},
  "variables": {},
  "schema": {
    "id": "$.id"
  },
  "pipelines": [],
  "dependency": []
}
```

## extends

载入外部子配置文件，相同配置会覆盖当前配置。

```
type: Array[filename]

例：
extends: ["/data/json/log.json", "database.json"]
```

## imports

声明直接导入python package，导入后会自动注册为计算器，通过该方式可以直接调用python编写的函数。

```
type: Object{name: packagename}

例：
imports: {
    "math": "math"
}

使用：
schema: {
    "sin2": ["@math::sin", 2]
}
```

## sources

声明预处理文件信息，通过预处理文件可把配置文件的部分信息编写在单独文件中，配置文件载入时会用该文件内容替换对应位置，如可把database raw sql写在单独文件中。

```
type: Object{name: filename}

例：
imports: {
    "order": "sql/order.sql"
}

使用：
databases: [{
    "name": "test",
    "virtual_views": [{
        "name": "order",
        "query": "%order", /* 配置文件载入时会自动用sql/order.sql内容替换该处值 */
        "args": []
    }]
}]
```

## name

配置名称。

```
type: String
```

## input

数据输入源。

```
type: String
structure: &.[databasename].[tablename]::[primary_key]

例：
input: "&.json.order.json::id" /* 从数据库json定义的目录中读入order.json文件内容 */
```

## output

数据输出源。

```
type: String
structure: &.[databasename].[tablename]::[primary_key]

例：
output: "&.json.order.json::id" /* 数据输出到数据库json定义的目录的order.json文件中 */
output: "&.stdio.&1::id" /* 控制台输出 */
```

## databases

数据源定义。

```
type: Array[Object{
    name: dataname,
    driver: drivername,
    **[database driver params]
    /* 数据库连接参数请阅读数据库适配 */
}]

例：
databases: [
    {
      "name": "stdio",
      "driver": "textline"
    },
    {
      "name": "data",
      "driver": "json",
      "path": "./examples/demo/data"
    }
]
```

详细请查看数据库配置说明。

## logger

日志配置，和python内置logging配置格式相同，请自行阅读python logging文档。

## querys

数据数据查询条件。

```
type: Object

例：

querys: {
    "start_date": {">=": "2021-01-01"}
}

querys: {
    "m_created_at|datetime": {">=": ["@now|str %Y-%m-%d 22:00:00", "-3d"], "<": ["@now|str %Y-%m-%d 22:00:00"]},
    "date|datetime": {">=": ["@now|str %Y-%m-%d 22:00:00", "-3d"], "<": ["@now|str %Y-%m-%d 22:00:00"]}
}
```

## defines

## variables

## schema

输出数据格式定义。

```
type: Object

例：
schema: {
    "id": "$.id"
}
```

## pipelines

## dependency