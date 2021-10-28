# syncany

简单易用的数据同步转换导出框架。

在构建ETL和报表系统的过程中，存在大量需要从各个子系统收集整理数据的过程，常规写同步脚本非常繁琐。

syncany支持mysql、mongodb、postgresql、redis、elasticsearch、influxdb、clickhouse、execl、beanstalk等数据源读入或写结果数据，同时支持从不同DB不同数据库类型读入关联数据，并对数据进行转换计算，之后配合superset等图形报表框架，快速完成复杂报表分析系统搭建。

[https://pysyncany.readthedocs.io/](https://pysyncany.readthedocs.io/)

# 安装

```
pip3 install syncany
```

# 配置示例


```json
{
  "extends": ["examples/demo/json/database.json", "examples/demo/json/log.json"],
  "name": "demo",
  "input": "<<&.data.demo.json::_id",
  "output": ">>&.stdio.&1::site_id use I",
  "querys": {
    "start_date": {">=": "2021-01-01"}
  },
  "schema": {
    "site_id": ["#yield", "$.sites", [
      ":#aggregate", "$.*|int", "$$.*|int"
    ]],
    "site_name": ["#yield", "$.sites", [
      ":#aggregate", "$.*|int", [
        "$$.*|int", ["&.data.sites.json::site_id", {"status|int": {">=": 0}}], ":$.name"
      ]
    ]],
    "site_amount": ["#yield", "$.sites", [
      ":#aggregate", "$.*|int", [
        "$$.*|int", "&.data.orders.json::site_id", [
          ":#foreach|int", "$.*|array", [
            "#if", ["@lte", "$.status", 0], ["#make", {"value": "$.amount"}], "#continue"
          ], [
            ":@sum", "$.*|array", "value"
          ]
        ]
      ]
    ]],
    "timeout_at": ["#yield", "$.sites", [
      ":#aggregate", "$.*|int", {
        "#case": "$$$.vip_type",
        "1": "$.timeout_at",
        "#end": "$$$.rules.:0.timeout_time"
      }
    ]],
    "vip_timeout_at": ["#yield", "$.sites", [
      ":#aggregate", "$.*|int", {
        "#match": "$$$.vip_type",
        "/2/": "$$.vip_timeout_at",
        "#end": "$$$.rules.:0.timeout_time"
      }
    ]]
  }
}
```

## 运行示例

```bash
# 克隆并在项目目录下执行即可看到输出，具体输入数据请查看examples/demo/data
python3 ./bin/syncany examples/demo/demo.json --start_date__gte="2021-01-01"
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ site_id                     ┃site_name                       ┃site_amount                            ┃timeout_at                         ┃vip_timeout_at                                 ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 8                           │黄豆网                          │17.04                                  │16:00:00                           │11:00:00                                       │
│ 15                          │青菜网                          │7.2                                    │15:00:00                           │10:00:00                                       │
│ 21                          │去啥网                          │0                                      │16:00:00                           │11:00:00                                       │
│ 26                          │汽车网                          │0                                      │16:00:00                           │11:00:00                                       │
│ 28                          │火箭网                          │0                                      │15:00:00                           │10:00:00                                       │
│ 34                          │卫星网                          │11.2                                   │16:40:00                           │11:20:00                                       │
└─────────────────────────────┴────────────────────────────────┴───────────────────────────────────────┴───────────────────────────────────┴───────────────────────────────────────────────┘
2021-03-18 17:33:54,570 2377 INFO demo loader: DBLoader <- &.data.demo.json::_id loader_querys: 1 loader_rows: 6
2021-03-18 17:33:54,570 2377 INFO demo join_count: 2 join_querys: 14 join_rows: 13
2021-03-18 17:33:54,571 2377 INFO demo outputer: DBInsertOutputer -> &.stdio.&1::site_id outputer_querys: 0 outputer_operators: 1 outputer_load_rows: 0 outputer_rows: 6
2021-03-18 17:33:54,571 2377 INFO demo finish examples/demo/demo.json demo 96.28ms
```

# License

syncany uses the MIT license, see LICENSE file for the details.