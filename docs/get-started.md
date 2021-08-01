# 运行要求

```
python3.6及以上
```

# 安装

```bash
pip install syncany
```

or 

```bash
git clone https://github.com/snower/syncany

cd syncany

python3 setup.py install
```


如需数据库支持，请继续安装以下包以支持数据库连接：
```
pymongo>=3.6.1
PyMySQL>=0.8.1
openpyxl>=2.5.0
psycopg2>=2.8.6
elasticsearch>=6.3.1
influxdb>=5.3.1
clickhouse_driver>=0.1.5
redis>=3.5.3
```

# 测试

```bash
git clone https://github.com/snower/syncany

cd syncany

python3 bin/syncany examples/demo/demo.json

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
2021-04-16 11:18:20,868 3292 INFO demo loader: DBLoader <- &.data.demo.json::_id loader_querys: 1 loader_rows: 6
2021-04-16 11:18:20,868 3292 INFO demo join_count: 2 join_querys: 14 join_rows: 13
2021-04-16 11:18:20,868 3292 INFO demo outputer: DBInsertOutputer -> &.stdio.&1::site_id outputer_querys: 0 outputer_operators: 1 outputer_load_rows: 0 outputer_rows: 6
2021-04-16 11:18:20,868 3292 INFO demo finish examples/demo/demo.json demo 148.39ms
```

# 更多示例

[https://github.com/snower/syncany/tree/master/examples](https://github.com/snower/syncany/tree/master/examples)