# syncany

简单易用的数据同步导出框架。

在构建ETL和报表系统的过程中，存在大量需要从各个子系统收集整理数据的过程，常规写同步脚本非常繁琐。

syncany支持从mysql、mongodb、postgresql、execl等数据源读入数据，同时支持从不同DB，不同数据库类型读入关联数据，完成关联字段转义。

同时支持把数据输出到mysql、mongodb、postgresql、execl等，配合superset等图形报表框架，数分钟并可完成复杂报表分析系统搭建。

# 安装

```
pip3 install syncany
```

# 使用示例

## 数据源

```

#mongodb Database:user Collection:user

{
   "_id": ObjectId("5b750a3f943039305a26c1ec"),
   "username": "13815886467",
   "status": NumberInt(0),
   "nickname": "随遇而安",
   "avatar": "FozMISQ-xrhGX2Z8hf0G2Ihb41rW_132x132_7372.jpeg",
   "gender": NumberInt(2),
   "crts": ISODate("2018-08-16T05:23:11.59Z"),
   "upts": ISODate("2018-08-16T05:23:11.59Z") 
}

# mysql Database:order Table: user_order

CREATE TABLE `user_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `order_id` varchar(24) NOT NULL,
  `uid` varchar(24) NOT NULL,
  `order_type` varchar(32) NOT NULL,
  `total_fee` int(11) NOT NULL DEFAULT '0',
  `status` int(11) NOT NULL,
  `order_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_order_id` (`order_id`),
  KEY `idx_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `user_order`
(`order_id`,`uid`,`order_type`,`total_fee`,`status`,`order_at`)
VALUES ('201806091836318176287421', '5b750a3f943039305a26c1ec', 'groupbuy', 3800, 2001, '2018-08-16 18:23:12');

# json file status.json

[
  {"status": 1001, "verbose_name": "用户下单"},
  {"status": 2001, "verbose_name": "等待支付"},
  {"status": 2101, "verbose_name": "支付中"},
  {"status": 2801, "verbose_name": "支付成功"},
  {"status": 2901, "verbose_name": "支付失败"},
  {"status": 2902, "verbose_name": "支付取消"},
  {"status": 3001, "verbose_name": "等待发货"},
  {"status": 3010, "verbose_name": "准备发货"},
  {"status": 3101, "verbose_name": "配送中"},
  {"status": 3901, "verbose_name": "配送完成"},
  {"status": 3902, "verbose_name": "配送失败"},
  {"status": 4801, "verbose_name": "申请退款"},
  {"status": 4802, "verbose_name": "退款中"},
  {"status": 4901, "verbose_name": "已退款"},
  {"status": 4902, "verbose_name": "退款失败"},
]

```

## 同步json配置

```
{
  "extends": ["json/database.json", "json/log.json"],
  "name": "user_order",
  "input": "&.mysql_order.user_order::order_id",
  "output": "&.exports.用户订单.xlsx#订单列表::订单号",
  "querys": {
    "order_at|datetime": [">=", "<"]
  },
  "schema": {
    "订单号": "$.order_id",
    "用户ID": "$.uid",
    "用户名": ["$.uid|ObjectId", "&.mongo_user.user::_id", "$.username"],
    "用户昵称": ["$.uid|ObjectId", "&.mongo_user.user::_id", "$.nickname"],
    "订单类型": {
      "name": "case_valuer",
      "key": "order_type",
      "case": {
        "groupbuy": "团购订单"
      },
      "default_case": "普通订单"
    },
    "订单金额(元)": ["@div", "$.total_fee", 100],
    "订单状态": ["$.status", "&.json.status.json::status", "$.verbose_name"],
    "下单时间": "$.order_at|datetime %Y-%m-%d %H:%M:%S"
  }
}
```

## 运行导出

```
syncany json/user_order.json --order_at__gte="2018-05-01 00:00:00" --order_at__lt="2018-08-17 00:00:00"
2018-08-16 16:26:43,518 24307 INFO loader: DBLoader <- &.mysql_order.user_order::order_id loader_querys: 1 loader_rows: 1
2018-08-16 16:26:43,520 24307 INFO join_count: 2 join_querys: 2 join_rows: 15
2018-08-16 16:26:43,521 24307 INFO outputer: DBUpdateInsertOutputer -> &.exports.用户订单.xlsx#订单列表::订单号 outputer_querys: 1 outputer_operators: 1 outputer_load_rows: 0 outputer_rows: 1
2018-08-16 16:26:43,521 24307 INFO finish json/user_order.json user_order 569.24ms

```

## 导出结果

用户订单.xlsx

订单号 | 用户ID | 用户名 | 用户昵称 | 订单类型 | 订单金额(元) | 订单状态 | 下单时间
---- | --- | --- | --- | --- | --- | --- | ---
201806091836318176287421 | 5b750a3f943039305a26c1ec | 13815886467 | 随遇而安 | 团购订单 | 38.00 | 等待支付 | 2018-08-16 18:23:12

# License

syncany uses the MIT license, see LICENSE file for the details.