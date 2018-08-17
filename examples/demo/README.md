# 导出订单

## 安装&恢复数据

```
mysql -h127.0.0.1 -uroot -p123456 order < user_order.sql
mongorestore -d user -c user user
```

## 运行

```
../../bin/syncany json/user_order.json --order_at__gte="2018-05-01 00:00:00" --order_at__lt="2018-08-17 00:00:00"
```