# -*- coding: utf-8 -*-
# 2020/11/19
# create by: snower


import time
import json
try:
    import msgpack
except ImportError:
    msgpack = None

try:
    import pystalkd
except ImportError:
    pystalkd = None
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class JsonSerialize(object):
    def loads(self, data):
        return json.loads(data, encoding="utf-8")

    def dumps(self, value):
        return json.dumps(value, ensure_ascii=False).encode("utf-8")

class MsgpackSerialize(object):
    def __init__(self):
        if not msgpack:
            raise ImportError("msgpack is required")

    def loads(self, data):
        return msgpack.loads(data)

    def dumps(self, value):
        return msgpack.dumps(value)

class BeanstalkQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(BeanstalkQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def filter_limit(self, count, start=None):
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        queue_name = self.name.split(".")
        connection = self.db.ensure_connection()
        connection.watch(queue_name[1] if len(queue_name) > 1 else queue_name[0])

        load_datas, start_time = [], time.time()
        while True:
            job = connection.reserve(timeout=2)
            if not job:
                if not load_datas:
                    start_time = time.time()
                continue

            load_datas.append(self.db.serialize.loads(job.body))
            job.delete()

            if len(load_datas) >= self.db.bulk_size or time.time() - start_time >= self.db.wait_timeout:
                break

        if not self.query:
            if self.limit:
                datas = load_datas[self.limit[0]: self.limit[1]]
            else:
                datas = load_datas
        else:
            index, datas = 0, []
            for data in load_datas:
                if self.limit and (index < self.limit[0] or index > self.limit[1]):
                    continue

                succed = True
                for (key, exp), (value, cmp) in self.query.items():
                    if key not in data:
                        succed = False
                        break
                    if not cmp(data[key], value):
                        succed = False
                        break

                if succed:
                    datas.append(data)
                    index += 1

        if self.orders:
            datas = sorted(datas, key=self.orders[0][0], reverse=True if self.orders[0][1] < 0 else False)
        return datas

class BeanstalkInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(BeanstalkInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        queue_name = self.name.split(".")
        connection = self.db.ensure_connection()
        connection.use(queue_name[1] if len(queue_name) > 1 else queue_name[0])
        for data in self.datas:
            connection.put(self.db.serialize.dumps(data))

class BeanstalkUpdateBuilder(UpdateBuilder):
    pass

class BeanstalkDeleteBuilder(DeleteBuilder):
    pass

class BeanstalkDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 3306,
        "queue": "default",
    }

    SERIALIZES = {
        "json": JsonSerialize,
        "msgpack": MsgpackSerialize,
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.serialize = self.SERIALIZES.get(all_config.pop("serialize") if "serialize" in all_config else "json", JsonSerialize)()
        self.wait_timeout = int(all_config.pop("wait_timeout") if "wait_timeout" in all_config else 30)
        self.bulk_size = int(all_config.pop("bulk_size") if "bulk_size" in all_config else 500)
        super(BeanstalkDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if pystalkd is None:
                raise ImportError("pystalkd>=1.3.0 is required")

            self.connection = pystalkd.Beanstalkd.Connection(**self.config)
            self.connection.ignore("default")
        return self.connection

    def query(self, name, primary_keys=None, fields=()):
        return BeanstalkQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return BeanstalkInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None):
        return BeanstalkUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys=None):
        return BeanstalkDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None