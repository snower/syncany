# -*- coding: utf-8 -*-
# 2020/11/19
# create by: snower


import time
import pickle
import json
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class StringSerialize(object):
    def loads(self, data):
        return str(data)

    def dumps(self, value):
        return value


class JsonSerialize(object):
    def loads(self, data):
        return json.loads(data, encoding="utf-8")

    def dumps(self, value):
        return json.dumps(value, ensure_ascii=False).encode("utf-8")


class MsgpackSerialize(object):
    msgpack = None

    def __init__(self):
        try:
            import msgpack
        except ImportError:
            raise ImportError("msgpack is required")
        self.msgpack = msgpack

    def loads(self, data):
        return self.msgpack.loads(data)

    def dumps(self, value):
        return self.msgpack.dumps(value)


class PickleSerialize(object):
    def loads(self, data):
        return pickle.loads(data)

    def dumps(self, value):
        return pickle.dumps(value)


class BeanstalkQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(BeanstalkQueryBuilder, self).__init__(*args, **kwargs)

        self.limit = (0, self.db.bulk_size)

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
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def reserve(self, connection):
        load_datas, start_time = [], time.time()
        while True:
            job = connection.reserve(timeout=2)
            if not job:
                if not load_datas:
                    start_time = time.time()
                    continue
                elif time.time() - start_time >= self.db.wait_timeout:
                    break
                continue

            data = self.db.serialize.loads(job.body)
            if not data:
                continue
            load_datas.append(data)
            job.delete()

            if len(load_datas) >= (self.limit[1] - self.limit[0]) or time.time() - start_time >= self.db.wait_timeout:
                break
        return load_datas

    def commit(self):
        queue_name = self.name.split(".")
        connection = self.db.ensure_connection()
        connection.watch(queue_name[1] if len(queue_name) > 1 else queue_name[0])
        try:
            load_datas = self.reserve(connection)
        finally:
            connection.ignore(queue_name[1] if len(queue_name) > 1 else queue_name[0])

        if not self.query:
            datas = load_datas
        else:
            datas = []
            for data in load_datas:
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

        if self.orders:
            datas = sorted(datas, key=lambda x: x.get(self.orders[0][0]), reverse=True if self.orders[0][1] < 0 else False)
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            self.limit,
            self.orders)


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
            data = self.db.serialize.dumps(data)
            if not data:
                continue
            connection.put()

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


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
        "": StringSerialize,
        "pickle": PickleSerialize,
        "json": JsonSerialize,
        "msgpack": MsgpackSerialize,
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.serialize = self.SERIALIZES.get(all_config.pop("serialize") if "serialize" in all_config else "json", JsonSerialize)()
        self.ignore_serialize_error = all_config.pop("ignore_serialize_error") if "ignore_serialize_error" in all_config else False
        self.wait_timeout = int(all_config.pop("wait_timeout") if "wait_timeout" in all_config else 30)
        self.bulk_size = int(all_config.pop("bulk_size") if "bulk_size" in all_config else 500)
        super(BeanstalkDB, self).__init__(manager, all_config)

        self.connection = None
        if self.ignore_serialize_error:
            def catch_serialize_error(func):
                def _(*args, **kwargs):
                    try:
                        return func(*args, **kwargs)
                    except:
                        return None
                return _
            self.serialize.dumps = catch_serialize_error(self.serialize.dumps)
            self.serialize.loads = catch_serialize_error(self.serialize.loads)

    def ensure_connection(self):
        if not self.connection:
            try:
                import pystalkd
            except ImportError:
                raise ImportError("pystalkd>=1.3.0 is required")

            self.connection = pystalkd.Beanstalkd.Connection(**self.config)
            self.connection.ignore("default")
        return self.connection

    def query(self, name, primary_keys=None, fields=()):
        return BeanstalkQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return BeanstalkInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return BeanstalkUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return BeanstalkDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.connection:
            return
        self.connection.close()
        self.connection = None
