# -*- coding: utf-8 -*-
# 2020/11/19
# create by: snower

import pickle
import json
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, CacheBuilder, DataBase, DatabaseFactory


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


class RedisQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(RedisQueryBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            prefix_key = ".".join(db_name[1:]).split("#")
        else:
            prefix_key = db_name[0].split("#")
        if len(prefix_key) > 1:
            self.data_type, self.prefix_key = prefix_key[0], "#".join(prefix_key[1:])
        else:
            self.data_type, self.prefix_key = "", prefix_key[0]

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

    def command_keys(self):
        connection = self.db.ensure_connection()
        keys = connection.keys(self.prefix_key + "*")
        datas = []
        for key in keys:
            data = connection.get(key)
            if data is None:
                continue
            data = self.db.serialize.loads(data)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def command_hgetall(self):
        connection = self.db.ensure_connection()
        datas = []
        for key, value in connection.hgetall().items():
            data = self.db.serialize.loads(value)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def load(self):
        if self.data_type == "hash":
            return self.command_hgetall()
        return self.command_keys()

    def commit(self):
        try:
            load_datas = self.load()
            if not self.query:
                datas = load_datas[self.limit[0]: self.limit[1]] if self.limit else load_datas
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
                datas = sorted(datas, key=lambda x: x.get(self.orders[0][0]), reverse=True if self.orders[0][1] < 0 else False)
            return datas
        finally:
            self.db.release_connection()

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            self.limit,
            self.orders)


class RedisInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(RedisInsertBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            prefix_key = ".".join(db_name[1:]).split("#")
        else:
            prefix_key = db_name[0].split("#")
        if len(prefix_key) > 1:
            self.data_type, self.prefix_key = prefix_key[0], "#".join(prefix_key[1:])
        else:
            self.data_type, self.prefix_key = "", prefix_key[0]
        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def command_set(self, key, data):
        connection = self.db.ensure_connection()
        connection.set(self.prefix_key + ":" + key, data, self.db.expire_seconds)

    def command_hset(self, key, data):
        connection = self.db.ensure_connection()
        connection.hset(self.prefix_key, key, data)

    def save(self, key, data):
        if self.data_type == "hash":
            return self.command_hset(key, data)
        self.command_set(key, data)

    def commit(self):
        try:
            for data in self.datas:
                key = ":".join([str(data.get(primary_key, "")) for primary_key in self.primary_keys])
                data = self.db.serialize.dumps(data)
                if not data:
                    continue
                self.save(key, data)

            if self.data_type == "hash":
                connection = self.db.ensure_connection()
                connection.expire(self.prefix_key, self.db.expire_seconds)
            return self.datas
        finally:
            self.db.release_connection()

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


class RedisUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(RedisUpdateBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            prefix_key = ".".join(db_name[1:]).split("#")
        else:
            prefix_key = db_name[0].split("#")
        if len(prefix_key) > 1:
            self.data_type, self.prefix_key = prefix_key[0], "#".join(prefix_key[1:])
        else:
            self.data_type, self.prefix_key = "", prefix_key[0]

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

    def command_set(self, key, data):
        connection = self.db.ensure_connection()
        connection.set(self.prefix_key + ":" + key, data, self.db.expire_seconds)

    def command_hset(self, key, data):
        connection = self.db.ensure_connection()
        connection.hset(self.prefix_key, key, data)
        connection.expire(self.prefix_key, self.db.expire_seconds)

    def save(self, key, data):
        if self.data_type == "hash":
            return self.command_hset(key, data)
        self.command_set(key, data)

    def commit(self):
        try:
            key = ":".join([str(self.update.get(primary_key, "")) for primary_key in self.primary_keys])
            data = self.db.serialize.dumps(self.update)
            if not data:
                return None
            self.save(key, data)
            return self.update
        finally:
            self.db.release_connection()

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            human_repr_object(self.diff_data))


class RedisDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(RedisDeleteBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            prefix_key = ".".join(db_name[1:]).split("#")
        else:
            prefix_key = db_name[0].split("#")
        if len(prefix_key) > 1:
            self.data_type, self.prefix_key = prefix_key[0], "#".join(prefix_key[1:])
        else:
            self.data_type, self.prefix_key = "", prefix_key[0]

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

    def command_keys(self):
        connection = self.db.ensure_connection()
        keys = connection.keys(self.prefix_key + "*")
        datas = []
        for key in keys:
            data = connection.get(key)
            if data is None:
                continue
            data = self.db.serialize.loads(data)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def command_hgetall(self):
        connection = self.db.ensure_connection()
        datas = []
        for key, value in connection.hgetall().items():
            data = self.db.serialize.loads(value)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def load(self):
        if self.data_type == "hash":
            return self.command_hgetall()
        return self.command_keys()

    def command_del(self, key):
        connection = self.db.ensure_connection()
        connection.delete(self.prefix_key + ":" + key)

    def command_hdel(self, key):
        connection = self.db.ensure_connection()
        connection.hdel(self.prefix_key, key)

    def delete(self, key):
        if self.data_type == "hash":
            return self.command_hdel(key)
        self.command_del(key)

    def commit(self):
        try:
            load_datas = self.load()
            if not self.query:
                for data in load_datas:
                    self.delete(data["_key"])
            else:
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
                        self.delete(data["_key"])
            return load_datas
        finally:
            self.db.release_connection()

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()])


class RedisCacheBuilder(CacheBuilder):
    def __init__(self, *args, **kwargs):
        super(RedisCacheBuilder, self).__init__(*args, **kwargs)

    def get(self, key):
        connection = self.db.ensure_connection()
        try:
            return self.db.serialize.loads(connection.get(self.prefix_key + ":" + key))
        except Exception:
            return None
        finally:
            self.db.release_connection()

    def set(self, key, value, exprie_seconds=86400):
        connection = self.db.ensure_connection()
        try:
            data = self.db.serialize.dumps(value)
            connection.set(self.prefix_key + ":" + key, data, exprie_seconds)
        finally:
            self.db.release_connection()

    def delete(self, key):
        connection = self.db.ensure_connection()
        try:
            connection.delete(self.prefix_key + ":" + key)
        finally:
            self.db.release_connection()


class RedisDBFactory(DatabaseFactory):
    def create(self):
        try:
            import redis
        except ImportError:
            raise ImportError("redis>=3.5.3 is required")
        return redis.Redis(**self.config)

    def ping(self, driver):
        return driver.ping()

    def close(self, driver):
        driver.close()


class RedisDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 6709,
        "password": None,
        "db": 0,
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
        self.expire_seconds = all_config.pop("expire_seconds") if "expire_seconds" in all_config else 24 * 60 * 60

        super(RedisDB, self).__init__(manager, all_config)

        self.connection_key = None
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
        if self.connection:
            return self.connection.raw()
        if not self.connection_key:
            self.connection_key = self.get_key(self.config)
            if not self.manager.has(self.connection_key):
                self.manager.register(self.connection_key, RedisDBFactory(self.connection_key, self.config))
        self.connection = self.manager.acquire(self.connection_key)
        return self.connection.raw()

    def release_connection(self):
        if not self.connection:
            return
        self.manager.release(self.connection_key, self.connection)
        self.connection = None

    def query(self, name, primary_keys=None, fields=()):
        return RedisQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return RedisInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return RedisUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return RedisDeleteBuilder(self, name, primary_keys)

    def cache(self, name, prefix_key, config=None):
        return RedisCacheBuilder(name, prefix_key, config)

    def close(self):
        if not self.connection:
            return
        self.connection.raw().close()
        self.connection = None