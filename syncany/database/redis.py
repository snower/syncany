# -*- coding: utf-8 -*-
# 2020/11/19
# create by: snower

import pickle
import json
from ..utils import human_repr_object, sorted_by_keys
from ..taskers.context import TaskerContext
from ..taskers.iterator import TaskerDataIterator
from .database import Cmper, QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, CacheBuilder, DataBase, DatabaseFactory


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


class RedisCommand(object):
    def __init__(self, serialize, base_prefix, name):
        self.serialize = serialize
        db_name = name.split(".")
        if len(db_name) > 1:
            prefix_key = ".".join(db_name[1:]).split("#")
        else:
            prefix_key = db_name[0].split("#")
        if len(prefix_key) > 1:
            self.data_type, self.prefix_key = prefix_key[0], base_prefix + "#".join(prefix_key[1:])
        else:
            self.data_type, self.prefix_key = "list", base_prefix + prefix_key[0]

    def command_keys(self, connection):
        keys = connection.keys(self.prefix_key + "*")
        datas = []
        for key in keys:
            data = connection.get(key)
            if data is None:
                continue
            data = self.serialize.loads(data)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def command_lrange(self, connection):
        datas = []
        for value in connection.lrange(self.prefix_key, 0, -1):
            data = self.serialize.loads(value)
            if not data:
                continue
            datas.append(data)
        return datas

    def command_hgetall(self, connection):
        datas = []
        for key, value in connection.hgetall(self.prefix_key).items():
            data = self.serialize.loads(value)
            if not data:
                continue
            if isinstance(data, dict):
                data["_key"] = key
            else:
                data = {"_key": key, "_value": data}
            datas.append(data)
        return datas

    def load_datas(self, connection):
        if self.data_type == "hash":
            return self.command_hgetall(connection)
        if self.data_type == "list":
            return self.command_lrange(connection)
        return self.command_keys(connection)

    def command_set(self, connection, primary_keys, datas, expire_seconds):
        for data in datas:
            key = ":".join([str(data.get(primary_key, "")) for primary_key in primary_keys])
            data = self.serialize.dumps(data)
            connection.set(self.prefix_key + ":" + key, data, expire_seconds)

    def command_lrpush(self, connection, primary_keys, datas, expire_seconds):
        datas = tuple(self.serialize.dumps(data) for data in datas)
        if not datas:
            return
        connection.rpush(self.prefix_key, *datas)
        connection.expire(self.prefix_key, expire_seconds)

    def command_hset(self, connection, primary_keys, datas, expire_seconds):
        for data in datas:
            key = ":".join([str(data.get(primary_key, "")) for primary_key in primary_keys])
            data = self.serialize.dumps(data)
            connection.hset(self.prefix_key, key, data)
        connection.expire(self.prefix_key, expire_seconds)

    def save_datas(self, connection, primary_keys, datas, expire_seconds):
        if self.data_type == "hash":
            return self.command_hset(connection, primary_keys, datas, expire_seconds)
        if self.data_type == "list":
            return self.command_lrpush(connection, primary_keys, datas, expire_seconds)
        self.command_set(connection, primary_keys, datas, expire_seconds)

    def command_del_keys(self, connection):
        keys = connection.keys(self.prefix_key + "*")
        for key in keys:
            connection.delete(key)

    def command_del_list(self, connection):
        connection.delete(self.prefix_key)

    def command_del_hash(self, connection):
        connection.delete(self.prefix_key)

    def delete_datas(self, connection):
        if self.data_type == "hash":
            return self.command_del_hash(connection)
        if self.data_type == "list":
            return self.command_del_list(connection)
        self.command_del_keys(connection)


class RedisQueryBuilder(QueryBuilder, RedisCommand):
    def __init__(self, *args, **kwargs):
        QueryBuilder.__init__(self, *args, **kwargs)
        RedisCommand.__init__(self, self.db.serialize, self.db.base_prefix, self.name)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        tasker_context, iterator, iterator_name, datas = None, None, None, None
        if self.limit and (self.query or self.orders):
            tasker_context = TaskerContext.current()
            if tasker_context:
                iterator_name = "redis::" + self.name
                iterator = tasker_context.get_iterator(iterator_name)
                if iterator and iterator.offset == self.limit[0]:
                    datas, iterator.offset = iterator.datas, self.limit[1]

        if not datas:
            try:
                connection = self.db.ensure_connection()
                load_datas = self.load_datas(connection)
                if not self.query:
                    datas = load_datas[:]
                else:
                    datas = []
                    for data in load_datas:
                        succed = True
                        for key, exp, value, cmp in self.query:
                            if key not in data:
                                succed = False
                                break
                            if not cmp(data[key], value):
                                succed = False
                                break
                        if succed:
                            datas.append(data)

                if self.orders:
                    datas = sorted_by_keys(datas, keys=[(key, True if direct < 0 else False)
                                                        for key, direct in self.orders] if self.orders else None)
                if tasker_context and self.limit and (self.query or self.orders):
                    tasker_context.add_iterator(iterator_name, TaskerDataIterator(datas, self.limit[1]))
            finally:
                self.db.release_connection()

        if self.limit:
            datas = datas[self.limit[0]: self.limit[1]]
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            self.limit,
            self.orders)


class RedisInsertBuilder(InsertBuilder, RedisCommand):
    def __init__(self, *args, **kwargs):
        InsertBuilder.__init__(self, *args, **kwargs)
        RedisCommand.__init__(self, self.db.serialize, self.db.base_prefix, self.name)

    def commit(self):
        try:
            connection = self.db.ensure_connection()
            self.save_datas(connection, self.primary_keys, self.datas, self.db.expire_seconds)
            return self.datas
        finally:
            self.db.release_connection()
            tasker_context = TaskerContext.current()
            if tasker_context:
                tasker_context.remove_iterator("redis::" + self.name)

    def verbose(self):
        return "datas(%d): \n%s" % (len(self.datas), human_repr_object(self.datas))


class RedisUpdateBuilder(UpdateBuilder, RedisCommand):
    def __init__(self, *args, **kwargs):
        UpdateBuilder.__init__(self, *args, **kwargs)
        RedisCommand.__init__(self, self.db.serialize, self.db.base_prefix, self.name)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def commit(self):
        try:
            connection = self.db.ensure_connection()
            datas = []
            for data in self.load_datas(connection):
                succed = True
                for key, exp, value, cmp in self.query:
                    if key not in data:
                        succed = False
                        break
                    if not cmp(data[key], value):
                        succed = False
                        break

                if succed:
                    datas.append(self.update)
                else:
                    datas.append(data)
            self.delete_datas(connection)
            if datas:
                self.save_datas(connection, self.primary_keys, datas, self.db.expire_seconds)
        finally:
            self.db.release_connection()
            tasker_context = TaskerContext.current()
            if tasker_context:
                tasker_context.remove_iterator("redis::" + self.name)

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            human_repr_object(self.diff_data))


class RedisDeleteBuilder(DeleteBuilder, RedisCommand):
    def __init__(self, *args, **kwargs):
        DeleteBuilder.__init__(self, *args, **kwargs)
        RedisCommand.__init__(self, self.db.serialize, self.db.base_prefix, self.name)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def commit(self):
        try:
            connection = self.db.ensure_connection()
            if not self.query:
                self.delete_datas(connection)
                return []

            datas = []
            for data in self.load_datas(connection):
                succed = True
                for key, exp, value, cmp in self.query:
                    if key not in data:
                        succed = False
                        break
                    if not cmp(data[key], value):
                        succed = False
                        break

                if not succed:
                    datas.append(data)

            self.delete_datas(connection)
            if datas:
                self.save_datas(connection, self.primary_keys, datas, self.db.expire_seconds)
        finally:
            self.db.release_connection()
            tasker_context = TaskerContext.current()
            if tasker_context:
                tasker_context.remove_iterator("redis::" + self.name)

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query])


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
        return driver.instance.ping()

    def close(self, driver):
        driver.instance.close()


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

        self.base_prefix = all_config.pop("prefix") if "prefix" in all_config else ""
        self.serialize = self.SERIALIZES.get(all_config.pop("serialize") if "serialize" in all_config else "json", JsonSerialize)()
        self.ignore_serialize_error = all_config.pop("ignore_serialize_error") if "ignore_serialize_error" in all_config else False
        self.expire_seconds = all_config.pop("expire_seconds") if "expire_seconds" in all_config else 86400

        super(RedisDB, self).__init__(manager, all_config)

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

    def build_factory(self):
        return RedisDBFactory(self.get_config_key(), self.config)

    def ensure_connection(self):
        return self.acquire_driver().raw()

    def release_connection(self):
        self.release_driver()

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

    def is_dynamic_schema(self, name):
        return True