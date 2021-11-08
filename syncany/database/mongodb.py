# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import datetime
import uuid
try:
    from bson import SON, ObjectId, Binary, Code, DBRef, Decimal128, \
        Int64, MaxKey, MinKey, Regex, Timestamp
except ImportError:
    pass
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


class MongoQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoQueryBuilder, self).__init__(*args, **kwargs)

        name = self.name.split(".")
        self.db_name = self.db.db_name
        self.collection_name = ".".join(name[1:])
        self.bquery = None

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$in"] = value

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def filter_cursor(self, last_data, offset, count):
        if len(self.primary_keys) == 1 and self.primary_keys[0] in last_data:
            if self.primary_keys[0] not in self.query:
                self.query[self.primary_keys[0]] = {}
            self.query[self.primary_keys[0]]["$gt"] = last_data[self.primary_keys[0]]
        else:
            self.limit = (offset, count)

    def order_by(self, key, direct=1):
        self.orders.append((key, 1 if direct else -1))

    def format_table(self):
        for virtual_collection in self.db.virtual_collections:
            if virtual_collection["name"] != self.collection_name:
                continue
            if isinstance(virtual_collection["query"], list) and \
                    virtual_collection["query"] and isinstance(virtual_collection["query"][0], str):
                virtual_collection["query"] = " ".join(virtual_collection["query"])
            return virtual_collection['query'], virtual_collection.get("args", [])
        return None, None

    def format_value(self, value):
        if isinstance(value, str):
            return '"' + value + '"'
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, list):
            return "[" + ", ".join([self.format_value(v) for v in value]) + "]"
        if isinstance(value, dict):
            return "{" + ", ".join([self.format_value(k) + ": " + self.format_value(v) for k, v in value.items()]) + "}"
        if value is True:
            return 'true'
        if value is False:
            return 'false'
        if value is None:
            return 'null'
        return str(value)

    def format_query(self, virtual_collection, virtual_args):
        if not isinstance(virtual_collection, str):
            if not isinstance(virtual_collection, list):
                virtual_collection = [virtual_collection]
            return [{"$match": self.query}] + virtual_collection

        UUID, true, false, null = uuid.UUID, True, False, None
        def Datetime(*args):
            if len(args) == 1 and isinstance(args[0], str):
                return datetime.datetime.strptime(args[0], "%Y-%m-%dT%H:%M:%S.%f%z")
            return datetime.datetime(*args)

        exps = {">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "!=": "$ne", "in": "$in"}
        virtual_values, matched_querys = [], []
        for arg in virtual_args:
            if isinstance(arg, str) or arg[1] == "==":
                if arg in self.query and not isinstance(self.query[arg], dict):
                    virtual_values.append(self.format_value(self.query[arg]))
                    matched_querys.append(arg)
                else:
                    virtual_values.append('""')
            else:
                if arg[0] in self.query and arg[1] in exps and exps[arg[1]] in self.query[arg[0]]:
                    virtual_values.append(self.format_value(self.query[arg[0]][exps[arg[1]]]))
                    matched_querys.append((arg[0], arg[1]))
                else:
                    virtual_values.append('""')
        if virtual_values:
            virtual_collection = virtual_collection % tuple(virtual_values)

        for mq in matched_querys:
            if isinstance(mq, tuple):
                if mq[0] not in self.query:
                    continue
                self.query[mq[0]].pop(exps[mq[1]], None)
                if not self.query[mq[0]]:
                    self.query.pop(mq[0], None)
            else:
                self.query.pop(mq, None)

        virtual_collection = eval(virtual_collection)
        if not isinstance(virtual_collection, list):
            virtual_collection = [virtual_collection]
        return [{"$match": self.query}] + virtual_collection

    def commit(self):
        virtual_collection, virtual_args = self.format_table()
        connection = self.db.ensure_connection()
        try:
            if virtual_collection:
                virtual_collection = self.format_query(virtual_collection, virtual_args)
                if self.limit:
                    if self.limit[0]:
                        virtual_collection.append({"$skip": self.limit[0]})
                    virtual_collection.append({"$limit": self.limit[1]})
                if self.orders:
                    virtual_collection.append({"$sort": SON(list(self.orders))})
                cursor = connection[self.db_name][self.collection_name].aggregate(virtual_collection, allowDiskUse=True)
                self.bquery = virtual_collection
            else:
                fields = {field: 1 for field in self.fields} if self.fields else None
                cursor = connection[self.db_name][self.collection_name].find(self.query, fields)
                if self.limit:
                    if self.limit[0]:
                        cursor.skip(self.limit[0])
                    cursor.limit(self.limit[1])
                if self.orders:
                    cursor.sort(self.orders)
                self.bquery = (self.query, fields) if fields else self.query
            return list(cursor)
        finally:
            self.db.release_connection()

    def verbose(self):
        if isinstance(self.bquery, tuple):
            return "collection: %s\nquery: %svalues: %s\nlimit: %s\norderBy: %s" % (
                self.collection_name, self.bquery[0], human_repr_object(self.bquery[1]), self.limit, self.orders)
        return "collection: %s\nquery: %s\nlimit: %s\norderBy: %s" % (
            self.collection_name, human_repr_object(self.bquery), self.limit, self.orders)


class MongoInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoInsertBuilder, self).__init__(*args, **kwargs)

        name = self.name.split(".")
        self.db_name = self.db.db_name
        self.collection_name = ".".join(name[1:])

    def commit(self):
        connection = self.db.ensure_connection()
        try:
            if isinstance(self.datas, list):
                return connection[self.db_name][self.collection_name].insert_many(self.datas)
            return connection[self.db_name][self.collection_name].insert_one(self.datas)
        finally:
            self.db.release_connection()

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)

class MongoUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoUpdateBuilder, self).__init__(*args, **kwargs)

        name = self.name.split(".")
        self.db_name = self.db.db_name
        self.collection_name = ".".join(name[1:])

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$in"] = value

    def commit(self):
        connection = self.db.ensure_connection()
        update = {}
        for key, value in self.update.items():
            if self.diff_data and key not in self.diff_data:
                continue
            update[key] = value
        try:
            return connection[self.db_name][self.collection_name].update_one(self.query, {"$set": update})
        finally:
            self.db.release_connection()

    def verbose(self):
        update = {}
        for key, value in self.update.items():
            if self.diff_data and key not in self.diff_data:
                continue
            update[key] = value
        return "collection: %s\nquery: %s\nupdateDatas: %s" % (
            self.collection_name, human_repr_object(self.query), human_repr_object(update))


class MongoDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoDeleteBuilder, self).__init__(*args, **kwargs)

        name = self.name.split(".")
        self.db_name = self.db.db_name
        self.collection_name = ".".join(name[1:])

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["$in"] = value

    def commit(self):
        connection = self.db.ensure_connection()
        try:
            return connection[self.db_name][self.collection_name].remove(self.query, multi=True)
        finally:
            self.db.release_connection()

    def verbose(self):
        return "collection: %s\nquery: %s" % (self.collection_name, human_repr_object(self.query))


class MongoDBFactory(DatabaseFactory):
    def __init__(self, *args, **kwargs):
        super(MongoDBFactory, self).__init__(*args, **kwargs)

        self.client = None

    def create(self):
        if self.client:
            return self.client

        try:
            import pymongo
        except ImportError:
            raise ImportError("pymongo>=3.6.1 is required")
        self.client = pymongo.MongoClient(**self.config)
        self.drivers.append(self.client)
        return self.client

    def ping(self, driver):
        return True

    def close(self, driver):
        if self.client == driver:
            self.drivers.pop()
            self.client = None
        driver.close()

    def pop(self):
        return self.client

    def append(self, driver):
        if self.client == driver:
            return
        driver.close()


class MongoDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 27017,
        "tz_aware": True,
        "maxPoolSize": 4,
        "maxIdleTimeMS": 7200000,
        "readPreference": "secondaryPreferred",
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config.pop("db") if "db" in all_config else all_config["name"]
        self.virtual_collections = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(MongoDB, self).__init__(manager, all_config)

        self.connection_key = None
        self.connection = None

    def ensure_connection(self):
        if self.connection:
            return self.connection.raw()
        if not self.connection_key:
            self.connection_key = self.get_key(self.config)
            if not self.manager.has(self.connection_key):
                self.manager.register(self.connection_key, MongoDBFactory(self.connection_key, self.config))
        self.connection = self.manager.acquire(self.connection_key)
        return self.connection.raw()

    def release_connection(self):
        if not self.connection:
            return
        self.manager.release(self.connection_key, self.connection)
        self.connection = None

    def query(self, name, primary_keys=None, fields=()):
        return MongoQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return MongoInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return MongoUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return MongoDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.connection:
            return
        self.connection.raw().close()
        self.connection = None

    def verbose(self):
        return "%s<%s>" % (self.name, self.db_name)