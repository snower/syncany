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
try:
    import pymongo
except ImportError:
    pymongo = None
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class MongoQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoQueryBuilder, self).__init__(*args, **kwargs)

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

    def filter_limit(self, count, start=None):
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def order_by(self, key, direct=1):
        self.orders.append((key, 1 if direct else -1))

    def format_table(self):
        for virtual_collection in self.db.virtual_collections:
            if virtual_collection["name"] != self.collection_name:
                continue
            if isinstance(virtual_collection["query"], list):
                virtual_collection["query"] = " ".join(virtual_collection["query"])
            return virtual_collection['query']

    def format_query(self, virtual_collection):
        UUID, true, false, null = uuid.UUID, True, False, None
        def Datetime(*args):
            if len(args) == 1 and isinstance(args[0], str):
                return datetime.datetime.strptime(args[0], "%Y-%m-%dT%H:%M:%S.%f%z")
            return datetime.datetime(*args)

        virtual_collection = eval(virtual_collection)
        if isinstance(virtual_collection, list):
            virtual_collection = [virtual_collection]
        return [{"$match": self.query}] + virtual_collection

    def commit(self):
        virtual_collection = self.format_table()
        connection = self.db.ensure_connection()

        if virtual_collection:
            virtual_collection = self.format_query(virtual_collection)
            if self.limit:
                if self.limit[0]:
                    virtual_collection.append({"$skip": self.limit[0]})
                virtual_collection.append({"$limit": self.limit[0]})
            if self.orders:
                virtual_collection.append({"$sort": SON(list(self.orders))})
            cursor = connection[self.db_name][self.collection_name].aggregate(virtual_collection)
        else:
            cursor = connection[self.db_name][self.collection_name].find(self.query, {field: 1 for field in self.fields} if self.fields else None)
            if self.limit:
                if self.limit[0]:
                    cursor.skip(self.limit[0])
                cursor.limit(self.limit[1])
            if self.orders:
                cursor.sort(self.orders)
        return list(cursor)

class MongoInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MongoInsertBuilder, self).__init__(*args, **kwargs)

        name = self.name.split(".")
        self.db_name = self.db.db_name
        self.collection_name = ".".join(name[1:])

    def commit(self):
        connection = self.db.ensure_connection()
        if isinstance(self.datas, list):
            return connection[self.db_name][self.collection_name].insert_many(self.datas)
        return connection[self.db_name][self.collection_name].insert_one(self.datas)

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
        return connection[self.db_name][self.collection_name].update_one(self.query, {"$set": update})

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
        return connection[self.db_name][self.collection_name].remove(self.query, multi=True)

class MongoDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 27017,
        "tz_aware": True,
        "maxPoolSize": 4,
        "maxIdleTimeMS": 7200000,
        "readPreference": "secondaryPreferred",
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config.pop("db") if "db" in all_config else all_config["name"]
        self.virtual_collections = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(MongoDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if pymongo is None:
                raise ImportError("pymongo>=3.6.1 is required")

            self.connection = pymongo.MongoClient(**self.config)
        return self.connection

    def query(self, name, primary_keys=None, fields=()):
        return MongoQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return MongoInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return MongoUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return MongoDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None