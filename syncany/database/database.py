# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import hashlib
from collections import deque
import threading


class QueryBuilder(object):
    def __init__(self, db, name, primary_keys, fields):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.fields = fields
        self.query = {}
        self.orders = []
        self.limit = None

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def filter_limit(self, count, start=None):
        raise NotImplementedError()

    def filter_cursor(self, last_data, offset, count):
        raise NotImplementedError()

    def order_by(self, key, direct=1):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def verbose(self):
        return ""


class InsertBuilder(object):
    def __init__(self, db, name, primary_keys, fields, datas):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.fields = fields
        self.datas = datas

    def commit(self):
        raise NotImplementedError()

    def verbose(self):
        return ""


class UpdateBuilder(object):
    def __init__(self, db, name, primary_keys, fields, update, diff_data=None):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.fields = fields
        self.query = {}
        self.update = update
        self.diff_data = diff_data

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def verbose(self):
        return ""


class DeleteBuilder(object):
    def __init__(self, db, name, primary_keys):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.query = {}

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def verbose(self):
        return ""


class DataBase(object):
    def __init__(self, manager, config):
        self.manager = manager
        self.name = config.pop("name")
        self.config = config

    def get_key(self, config):
        cs = []
        for key in sorted(config.keys()):
            cs.append("%s=%s" % (key, config[key]))
        return hashlib.md5("&".join(cs).encode("utf-8")).hexdigest()

    def query(self, name, primary_keys=None, fields=()):
        return QueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return InsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return UpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return DeleteBuilder(self, name, primary_keys)

    def flush(self):
        pass

    def close(self):
        pass

    def get_default_loader(self):
        return "db_loader"

    def get_default_outputer(self):
        return "db_update_delete_insert_outputer"

    def verbose(self):
        return self.name


class DatabaseFactory(object):
    def __init__(self, key, config):
        self.key = key
        self.config = config
        self.drivers = deque()
        self.lock = threading.Lock()

    def create(self):
        raise NotImplementedError

    def ping(self, driver):
        raise NotImplementedError

    def close(self, driver):
        raise NotImplementedError


class DatabaseManager(object):
    def __init__(self):
        self.factorys = {}

    def has(self, key):
        return key in self.factorys

    def register(self, key, factory):
        if key in self.factorys:
            return
        self.factorys[key] = factory

    def acquire(self, key):
        factory = self.factorys[key]
        with factory.lock:
            while factory.drivers:
                driver = factory.pop()
                try:
                    if factory.ping(driver):
                        return factory
                except:
                    continue
            return factory.create()

    def release(self, key, driver):
        factory = self.factorys[key]
        with factory.lock:
            factory.drivers.append(driver)

    def close(self):
        for factory in self.factorys:
            with factory.lock:
                while factory.drivers:
                    factory.close(factory.drivers.pop())
        self.factorys = []