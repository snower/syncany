# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
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

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
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


class CacheBuilder(object):
    def __init__(self, db, name, prefix_key, config):
        self.db = db
        self.name = name
        self.prefix_key = prefix_key
        self.config = config or {}

    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value, exprie_seconds=86400):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()


class DataBase(object):
    def __init__(self, manager, config):
        self.manager = manager
        self.name = config.pop("name")
        self.config = config
        self.key = None

    def get_key(self, config):
        if self.key is not None:
            return self.key

        cs = []
        for key in sorted(config.keys()):
            cs.append("%s=%s" % (key, config[key]))
        self.key = hashlib.md5("&".join(cs).encode("utf-8")).hexdigest()
        return self.key

    def query(self, name, primary_keys=None, fields=()):
        return QueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return InsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return UpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return DeleteBuilder(self, name, primary_keys)

    def cache(self, name, prefix_key, config=None):
        raise NotImplementedError()

    def flush(self):
        pass

    def close(self):
        pass

    def is_dynamic_schema(self, name):
        return False

    def is_streaming(self, name):
        return False

    def set_streaming(self, name, is_streaming=False):
        pass

    def sure_loader(self, loader):
        if not loader:
            return "db_loader"
        return loader

    def sure_outputer(self, outputer):
        if not outputer:
            return "db_update_delete_insert_outputer"
        return outputer

    def verbose(self):
        return self.name


class DatabaseDriver(object):
    def __init__(self, factory, instance):
        self.factory = factory
        self.instance = instance
        self.idle_time = time.time()
        self.closed = False

    def __getitem__(self, item):
        return self.instance.__getitem__(item)

    def __getattr__(self, item):
        return getattr(self.instance, item)

    def ping(self):
        return self.factory.ping(self)

    def close(self):
        self.closed = True
        self.factory.close(self)

    def raw(self):
        return self.instance


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

    def pop(self):
        return self.drivers.pop()

    def append(self, driver):
        self.drivers.append(driver)


class DatabaseManager(object):
    def __init__(self, idle_timeout=7200, ping_idle_timeout=300):
        self.factorys = {}
        self.lock = threading.Lock()
        self.idle_timeout = idle_timeout
        self.ping_idle_timeout = ping_idle_timeout
        self.closed = False

    def has(self, key):
        with self.lock:
            return key in self.factorys

    def register(self, key, factory):
        with self.lock:
            if key in self.factorys:
                return
            self.factorys[key] = factory

    def acquire(self, key):
        with self.lock:
            factory = self.factorys[key]

        with factory.lock:
            while factory.drivers:
                driver = factory.pop()
                if time.time() - driver.idle_time < self.ping_idle_timeout:
                    return driver
                try:
                    if driver.ping():
                        return driver
                except:
                    driver.close()
            return DatabaseDriver(factory, factory.create())

    def release(self, key, driver):
        if self.closed:
            return driver.close()

        with self.lock:
            if key not in self.factorys:
                self.factorys[key] = driver.factory
            factory = self.factorys[key]

        with factory.lock:
            factory.append(driver)
            driver.idle_time = time.time()

    def close(self):
        self.closed = True
        with self.lock:
            factorys, self.factorys = self.factorys, {}

        for key in list(factorys.keys()):
            factory = factorys[key]
            with factory.lock:
                while factory.drivers:
                    driver = factory.pop()
                    try:
                        driver.close()
                    except:
                        pass
            factorys.pop(key)

    def check_timeout(self):
        now = time.time()

        with self.lock:
            factorys = ((key, factory) for key, factory in self.factorys.items())

        for key, factory in factorys:
            with factory.lock:
                for _ in range(len(factory.drivers)):
                    driver = factory.drivers.popleft()
                    if now - driver.idle_time > self.idle_timeout:
                        driver.close()
                    else:
                        factory.drivers.append(driver)

        with self.lock:
            for key, factory in factorys:
                if not factory.drivers:
                    self.factorys.pop(key, None)
