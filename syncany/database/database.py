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

    def cache(self, name, prefix_key, config=None):
        raise NotImplementedError()

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


class DatabaseDriver(object):
    def __init__(self, factory, driver):
        self.factory = factory
        self.driver = driver
        self.idle_time = time.time()
        self.closed = False

    def __getitem__(self, item):
        return self.driver.__getitem__(item)

    def __getattr__(self, item):
        return getattr(self.driver, item)

    def ping(self):
        self.factory.ping(self.driver)

    def close(self):
        self.closed = True
        self.factory.close(self.driver)

    def raw(self):
        return self.driver


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
            driver = DatabaseDriver(factory, factory.create())
            factory.append(driver)
            self.factorys[key] = factory

    def acquire(self, key):
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
                    continue
            return DatabaseDriver(factory, factory.create())

    def release(self, key, driver):
        if self.closed:
            return driver.close()

        if key not in self.factorys:
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
            for key in list(self.factorys.keys()):
                factory = self.factorys[key]
                with factory.lock:
                    while factory.drivers:
                        driver = factory.pop()
                        driver.close()
                self.factorys.pop(key)

    def check_timeout(self):
        now = time.time()
        for key in list(self.factorys.keys()):
            factory = self.factorys[key]
            with factory.lock:
                for _ in range(len(factory.drivers)):
                    driver = factory.drivers.popleft()
                    if now - driver.idle_time > self.idle_timeout:
                        driver.close()
                    else:
                        factory.drivers.append(driver)

            with self.lock:
                if not factory.drivers:
                    self.factorys.pop(key)
