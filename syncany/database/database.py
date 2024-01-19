# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import datetime
from collections import deque
import threading
from ..utils import parse_datetime, parse_date, parse_time, get_timezone


class Cmper(object):
    @classmethod
    def ensure_value_type(cls, a, b):
        if isinstance(a, datetime.date):
            if isinstance(a, datetime.datetime):
                if isinstance(b, datetime.date):
                    return a, datetime.datetime(b.year, b.month, b.day, tzinfo=get_timezone())
                if isinstance(b, datetime.time):
                    now = datetime.datetime.now()
                    return a, datetime.datetime(now.year, now.month, now.day, b.hour, b.minute, b.second,
                                                b.microsecond, tzinfo=get_timezone())
                try:
                    return a, parse_datetime(b, None, get_timezone())
                except:
                    return a, None
            if isinstance(b, datetime.datetime):
                return a, datetime.date(b.year, b.month, b.day)
            try:
                return a, parse_date(b, None, get_timezone())
            except:
                return a, None
        if isinstance(a, datetime.time):
            if isinstance(b, datetime.datetime):
                return datetime.time(b.hour, b.minute, b.second, b.microsecond)
            try:
                return a, parse_time(b, None, get_timezone())
            except:
                return a, None
        if isinstance(a, int):
            try:
                return a, int(b)
            except ValueError:
                return a, 0
        if isinstance(a, float):
            try:
                return a, float(b)
            except ValueError:
                return a, 0.0
        try:
            return a, type(a)(b)
        except:
            return a, None

    @classmethod
    def cmp_gt(cls, a, b):
        try:
            return a > b
        except TypeError:
            if a is None:
                return False
            if b is None:
                return True
            a, b = cls.ensure_value_type(a, b)
            if b is None:
                return True
            return a > b

    @classmethod
    def cmp_gte(cls, a, b):
        try:
            return a >= b
        except TypeError:
            if a is None:
                return True if b is None else False
            if b is None:
                return True
            a, b = cls.ensure_value_type(a, b)
            if b is None:
                return True
            return a >= b

    @classmethod
    def cmp_lt(cls, a, b):
        try:
            return a < b
        except TypeError:
            if a is None:
                return False if b is None else True
            if b is None:
                return False
            a, b = cls.ensure_value_type(a, b)
            if b is None:
                return False
            return a < b

    @classmethod
    def cmp_lte(cls, a, b):
        try:
            return a <= b
        except TypeError:
            if a is None:
                return True
            if b is None:
                return True
            a, b = cls.ensure_value_type(a, b)
            if b is None:
                return False
            return a <= b

    @classmethod
    def cmp_eq(cls, a, b):
        if a == b:
            return True
        if a is None or b is None:
            return False
        a, b = cls.ensure_value_type(a, b)
        if b is None:
            return False
        return a == b

    @classmethod
    def cmp_ne(cls, a, b):
        if a == b:
            return False
        if a is None or b is None:
            return True
        a, b = cls.ensure_value_type(a, b)
        if b is None:
            return True
        return a != b

    @classmethod
    def cmp_in(cls, a, b):
        if b is None:
            return False
        return a in b



class QueryBuilder(object):
    def __init__(self, db, name, primary_keys, fields):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.fields = fields
        self.query = []
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
        self.query = []
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
        self.query = []

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
        self.config_key = None
        self.database_driver = None

    def get_config_key(self):
        if self.config_key is not None:
            return self.config_key
        self.config_key = "%s::%s" % (self.__class__.__name__, self.name)
        return self.config_key

    def build_factory(self):
        raise NotImplementedError()

    def acquire_driver(self):
        if self.database_driver:
            return self.database_driver
        if not self.manager.has(self.get_config_key()):
            self.manager.register(self.get_config_key(), self.build_factory())
        self.database_driver = self.manager.acquire(self.get_config_key())
        return self.database_driver

    def release_driver(self):
        if not self.database_driver:
            return
        self.manager.release(self.get_config_key(), self.database_driver)
        self.database_driver = None

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
        self.release_driver()

    def is_dynamic_schema(self, name):
        return False

    def is_streaming(self, name):
        return self.manager.get_state(self.get_config_key() + "::" + name, "is_streaming")

    def set_streaming(self, name, is_streaming=None):
        if is_streaming is None:
            return
        self.manager.set_state(self.get_config_key() + "::" + name, "is_streaming", is_streaming if is_streaming else None)

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
        self.states = {}
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

    def get_state(self, name, key):
        if name not in self.states:
            return None
        return self.states[name].get(key)

    def set_state(self, name, key, value):
        if value is None:
            if name not in self.states:
                return
            self.states[name].pop(key, None)
            if not self.states[name]:
                self.states.pop(name, None)
        else:
            if name not in self.states:
                self.states[name] = {}
            self.states[name][key] = value
