# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import types
from .database import DataBase, DatabaseFactory, DatabaseManager, DatabaseDriver, CacheBuilder, \
    QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder
from ..errors import DatabaseUnknownException

DATABASES = {
    "memory": ".memory.MemoryDB",
    "textline": ".textline.TextLineDB",
    "mongo": ".mongodb.MongoDB",
    "mysql": ".mysql.MysqlDB",
    "postgresql": ".postgresql.PostgresqlDB",
    "sqlserver": ".sqlserver.SqlServerDB",
    "clickhouse": ".clickhouse.ClickhouseDB",
    "influxdb": ".influxdb.InfluxDB",
    "elasticsearch": ".elasticsearch.ElasticsearchDB",
    "execl": ".excel.ExeclDB",
    "csv": ".csv.CsvDB",
    "json": ".json.JsonDB",
    "beanstalk": ".beanstalk.BeanstalkDB",
    "redis": ".redis.RedisDB",
    "http": ".http.HttpDataBase",
    "sqlite": ".sqlite.SqliteDB",
}


class DatabaseInstanceBuilder(object):
    driver_name = None
    driver_instance = None
    manager = None
    name = None
    config = None
    update_attrs = None

    def __init__(self, driver_name):
        self.driver_name = driver_name
        self.update_attrs = []

    def __call__(self, manager, config):
        self.manager = manager
        self.name = config.get("name")
        self.config = config
        return self

    def __getattr__(self, item):
        if self.driver_instance is None:
            if item == "sure_loader":
                return lambda loader: loader
            if item == "sure_outputer":
                return lambda outputer: outputer
            if item == "verbose":
                return lambda: self.name
            if item in ("get_key", "query", "insert", "update", "delete", "cache",
                        "flush", "close", "is_dynamic_schema", "is_streaming", "set_streaming"):
                return lambda *args, **kwargs: None
            raise AttributeError(item)
        return getattr(self.driver_instance, item)

    def __setattr__(self, key, value):
        if self.driver_instance is None:
            if key in ("get_key", "query", "insert", "update", "delete", "cache",
                       "flush", "close", "is_dynamic_schema", "is_streaming", "set_streaming", "sure_loader",
                       "sure_outputer", "verbose"):
                self.update_attrs.append((key, value))
            else:
                return super(DatabaseInstanceBuilder, self).__setattr__(key, value)
        else:
            setattr(self.driver_instance, key, value)

    def build(self):
        if isinstance(DATABASES[self.driver_name], str):
            module_name, _, cls_name = DATABASES[self.driver_name].rpartition(".")
            if module_name[0] == ".":
                module_name = module_name[1:]
                module = __import__(module_name, globals(), locals(), [module_name], 1)
            elif "." in module_name:
                from_module_name, _, module_name = module_name.rpartition(".")
                module = __import__(from_module_name, globals(), locals(), [module_name])
            else:
                module = __import__(module_name, globals(), locals())
            database_cls = getattr(module, cls_name)
            if not isinstance(database_cls, type) or not issubclass(database_cls, DataBase):
                raise TypeError("is not DataBase")
            DATABASES[self.driver_name] = database_cls
        elif isinstance(DATABASES[self.driver_name], (types.FunctionType, types.LambdaType)):
            database_cls = DATABASES[self.driver_name]()
            if not isinstance(database_cls, type) or not issubclass(database_cls, DataBase):
                raise TypeError("is not DataBase")
            DATABASES[self.driver_name] = database_cls

        if self.driver_instance is None:
            self.driver_instance = DATABASES[self.driver_name](self.manager, self.config)
            for key, value in self.update_attrs:
                setattr(self.driver_instance, key, value)
        return self.driver_instance


class DatabaseInstanceManager(dict):
    def instance(self, name):
        instance = super(DatabaseInstanceManager, self).__getitem__(name)
        if isinstance(instance, DatabaseInstanceBuilder):
            instance = instance.build()
            super(DatabaseInstanceManager, self).__setitem__(name, instance)
        return instance


def find_database(name):
    if name not in DATABASES:
        raise DatabaseUnknownException("%s is unknown database driver" % name)
    return DatabaseInstanceBuilder(name)


def register_database(name, database=None):
    if database is None:
        def _(database):
            if not isinstance(database, str) and not callable(database) \
                    and (not isinstance(database, type) or not issubclass(database, DataBase)):
                raise TypeError("is not DataBase")
            DATABASES[name] = database
            return database
        return _

    if not isinstance(database, str) and not callable(database) \
            and (not isinstance(database, type) or not issubclass(database, DataBase)):
        raise TypeError("is not DataBase")
    DATABASES[name] = database
    return database
