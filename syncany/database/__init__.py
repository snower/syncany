# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .database import DataBase
from ..errors import DatabaseUnknownException

DATABASES = {}

def find_database(name):
    if name == "memory":
        from .memory import MemoryDB
        return MemoryDB
    if name == "textline":
        from .textline import TextLineDB
        return TextLineDB
    if name == "mongo":
        from .mongodb import MongoDB
        return MongoDB
    if name == "mysql":
        from .mysql import MysqlDB
        return MysqlDB
    if name == "postgresql":
        from .postgresql import PostgresqlDB
        return PostgresqlDB
    if name == "clickhouse":
        from .clickhouse import ClickhouseDB
        return ClickhouseDB
    if name == "influxdb":
        from .influxdb import InfluxDB
        return InfluxDB
    if name == "elasticsearch":
        from .elasticsearch import ElasticsearchDB
        return ElasticsearchDB
    if name == "execl":
        from .excel import ExeclDB
        return ExeclDB
    if name == "csv":
        from .csv import CsvDB
        return CsvDB
    if name == "json":
        from .json import JsonDB
        return JsonDB
    if name == "beanstalk":
        from .beanstalk import BeanstalkDB
        return BeanstalkDB
    if name == "redis":
        from .redis import RedisDB
        return RedisDB
    if name not in DATABASES:
        raise DatabaseUnknownException("%s is unknown database driver" % name)
    return DATABASES[name]

def register_database(name, database):
    if not issubclass(database, DataBase):
        raise TypeError("is not DataBase")
    DATABASES[name] = database
    return database