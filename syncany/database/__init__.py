# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .database import DataBase
from ..errors import DatabaseUnknownException

DATABASES = {
    "memory": ".memory.MemoryDB",
    "textline": ".textline.TextLineDB",
    "mongo": ".mongodb.MongoDB",
    "mysql": ".mysql.MysqlDB",
    "postgresql": ".postgresql.PostgresqlDB",
    "clickhouse": ".clickhouse.ClickhouseDB",
    "influxdb": ".influxdb.InfluxDB",
    "elasticsearch": ".elasticsearch.ElasticsearchDB",
    "execl": ".excel.ExeclDB",
    "csv": ".csv.CsvDB",
    "json": ".json.JsonDB",
    "beanstalk": ".beanstalk.BeanstalkDB",
    "redis": ".redis.RedisDB",
    "http": ".http.HttpDataBase",
}


def find_database(name):
    if name not in DATABASES:
        raise DatabaseUnknownException("%s is unknown database driver" % name)

    if isinstance(DATABASES[name], str):
        module_name, _, cls_name = DATABASES[name].rpartition(".")
        if module_name[0] == ".":
            module_name = module_name[1:]
            module = __import__(module_name, globals(), globals(), [module_name], 1)
        else:
            module = __import__(module_name)
        DATABASES[name] = getattr(module, cls_name)
    return DATABASES[name]


def register_database(name, database):
    if not issubclass(database, DataBase):
        raise TypeError("is not DataBase")
    DATABASES[name] = database
    return database
