# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .database import DataBase
from .memory import MemoryDB
from .textline import TextLineDB
from .mongodb import MongoDB
from .mysql import MysqlDB
from .postgresql import PostgresqlDB
from .elasticsearch import ElasticsearchDB
from .excel import ExeclDB
from .csv import CsvDB
from .json import JsonDB
from ..errors import DatabaseUnknownException

DATABASES = {
    "memory": MemoryDB,
    "textline": TextLineDB,
    "mongo": MongoDB,
    "mysql": MysqlDB,
    "postgresql": PostgresqlDB,
    "elasticsearch": ElasticsearchDB,
    "execl": ExeclDB,
    "csv": CsvDB,
    "json": JsonDB,
}

def find_database(name):
    if name not in DATABASES:
        raise DatabaseUnknownException("%s is unknown database driver" % name)
    return DATABASES[name]

def register_database(name, database):
    if not issubclass(database, DataBase):
        raise TypeError("is not DataBase")
    DATABASES[name] = database
    return database