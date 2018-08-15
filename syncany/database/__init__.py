# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .mongodb import MongoDB
from .mysql import MysqlDB
from .excel import ExeclDB
from .csv import CsvDB
from .json import JsonDB

DATABASES = {
    "mongo": MongoDB,
    "mysql": MysqlDB,
    "execl": ExeclDB,
    "csv": CsvDB,
    "json": JsonDB,
}

def find_database(name):
    return DATABASES[name]