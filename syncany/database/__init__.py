# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .mongodb import MongoDB
from .mysql import MysqlDB

DATABASES = {
    "mongo": MongoDB,
    "mysql": MysqlDB,
}

def find_database(name):
    return DATABASES[name]