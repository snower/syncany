# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .outputer import Outputer
from .db_update_delete_insert import DBUpdateDeleteInsertOutputer
from .db_update_insert import DBUpdateInsertOutputer
from .db_delete_insert import DBDeleteInsertOutputer
from .db_insert import DBInsertOutputer
from ..errors import OutputerUnknownException

OUTPUTERS = {
    "db_update_delete_insert_outputer": DBUpdateDeleteInsertOutputer,
    "db_update_insert_outputer": DBUpdateInsertOutputer,
    "db_delete_insert_outputer": DBDeleteInsertOutputer,
    "db_insert_outputer": DBInsertOutputer,
}

def find_outputer(name):
    if name not in OUTPUTERS:
        raise OutputerUnknownException("%s is unknown outputer" % name)
    return OUTPUTERS[name]

def register_outputer(name, outputer):
    if not issubclass(outputer, Outputer):
        raise TypeError("is not Outputer")
    OUTPUTERS[name] = outputer
    return outputer