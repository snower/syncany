# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .db_update_delete_insert import DBUpdateDeleteInsertOutputer
from .db_update_insert import DBUpdateInsertOutputer
from .db_delete_insert import DBDeleteInsertOutputer
from .db_insert import DBInsertOutputer

OUTPUTERS = {
    "db_update_delete_insert_outputer": DBUpdateDeleteInsertOutputer,
    "db_update_insert_outputer": DBUpdateInsertOutputer,
    "db_delete_insert_outputer": DBDeleteInsertOutputer,
    "db_insert_outputer": DBInsertOutputer,
}

def find_outputer(name):
    return OUTPUTERS.get(name)