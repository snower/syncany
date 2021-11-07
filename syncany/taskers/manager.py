# -*- coding: utf-8 -*-
# 2021/11/7
# create by: snower

class TaskerManager(object):
    def __init__(self, database_manager):
        self.database_manager = database_manager

    def close(self):
        self.database_manager.close()