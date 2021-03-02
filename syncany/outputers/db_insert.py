# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

from .db import DBOutputer

class DBInsertOutputer(DBOutputer):
    def insert(self, datas):
        insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), datas)
        insert.commit()
        self.outputer_state["insert_count"] += 1

    def store(self, datas):
        super(DBInsertOutputer, self).store(datas)
        self.insert(datas)