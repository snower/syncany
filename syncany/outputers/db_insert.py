# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

from .db import DBOutputer

class DBInsertOutputer(DBOutputer):
    def insert(self, datas):
        for i in range(int(len(datas) / 500.0 + 1)):
            bdatas = datas[i * 500: (i + 1) * 500]
            if not bdatas:
                break

            insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), bdatas)
            insert.commit()
            self.operators.append(insert)

    def store(self, datas):
        super(DBInsertOutputer, self).store(datas)
        self.insert(datas)