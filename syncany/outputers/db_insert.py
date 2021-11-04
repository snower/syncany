# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

import math
from .db import DBOutputer

class DBInsertOutputer(DBOutputer):
    def insert(self, datas):
        if self.insert_batch > 0:
            for i in range(math.ceil(float(len(datas)) / float(self.insert_batch))):
                insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()),
                                        datas[i * self.insert_batch: (i + 1) * self.insert_batch])
                insert.commit()
                self.outputer_state["insert_count"] += 1
        else:
            insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), datas)
            insert.commit()
            self.outputer_state["insert_count"] += 1

    def store(self, datas):
        super(DBInsertOutputer, self).store(datas)
        self.insert(datas)