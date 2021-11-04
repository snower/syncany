# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import math
from collections import defaultdict
from .db import DBOutputer

class DBDeleteInsertOutputer(DBOutputer):
    def remove(self):
        delete = self.db.delete(self.name, self.primary_keys)

        in_exps = defaultdict(list)
        for key, exp, value in self.filters:
            if exp == "in":
                in_exps[key].extend(value)

        for key, exp, value in self.filters:
            if exp == "eq":
                in_exps[key].append(value)

        for key, exp, value in self.filters:
            if exp == "eq":
                if key not in in_exps:
                    continue

                if len(in_exps[key]) > 1:
                    exp, value = "in", in_exps.pop(key)

            getattr(delete, "filter_%s" % exp)(key, value)

        if self.current_cursor:
            for primary_key in self.primary_keys:
                if primary_key not in self.current_cursor[0]:
                    continue
                delete.filter_gt(primary_key, self.current_cursor[0][primary_key])

        delete.commit()
        self.outputer_state["delete_count"] += 1

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
        super(DBDeleteInsertOutputer, self).store(datas)

        if self.filters:
            self.remove()
        self.insert(datas)