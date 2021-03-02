# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

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

        delete.commit()
        self.outputer_state["delete_count"] += 1

    def insert(self, datas):
        insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), datas)
        insert.commit()
        self.outputer_state["insert_count"] += 1

    def store(self, datas):
        super(DBDeleteInsertOutputer, self).store(datas)

        if self.filters:
            self.remove()
        self.insert(datas)