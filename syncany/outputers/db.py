# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .outputer import Outputer

class DBOutputer(Outputer):
    def __init__(self, db, name, *args, **kwargs):
        self.insert_batch = kwargs.pop("insert_batch", 0)
        super(DBOutputer, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name

    def clone(self):
        outputer = self.__class__(self.db, self.name, self.primary_keys)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        outputer.schema = schema
        outputer.filters = [filter for filter in self.filters]
        outputer.insert_batch = self.insert_batch
        return outputer

    def statistics(self):
        operator_count = self.outputer_state["insert_count"] + self.outputer_state["update_count"] \
                         + self.outputer_state["delete_count"]
        return {
            "querys": self.outputer_state["query_count"],
            "operators": operator_count,
            "load_rows": self.outputer_state["load_count"],
            "rows": len(self.datas)
        }