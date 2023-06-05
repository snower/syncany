# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .outputer import Outputer, LoadDataValue


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
        outputer.orders = [order for order in self.orders]
        outputer.insert_batch = self.insert_batch
        return outputer

    def is_dynamic_schema(self):
        return self.db.is_dynamic_schema(self.name)

    def is_streaming(self):
        return self.db.is_streaming(self.name)

    def set_streaming(self, is_streaming=None):
        if is_streaming is None:
            return
        self.db.set_streaming(self.name, is_streaming)

    def statistics(self):
        operator_count = self.outputer_state["insert_count"] + self.outputer_state["update_count"] \
                         + self.outputer_state["delete_count"]
        return {
            "querys": self.outputer_state["query_count"],
            "operators": operator_count,
            "load_rows": self.outputer_state["load_count"],
            "rows": len(self.datas)
        }