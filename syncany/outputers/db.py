# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from collections import OrderedDict
from .outputer import Outputer

class DBOutputer(Outputer):
    def __init__(self, db, name, *args, **kwargs):
        super(DBOutputer, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name
        self.querys = []
        self.operators = []

    def clone(self):
        outputer = self.__class__(self.db, self.name, self.primary_keys)
        schema = OrderedDict()
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        outputer.schema = schema
        outputer.filters = [filter for filter in self.filters]
        return outputer

    def statistics(self):
        return {
            "querys": len(self.querys),
            "operators": len(self.operators),
            "load_rows": len(self.load_datas),
            "rows": len(self.datas)
        }