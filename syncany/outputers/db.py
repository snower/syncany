# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .outputer import Outputer

class DBOutputer(Outputer):
    def __init__(self, db, name, *args, **kwargs):
        super(DBOutputer, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name
        self.querys = []
        self.operators = []

    def statistics(self):
        return {
            "querys": len(self.querys),
            "operators": len(self.operators),
            "load_rows": len(self.load_datas),
            "rows": len(self.datas)
        }