# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .db import DBValuer

class DBJoinValuer(DBValuer):
    def __init__(self, loader, foreign_key, valuer, *args, **kwargs):
        super(DBJoinValuer, self).__init__(*args, **kwargs)

        self.loader = loader
        self.foreign_key = foreign_key
        self.valuer = valuer
        self.matcher = None

    def clone(self):
        return self.__class__(self.loader, self.foreign_key, self.valuer.clone(), self.key, self.filter)

    def fill(self, data):
        super(DBJoinValuer, self).fill(data)

        self.matcher = self.loader.filter_eq(self.foreign_key, self.value)
        self.matcher.add_valuer(self.valuer)
        return self

    def get(self):
        self.loader.load()

        return self.valuer.get()

    def childs(self):
        return [self.valuer]

    def get_fields(self):
        return [self.key]