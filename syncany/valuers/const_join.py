# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .const import ConstValuer

class ConstJoinValuer(ConstValuer):
    def __init__(self, loader, foreign_key, valuer, *args, **kwargs):
        super(ConstJoinValuer, self).__init__(*args, **kwargs)

        self.loader = loader
        self.foreign_key = foreign_key,
        self.valuer = valuer

    def clone(self):
        return self.__class__(self.loader, self.foreign_key, self.valuer.clone(), self.value, self.key, self.filter)

    def fill(self, data):
        self.loader.filter_eq(self.foreign_key, self.value)
        return self

    def get(self):
        self.loader.load()
        return self.valuer.get()

    def childs(self):
        return [self.valuer]

    def get_fields(self):
        return [self.key]

    def get_final_filter(self):
        return self.valuer.get_final_filter()

    def require_loaded(self):
        return True