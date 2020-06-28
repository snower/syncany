# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .const import ConstValuer

class ConstJoinValuer(ConstValuer):
    def __init__(self, loader, foreign_key, valuer, inherit_valuers, *args, **kwargs):
        super(ConstJoinValuer, self).__init__(*args, **kwargs)

        self.loader = loader
        self.foreign_key = foreign_key
        self.valuer = valuer
        self.inherit_valuers = inherit_valuers

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        valuer = self.valuer.clone()
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.loader, self.foreign_key, valuer, inherit_valuers, self.value, self.key, self.filter)

    def fill(self, data):
        self.loader.filter_eq(self.foreign_key, self.value)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(self.value)
        return self

    def get(self):
        self.loader.load()
        return self.valuer.get()

    def childs(self):
        valuers = [self.valuer]
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                valuers.append(inherit_valuer)
        return valuers

    def get_fields(self):
        return [self.key]

    def get_final_filter(self):
        return self.valuer.get_final_filter()

    def require_loaded(self):
        return True