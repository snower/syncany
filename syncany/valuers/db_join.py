# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .db import DBValuer

class DBJoinValuer(DBValuer):
    def __init__(self, loader, foreign_key, foreign_filters, args_valuer, valuer, inherit_valuers, *args, **kwargs):
        super(DBJoinValuer, self).__init__(*args, **kwargs)

        self.loader = loader
        self.foreign_key = foreign_key
        self.args_valuer = args_valuer
        self.valuer = valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        self.matcher = None

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        valuer = self.valuer.clone()
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.loader, self.foreign_key, self.foreign_filters,
                              self.args_valuer.clone() if self.args_valuer else None,
                              valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.args_valuer:
            self.args_valuer.fill(data)
            self.matcher = self.loader.filter_eq(self.foreign_key, self.args_valuer.get())
        elif self.key:
            super(DBJoinValuer, self).fill(data)
            self.matcher = self.loader.filter_eq(self.foreign_key, self.value)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.matcher.add_valuer(self.valuer)
        return self

    def get(self):
        self.loader.load()

        return self.valuer.get()

    def childs(self):
        valuers = []
        if self.args_valuer:
            valuers.append(self.args_valuer)
        if self.valuer:
            valuers.append(self.valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                valuers.append(inherit_valuer)
        return valuers

    def get_fields(self):
        fields = []

        if self.args_valuer:
            for field in self.args_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields if fields else super(DBJoinValuer, self).get_fields()

    def get_final_filter(self):
        return self.valuer.get_final_filter()

    def require_loaded(self):
        return True