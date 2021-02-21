# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer

class DBJoinValuer(Valuer):
    def __init__(self, loader, foreign_key, foreign_filters, args_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_key = foreign_key
        self.args_valuer = args_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        super(DBJoinValuer, self).__init__(*args, **kwargs)

        self.matcher = None

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        return_valuer = self.return_valuer.clone()
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.loader, self.foreign_key, self.foreign_filters,
                              self.args_valuer.clone() if self.args_valuer else None,
                              return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.args_valuer:
            self.args_valuer.fill(data)
            self.matcher = self.loader.filter_eq(self.foreign_key, self.args_valuer.get())
        elif self.key:
            super(DBJoinValuer, self).fill(data)
            self.matcher = self.loader.filter_eq(self.foreign_key, self.value)

        self.matcher.add_valuer(self.return_valuer)
        return self

    def get(self):
        self.loader.load()
        return self.return_valuer.get()

    def childs(self):
        valuers = []
        if self.args_valuer:
            valuers.append(self.args_valuer)
        if self.return_valuer:
            valuers.append(self.return_valuer)
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
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return None

    def require_loaded(self):
        return True