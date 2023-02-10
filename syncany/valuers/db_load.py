# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer


class DBLoadValuer(Valuer):
    def __init__(self, loader, foreign_keys, foreign_filters, return_valuer, inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_keys = foreign_keys
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        super(DBLoadValuer, self).__init__(*args, **kwargs)

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        return_valuer = self.return_valuer.clone()
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.loader, self.foreign_keys, self.foreign_filters,
                              return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not hasattr(self.loader, "db_load_valuer_loaded_datas"):
            self.loader.load()
            setattr(self.loader, "db_load_valuer_loaded_datas", self.loader.get())
        self.return_valuer.fill(self.loader.db_load_valuer_loaded_datas)
        return self

    def get(self):
        return self.return_valuer.get()

    def childs(self):
        valuers = []
        if self.return_valuer:
            valuers.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                valuers.append(inherit_valuer)
        return valuers

    def get_fields(self):
        fields = []
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
