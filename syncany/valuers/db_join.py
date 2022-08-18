# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer

class GroupDBJoinMatcher(object):
    def __init__(self, return_valuer):
        self.return_valuer = return_valuer
        self.valuers = []
        self.datas = None

    def add_valuer(self, valuer):
        self.valuers.append(valuer)

    def fill(self, valuer, data):
        if self.datas is not None:
            return self
        for valuer in self.valuers:
            if valuer.loaded is False:
                return self

        self.datas = []
        for valuer in self.valuers:
            if valuer.loaded is not True:
                continue
            self.datas.append(valuer.get())
        self.return_valuer.fill(self.datas)

    def get(self):
        if self.datas is not None:
            return self.datas

        self.datas = []
        for valuer in self.valuers:
            if valuer.loaded is not True:
                continue
            self.datas.append(valuer.get())
        self.return_valuer.fill(self.datas)
        return self.datas


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
            join_value = self.args_valuer.get()
        else:
            if self.key:
                super(DBJoinValuer, self).fill(data)
            join_value = self.value

        if isinstance(join_value, list):
            self.matcher = GroupDBJoinMatcher(self.return_valuer)
            for d in join_value:
                if d is None:
                    continue
                matcher = self.loader.filter_eq(self.foreign_key, d)
                return_valuer = DBJoinReturnValuer(self.matcher, "*")
                matcher.add_valuer(return_valuer)
                self.matcher.add_valuer(return_valuer)
        elif join_value is not None:
            self.matcher = self.loader.filter_eq(self.foreign_key, join_value)
            self.matcher.add_valuer(self.return_valuer)
        return self

    def get(self):
        self.loader.load()
        if isinstance(self.matcher, GroupDBJoinMatcher):
            self.matcher.get()
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


class DBJoinReturnValuer(Valuer):
    def __init__(self, matcher, *args, **kwargs):
        self.matcher = matcher
        self.loaded = False
        super(DBJoinReturnValuer, self).__init__(*args, **kwargs)

    def clone(self):
        return self.__class__(self.matcher, self.key, self.filter)

    def fill(self, data):
        super(DBJoinReturnValuer, self).fill(data)
        self.loaded = None if data is None else True
        self.matcher.fill(self, data)
        return self