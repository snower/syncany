# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer


class DBJoinValuer(Valuer):
    def __init__(self, loader, foreign_keys, foreign_filters, args_valuers, return_valuer, inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_keys = foreign_keys
        self.args_valuers = args_valuers
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        super(DBJoinValuer, self).__init__(*args, **kwargs)

        self.matcher = None
        self.is_group_matcher = False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        args_valuers = [args_valuer.clone() for args_valuer in self.args_valuers] if self.args_valuers else None
        return_valuer = self.return_valuer.clone()
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.loader, self.foreign_keys, self.foreign_filters,
                              args_valuers, return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.args_valuers:
            join_values = []
            for args_valuer in self.args_valuers:
                args_valuer.fill(data)
                join_values.append(args_valuer.get())
        else:
            if self.key:
                super(DBJoinValuer, self).fill(data)
            join_values = [self.value for _ in self.foreign_keys]

        max_value_size = max([len(join_value) if isinstance(join_value, list) else 1 for join_value in join_values])
        if max_value_size > 1:
            self.matcher = self.loader.create_group_macther(self.return_valuer)
            self.is_group_matcher = True
            for i in range(max_value_size):
                ds, has_value = [], False
                for join_value in join_values:
                    if not isinstance(join_value, list):
                        ds.append(join_value)
                        has_value = True if join_value is not None else has_value
                    else:
                        ds.append(join_value[i] if i < len(join_value) else None)
                        has_value = True if i < len(join_value) else has_value
                if not has_value:
                    continue
                matcher = self.loader.create_macther(self.foreign_keys, ds)
                return_valuer = DBJoinGroupMatchValuer(self.matcher, "*")
                matcher.add_valuer(return_valuer)
                self.matcher.add_valuer(return_valuer)
        elif join_values and any([join_value is not None for join_value in join_values]):
            self.matcher = self.loader.create_macther(self.foreign_keys, join_values)
            self.matcher.add_valuer(self.return_valuer)
        self.loader.try_load()
        return self

    def get(self):
        self.loader.load()
        if self.is_group_matcher:
            self.matcher.get()
        return self.return_valuer.get()

    def childs(self):
        valuers = []
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                valuers.append(args_valuer)
        if self.return_valuer:
            valuers.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                valuers.append(inherit_valuer)
        return valuers

    def get_fields(self):
        fields = []

        if self.args_valuers:
            for args_valuer in self.args_valuers:
                for field in args_valuer.get_fields():
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


class DBJoinGroupMatchValuer(Valuer):
    def __init__(self, matcher, *args, **kwargs):
        self.matcher = matcher
        self.loaded = False
        super(DBJoinGroupMatchValuer, self).__init__(*args, **kwargs)

    def clone(self):
        return self.__class__(self.matcher, self.key, self.filter)

    def fill(self, data):
        super(DBJoinGroupMatchValuer, self).fill(data)
        self.loaded = None if data is None else True
        self.matcher.fill(self, data)
        return self