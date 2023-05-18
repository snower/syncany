# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer


class DBJoinValuer(Valuer):
    def __init__(self, loader, foreign_keys, foreign_filters, args_valuers, intercept_valuer, return_valuer,
                 inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_keys = foreign_keys
        self.args_valuers = args_valuers
        self.intercept_valuer = intercept_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        super(DBJoinValuer, self).__init__(*args, **kwargs)

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        args_valuers = [args_valuer.clone(contexter) for args_valuer in self.args_valuers] if self.args_valuers else None
        intercept_valuer = self.intercept_valuer.clone(contexter) if self.intercept_valuer else None
        return_valuer = self.return_valuer.clone(contexter)
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextDBJoinValuer(self.loader, self.foreign_keys, self.foreign_filters,
                                       args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextDBJoinValuer):
            return ContextDBJoinValuer(self.loader, self.foreign_keys, self.foreign_filters,
                                       args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.loader, self.foreign_keys, self.foreign_filters,
                              args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, max_value_size, has_join_value = [], 0, False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                join_value = args_valuer.fill(data).get()
                if isinstance(join_value, list):
                    if len(join_value) > max_value_size:
                        max_value_size = len(join_value)
                    has_join_value = True
                else:
                    max_value_size, has_join_value = max_value_size or 1, join_value is not None
                join_values.append(join_value)
        else:
            if self.key:
                super(DBJoinValuer, self).fill(data)
            join_value = self.value
            for _ in self.foreign_keys:
                if isinstance(join_value, list):
                    if len(join_value) > max_value_size:
                        max_value_size = len(join_value)
                    has_join_value = True
                else:
                    max_value_size, has_join_value = max_value_size or 1, join_value is not None
                join_values.append(join_value)

        if max_value_size > 1:
            group_macther = self.loader.create_group_macther(DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                        self.return_valuer, None)
                                                             if self.intercept_valuer else self.return_valuer)
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
                return_valuer = DBJoinGroupMatchValuer(group_macther, "*")
                matcher.add_valuer(return_valuer)
                group_macther.add_valuer(return_valuer)
        elif has_join_value:
            matcher = self.loader.create_macther(self.foreign_keys, join_values)
            if self.intercept_valuer:
                matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                      self.return_valuer, None, "*"), None
            else:
                matcher.valuer, matcher.contexter_values = self.return_valuer, None

        self.loader.wait_try_load_count += 1
        if self.loader.wait_try_load_count >= 64:
            self.loader.try_load()
            self.loader.wait_try_load_count = 0
        return self

    def get(self):
        self.loader.load()
        return self.return_valuer.get()

    def childs(self):
        valuers = []
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                valuers.append(args_valuer)
        if self.intercept_valuer:
            valuers.append(self.intercept_valuer)
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
        if self.intercept_valuer:
            for field in self.intercept_valuer.get_fields():
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


class ContextDBJoinValuer(DBJoinValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextDBJoinValuer, self).__init__(*args, **kwargs)

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, max_value_size, has_join_value = [], 0, False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                join_value = args_valuer.fill(data).get()
                if isinstance(join_value, list):
                    if len(join_value) > max_value_size:
                        max_value_size = len(join_value)
                    has_join_value = True
                else:
                    max_value_size, has_join_value = max_value_size or 1, join_value is not None
                join_values.append(join_value)
        else:
            if self.key:
                super(DBJoinValuer, self).fill(data)
            join_value = self.value
            for _ in self.foreign_keys:
                if isinstance(join_value, list):
                    if len(join_value) > max_value_size:
                        max_value_size = len(join_value)
                    has_join_value = True
                else:
                    max_value_size, has_join_value = max_value_size or 1, join_value is not None
                join_values.append(join_value)

        if max_value_size > 1:
            group_macther = self.loader.create_group_macther(DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                        self.return_valuer, self.contexter)
                                                             if self.intercept_valuer else self.return_valuer)
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
                return_valuer = DBJoinGroupMatchValuer(group_macther, "*")
                matcher.add_valuer(return_valuer)
                group_macther.add_valuer(return_valuer)
        elif has_join_value:
            matcher = self.loader.create_macther(self.foreign_keys, join_values)
            if self.intercept_valuer:
                matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                      self.return_valuer, self.contexter, "*"), self.contexter.values
            else:
                matcher.valuer, matcher.contexter_values = self.return_valuer, self.contexter.values

        self.loader.wait_try_load_count += 1
        if self.loader.wait_try_load_count >= 64:
            contexter_values = self.contexter.values
            try:
                self.loader.try_load()
            finally:
                self.contexter.values = contexter_values
            self.loader.wait_try_load_count = 0
        return self

    def get(self):
        contexter_values = self.contexter.values
        try:
            self.loader.load()
        finally:
            self.contexter.values = contexter_values
        return self.return_valuer.get()


class DBJoinInterceptMatchValuer(Valuer):
    def __init__(self, intercept_valuer, return_valuer, contexter, *args, **kwargs):
        self.contexter = contexter
        self.intercept_valuer = intercept_valuer
        self.return_valuer = return_valuer
        super(DBJoinInterceptMatchValuer, self).__init__(*args, **kwargs)

    def clone(self, contexter=None):
        return DBJoinInterceptMatchValuer(self.intercept_valuer, self.return_valuer, contexter or self.contexter,
                                          self.key, self.filter)

    def fill(self, data):
        if isinstance(data, list):
            result = []
            for value in data:
                intercept_result = self.intercept_valuer.fill(value).get()
                if intercept_result is not None and not intercept_result:
                    continue
                result.append(value)
            if len(result) == 1:
                self.return_valuer.fill(result[0])
            else:
                self.return_valuer.fill(result or None)
            return self

        intercept_result = self.intercept_valuer.fill(data).get()
        if intercept_result is not None and not intercept_result:
            self.return_valuer.fill(None)
            return self
        self.return_valuer.fill(data)
        return self

    def get(self):
        return self.return_valuer.get()


class DBJoinGroupMatchValuer(Valuer):
    def __init__(self, matcher, *args, **kwargs):
        self.matcher = matcher
        self.loaded = False
        super(DBJoinGroupMatchValuer, self).__init__(*args, **kwargs)

    def clone(self, contexter=None):
        return self.__class__(self.matcher, self.key, self.filter)

    def fill(self, data):
        super(DBJoinGroupMatchValuer, self).fill(data)
        self.loaded = None if data is None else True
        self.matcher.fill(self, data)
        return self
