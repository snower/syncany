# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer


class DBJoinValuer(Valuer):
    matcher = None

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

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        args_valuers = [args_valuer.clone(contexter, **kwargs) for args_valuer in self.args_valuers] if self.args_valuers else None
        intercept_valuer = self.intercept_valuer.clone(contexter, **kwargs) if self.intercept_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs)
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

    def create_matcher(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                group_macther = self.loader.create_group_macther()
                for value in data:
                    group_macther.add_matcher(self.create_matcher(value))
                return group_macther
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.clone().fill(data)

        if self.args_valuers:
            join_values = [args_valuer.fill_get(data) for args_valuer in self.args_valuers]
        else:
            join_values = [super(DBJoinValuer, self).fill_get(data)]
        matcher = self.loader.create_macther(self.foreign_keys, join_values)
        matcher.valuer, matcher.intercept_valuer = self.return_valuer.clone(), self.intercept_valuer
        return matcher

    def create_group_matcher(self, join_values):
        def flat_join_values(join_values, list_indexs, i):
            if i >= len(list_indexs):
                return join_values
            cjoin_values = flat_join_values(join_values, list_indexs, i + 1)
            rjoin_values = []
            for join_value in join_values[list_indexs[i]]:
                for cjoin_value in cjoin_values:
                    cjoin_value = cjoin_value[:]
                    cjoin_value[list_indexs[i]] = join_value
                    rjoin_values.append(cjoin_value)
            return rjoin_values

        list_indexs = [i for i in range(len(join_values)) if isinstance(join_values[i], list)]
        join_values = flat_join_values(join_values, list_indexs, 0)
        group_macther = self.loader.create_group_macther()
        for join_value in join_values:
            matcher = self.loader.create_macther(self.foreign_keys, join_value)
            matcher.valuer, matcher.intercept_valuer = self.return_valuer.clone(), self.intercept_valuer
            group_macther.add_matcher(matcher)
        return group_macther

    def fill(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                self.matcher = self.create_matcher(data)
                self.loader.wait_try_load_count += 1
                if self.loader.wait_try_load_count >= 64:
                    self.loader.try_load()
                    self.loader.wait_try_load_count = 0
                return self
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, has_list_args = [], False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                value = args_valuer.fill_get(data)
                if isinstance(value, list):
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if isinstance(value, list):
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            matcher = self.create_group_matcher(join_values)
        else:
            matcher = self.loader.create_macther(self.foreign_keys, join_values)
            matcher.valuer, matcher.intercept_valuer = self.return_valuer, self.intercept_valuer
        self.matcher = matcher

        self.loader.wait_try_load_count += 1
        if self.loader.wait_try_load_count >= 64:
            self.loader.try_load()
            self.loader.wait_try_load_count = 0
        return self

    def get(self):
        self.loader.load()
        return self.matcher.get()

    def fill_get(self, data):
        return self.fill(data).get()

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
        self.matcher_context_id = (id(self), "matcher")
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

    @property
    def matcher(self):
        try:
            return self.contexter.values[self.matcher_context_id]
        except KeyError:
            return None

    @matcher.setter
    def matcher(self, v):
        if v is None:
            if self.matcher_context_id in self.contexter.values:
                self.contexter.values.pop(self.matcher_context_id)
            return
        self.contexter.values[self.matcher_context_id] = v

    def create_matcher(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                group_macther = self.loader.create_group_macther()
                for value in data:
                    group_macther.add_matcher(self.create_matcher(value))
                return group_macther
            data = data[0] if data else None

        contexter_values, self.contexter.values = self.contexter.values, {key: value
                                                                          for key, value in self.contexter.values.items()}
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.args_valuers:
            join_values = [args_valuer.fill_get(data) for args_valuer in self.args_valuers]
        else:
            join_values = [super(DBJoinValuer, self).fill_get(data)]
        matcher = self.loader.create_macther(self.foreign_keys, join_values)
        matcher.valuer, matcher.intercept_valuer, matcher.contexter_values = self.return_valuer, \
                                                                             self.intercept_valuer, self.contexter.values
        self.contexter.values = contexter_values
        return matcher

    def create_group_matcher(self, join_values):
        def flat_join_values(join_values, list_indexs, i):
            if i >= len(list_indexs):
                return join_values
            cjoin_values = flat_join_values(join_values, list_indexs, i + 1)
            rjoin_values = []
            for join_value in join_values[list_indexs[i]]:
                for cjoin_value in cjoin_values:
                    cjoin_value = cjoin_value[:]
                    cjoin_value[list_indexs[i]] = join_value
                    rjoin_values.append(cjoin_value)
            return rjoin_values

        list_indexs = [i for i in range(len(join_values)) if isinstance(join_values[i], list)]
        join_values = flat_join_values(join_values, list_indexs, 0)
        group_macther, contexter_values = self.loader.create_group_macther(), self.contexter.values
        for join_value in join_values:
            self.contexter.values = {key: value for key, value in contexter_values.items()}
            matcher = self.loader.create_macther(self.foreign_keys, join_value)
            matcher.valuer, matcher.intercept_valuer, matcher.contexter_values = self.return_valuer, \
                                                                                 self.intercept_valuer, self.contexter.values
            group_macther.add_matcher(matcher)
        self.contexter.values = contexter_values
        return group_macther

    def fill(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                self.contexter.values[self.matcher_context_id] = self.create_matcher(data)
                self.loader.wait_try_load_count += 1
                if self.loader.wait_try_load_count >= 64:
                    contexter_values = self.contexter.values
                    try:
                        self.loader.try_load()
                    finally:
                        self.contexter.values = contexter_values
                    self.loader.wait_try_load_count = 0
                return self
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, has_list_args = [], False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                value = args_valuer.fill_get(data)
                if isinstance(value, list):
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if isinstance(value, list):
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            matcher = self.create_group_matcher(join_values)
        else:
            matcher = self.loader.create_macther(self.foreign_keys, join_values)
            matcher.valuer, matcher.intercept_valuer, matcher.contexter_values = self.return_valuer, \
                                                                                 self.intercept_valuer, self.contexter.values
        self.contexter.values[self.matcher_context_id] = matcher

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
            return contexter_values[self.matcher_context_id].get()
        finally:
            self.contexter.values = contexter_values

    def fill_get(self, data):
        return self.fill(data).get()
