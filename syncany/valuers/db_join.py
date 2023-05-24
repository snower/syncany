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

    def new_init(self):
        super(DBJoinValuer, self).new_init()
        self.is_aggregate_return_valuer = True if self.return_valuer and self.return_valuer.is_aggregate() else False
        self.is_yield_return_valuer = True if self.return_valuer and self.return_valuer.is_yield() else False

    def clone_init(self, from_valuer):
        super(DBJoinValuer, self).clone_init(from_valuer)
        self.is_aggregate_return_valuer = from_valuer.is_aggregate_return_valuer
        self.is_yield_return_valuer = from_valuer.is_yield_return_valuer

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
                group_macther = self.loader.create_group_macther(self.is_aggregate_return_valuer, self.is_yield_return_valuer)
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
        if self.intercept_valuer:
            matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                  self.return_valuer.clone(),
                                                                                  None), None
        else:
            matcher.valuer, matcher.contexter_values = self.return_valuer.clone(), None
        return matcher

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

        if self.args_valuers:
            join_values = [args_valuer.fill_get(data) for args_valuer in self.args_valuers]
        else:
            join_values = [super(DBJoinValuer, self).fill_get(data)]
        matcher = self.loader.create_macther(self.foreign_keys, join_values)
        if self.intercept_valuer:
            matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                  self.return_valuer, None), None
        else:
            matcher.valuer, matcher.contexter_values = self.return_valuer, None
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
                group_macther = self.loader.create_group_macther(self.is_aggregate_return_valuer, self.is_yield_return_valuer)
                for value in data:
                    group_macther.add_matcher(self.create_matcher(value))
                return group_macther
            data = data[0] if data else None

        contexter_values, self.contexter.values = self.contexter.values, {key: value for key, value in self.contexter.values.items()}
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.args_valuers:
            join_values = [args_valuer.fill_get(data) for args_valuer in self.args_valuers]
        else:
            join_values = [super(DBJoinValuer, self).fill_get(data)]
        matcher = self.loader.create_macther(self.foreign_keys, join_values)
        if self.intercept_valuer:
            matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                  self.return_valuer,
                                                                                  self.contexter), self.contexter.values
        else:
            matcher.valuer, matcher.contexter_values = self.return_valuer, self.contexter.values
        self.contexter.values = contexter_values
        return matcher

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

        if self.args_valuers:
            join_values = [args_valuer.fill_get(data) for args_valuer in self.args_valuers]
        else:
            join_values = [super(DBJoinValuer, self).fill_get(data)]
        matcher = self.loader.create_macther(self.foreign_keys, join_values)
        if self.intercept_valuer:
            matcher.valuer, matcher.contexter_values = DBJoinInterceptMatchValuer(self.intercept_valuer,
                                                                                  self.return_valuer,
                                                                                  self.contexter), self.contexter.values
        else:
            matcher.valuer, matcher.contexter_values = self.return_valuer, self.contexter.values
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


class DBJoinInterceptMatchValuer(Valuer):
    def __init__(self, intercept_valuer, return_valuer, contexter, *args, **kwargs):
        self.intercept_valuer = intercept_valuer
        self.return_valuer = return_valuer
        self.contexter = contexter
        super(DBJoinInterceptMatchValuer, self).__init__("*", *args, **kwargs)

    def clone(self, contexter=None, **kwargs):
        return DBJoinInterceptMatchValuer(self.intercept_valuer, self.return_valuer, contexter or self.contexter,
                                          self.key, self.filter)

    def fill(self, data):
        if isinstance(data, list):
            result = []
            for value in data:
                intercept_result = self.intercept_valuer.fill_get(value)
                if intercept_result is not None and not intercept_result:
                    continue
                result.append(value)
            if len(result) == 1:
                self.return_valuer.fill(result[0])
            else:
                self.return_valuer.fill(result or None)
            return self

        intercept_result = self.intercept_valuer.fill_get(data)
        if intercept_result is not None and not intercept_result:
            self.return_valuer.fill(None)
            return self
        self.return_valuer.fill(data)
        return self

    def get(self):
        return self.return_valuer.get()

    def fill_get(self, data):
        return self.fill(data).get()
