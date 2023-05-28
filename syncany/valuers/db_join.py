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
        self.require_yield_values = False
        if self.intercept_valuer:
            setattr(self.intercept_valuer, "intercept_wait_loaded", self.intercept_valuer.require_loaded())

    def clone_init(self, from_valuer):
        super(DBJoinValuer, self).clone_init(from_valuer)
        self.require_yield_values = from_valuer.require_yield_values
        if self.intercept_valuer:
            self.intercept_valuer.intercept_wait_loaded = from_valuer.intercept_valuer.intercept_wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, db_join_valuers=None, loader=None, **kwargs):
        self.loader.primary_loader = loader
        if is_return_getter:
            self.require_yield_values = True
        if db_join_valuers is None:
            db_join_valuers = []
        db_join_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, db_join_valuers=db_join_valuers, **kwargs)
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                args_valuer.mount_loader(is_return_getter=False, db_join_valuers=db_join_valuers, **kwargs)
        if self.intercept_valuer:
            self.intercept_valuer.mount_loader(is_return_getter=False, db_join_valuers=db_join_valuers, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, db_join_valuers=db_join_valuers,
                                            **kwargs)

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

    def reinit(self):
        self.matcher = None
        return super(DBJoinValuer, self).reinit()

    def create_matcher(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                if self.require_yield_values:
                    group_macther = self.loader.create_group_matcher(is_yield=True)
                else:
                    group_macther = self.loader.create_group_matcher(is_yield=False,
                                                                     return_valuer=self.return_valuer.clone(inherited=True),
                                                                     contexter_valuesself=None)
                for value in data:
                    group_macther.add_matcher(self.create_matcher(value))
                return group_macther
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.clone(inherited=True).fill(data)

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
            return self.create_group_matcher(join_values)
        if self.require_yield_values:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=True, intercept_valuer=self.intercept_valuer,
                                                 valuer=self.return_valuer.clone(inherited=True), contexter_values=None)
        else:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=False, intercept_valuer=self.intercept_valuer,
                                                 valuer=None, contexter_values=None)
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
        if self.require_yield_values:
            group_macther = self.loader.create_group_matcher(is_yield=True)
        else:
            group_macther = self.loader.create_group_matcher(is_yield=False,
                                                             return_valuer=self.return_valuer.clone(inherited=True),
                                                             contexter_valuesself=None)
        for join_value in join_values:
            if self.require_yield_values:
                matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                     is_yield=True, intercept_valuer=self.intercept_valuer,
                                                     valuer=self.return_valuer.clone(inherited=True), contexter_values=None)
            else:
                matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                     is_yield=False, intercept_valuer=self.intercept_valuer,
                                                     valuer=None, contexter_values=None)
            self.loader.add_macther(matcher, self.foreign_keys, join_value)
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
            self.matcher = self.create_group_matcher(join_values)
        else:
            self.matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                      is_yield=self.require_yield_values,
                                                      intercept_valuer=self.intercept_valuer,
                                                      valuer=self.return_valuer,
                                                      contexter_values=None)

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
                if self.require_yield_values:
                    group_macther = self.loader.create_group_matcher(is_yield=True)
                    for value in data:
                        group_macther.add_matcher(self.create_matcher(value))
                    return group_macther

                contexter_values = self.contexter.values
                self.contexter.values = self.contexter.create_inherit_values(contexter_values)
                group_macther = self.loader.create_group_matcher(is_yield=False, return_valuer=self.return_valuer,
                                                                 contexter_values=self.contexter.values)
                for value in data:
                    group_macther.add_matcher(self.create_matcher(value))
                self.contexter.values = contexter_values
                return group_macther
            data = data[0] if data else None

        contexter_values = self.contexter.values
        self.contexter.values = self.contexter.create_inherit_values(contexter_values)
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
            return self.create_group_matcher(join_values)
        if self.require_yield_values:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=True, intercept_valuer=self.intercept_valuer,
                                                 valuer=self.return_valuer, contexter_values=self.contexter.values)
        else:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=False, intercept_valuer=self.intercept_valuer,
                                                 valuer=None, contexter_values=self.contexter.values)
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
        contexter_values = self.contexter.values
        if self.require_yield_values:
            group_macther = self.loader.create_group_matcher(is_yield=True)
        else:
            self.contexter.values = self.contexter.create_inherit_values(contexter_values)
            group_macther = self.loader.create_group_matcher(is_yield=False, return_valuer=self.return_valuer,
                                                             contexter_valuesself=self.contexter.values)
        for join_value in join_values:
            if self.require_yield_values:
                self.contexter.values = self.contexter.create_inherit_values(contexter_values)
                matcher = self.loader.create_matcher(self.foreign_keys, join_value,
                                                     is_yield=True, intercept_valuer=self.intercept_valuer,
                                                     valuer=self.return_valuer, contexter_values=self.contexter.values)
            else:
                matcher = self.loader.create_matcher(self.foreign_keys, join_value,
                                                     is_yield=False, intercept_valuer=self.intercept_valuer,
                                                     valuer=None, contexter_values=self.contexter.values)
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
            self.contexter.values[self.matcher_context_id] = self.create_group_matcher(join_values)
        else:
            self.contexter.values[self.matcher_context_id] = self.loader.create_matcher(self.foreign_keys, join_values,
                                                                                        is_yield=self.require_yield_values,
                                                                                        intercept_valuer=self.intercept_valuer,
                                                                                        valuer=self.return_valuer,
                                                                                        contexter_values=self.contexter.values)

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
