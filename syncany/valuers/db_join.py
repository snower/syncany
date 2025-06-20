# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer


class DBJoinValuer(Valuer):
    matcher = None

    def __init__(self, loader, foreign_keys, foreign_key_filters, foreign_querys, args_valuers, intercept_valuer, return_valuer,
                 inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_keys = foreign_keys
        self.foreign_key_filters = foreign_key_filters
        self.args_valuers = args_valuers
        self.intercept_valuer = intercept_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_querys = foreign_querys
        self.is_in_depth_citation = kwargs.pop("is_in_depth_citation", False)
        super(DBJoinValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(DBJoinValuer, self).new_init()
        self.require_yield_values = False
        if self.intercept_valuer:
            setattr(self.intercept_valuer, "intercept_wait_loaded", self.intercept_valuer.require_loaded())
        self.return_wait_loaded = self.return_valuer.require_loaded() if self.return_valuer else False
        self.is_in_depth_citation = self.is_in_depth_citation if self.is_in_depth_citation is not None else False
        self.join_batch = self.loader.join_batch
        self.wait_try_load_count = 0

    def clone_init(self, from_valuer):
        super(DBJoinValuer, self).clone_init(from_valuer)
        self.require_yield_values = from_valuer.require_yield_values
        if self.intercept_valuer:
            self.intercept_valuer.intercept_wait_loaded = from_valuer.intercept_valuer.intercept_wait_loaded
        self.return_wait_loaded = from_valuer.return_wait_loaded
        self.is_in_depth_citation = from_valuer.is_in_depth_citation
        self.join_batch = from_valuer.join_batch
        self.wait_try_load_count = 0

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_scoper(self, scoper=None, is_return_getter=True,db_join_valuers=None, loader=None, **kwargs):
        self.loader.primary_loader = loader
        if is_return_getter:
            self.require_yield_values = True
        if db_join_valuers is None:
            db_join_valuers = []
        db_join_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_scoper(scoper=scoper, is_return_getter=False,db_join_valuers=db_join_valuers, **kwargs)
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                args_valuer.mount_scoper(scoper=scoper, is_return_getter=False,db_join_valuers=db_join_valuers, **kwargs)
        if self.intercept_valuer:
            self.intercept_valuer.mount_scoper(scoper=scoper, is_return_getter=False,db_join_valuers=db_join_valuers, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_scoper(scoper=self.loader, is_return_getter=is_return_getter and True,
                                            db_join_valuers=db_join_valuers, **kwargs)
        self.optimize()

    def optimize(self):
        if self.args_valuers and len(self.args_valuers) == 1:
            self.args_valuer0_fill_get = self.args_valuers[0].fill_get
            self.fill = self.fill_args1

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        args_valuers = [args_valuer.clone(contexter, **kwargs) for args_valuer in self.args_valuers] if self.args_valuers else None
        intercept_valuer = self.intercept_valuer.clone(contexter, **kwargs) if self.intercept_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs)
        if contexter is not None:
            return ContextDBJoinValuer(self.loader, self.foreign_keys, self.foreign_key_filters, self.foreign_querys,
                                       args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextDBJoinValuer):
            return ContextDBJoinValuer(self.loader, self.foreign_keys, self.foreign_key_filters, self.foreign_querys,
                                       args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.loader, self.foreign_keys, self.foreign_key_filters, self.foreign_querys,
                              args_valuers, intercept_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.matcher = None
        return super(DBJoinValuer, self).reinit()

    def create_matcher(self, data):
        if isinstance(data, list):
            if len(data) > 1:
                if self.require_yield_values:
                    group_macther = self.loader.create_group_matcher(is_yield=True, return_value_wait_loaded=self.return_wait_loaded)
                else:
                    group_macther = self.loader.create_group_matcher(is_yield=False,
                                                                     return_valuer=self.return_valuer.clone(inherited=True),
                                                                     contexter_valuesself=None,
                                                                     return_value_wait_loaded=self.return_wait_loaded)
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
                if value.__class__ is list:
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if value.__class__ is list:
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            return self.create_group_matcher(join_values)
        if self.foreign_key_filters is not None:
            for i in range(min(len(self.foreign_key_filters), len(join_values))):
                foreign_key_filter = self.foreign_key_filters[i]
                if foreign_key_filter:
                    join_values[i] = foreign_key_filter.filter(join_values[i])
        if self.require_yield_values:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=True, intercept_valuer=self.intercept_valuer,
                                                 valuer=self.return_valuer.clone(inherited=True), contexter_values=None,
                                                 return_value_wait_loaded=self.return_wait_loaded)
        else:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=False, intercept_valuer=self.intercept_valuer,
                                                 valuer=None, contexter_values=None,
                                                 return_value_wait_loaded=self.return_wait_loaded)
        return matcher

    def create_group_matcher(self, join_values):
        def flat_join_values(join_values, list_indexs, i):
            if i >= len(list_indexs):
                return join_values
            cjoin_values = flat_join_values(join_values, list_indexs, i + 1)
            j, rjoin_values = list_indexs[i], []
            foreign_key_filter = self.foreign_key_filters[j] if self.foreign_key_filters is not None \
                                                                and j < len(self.foreign_key_filters[j]) else None
            for join_value in join_values[j]:
                for cjoin_value in cjoin_values:
                    cjoin_value = cjoin_value[:]
                    cjoin_value[j] = join_value if foreign_key_filter is None else foreign_key_filter.filter(join_value)
                    rjoin_values.append(cjoin_value)
            return rjoin_values

        list_indexs = [i for i in range(len(join_values)) if isinstance(join_values[i], list)]
        join_values = flat_join_values(join_values, list_indexs, 0)
        if self.require_yield_values:
            group_macther = self.loader.create_group_matcher(is_yield=True, return_value_wait_loaded=self.return_wait_loaded)
        else:
            group_macther = self.loader.create_group_matcher(is_yield=False,
                                                             return_valuer=self.return_valuer.clone(inherited=True),
                                                             contexter_valuesself=None,
                                                             return_value_wait_loaded=self.return_wait_loaded)
        for join_value in join_values:
            if self.require_yield_values:
                matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                     is_yield=True, intercept_valuer=self.intercept_valuer,
                                                     valuer=self.return_valuer.clone(inherited=True), contexter_values=None,
                                                     return_value_wait_loaded=self.return_wait_loaded)
            else:
                matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                     is_yield=False, intercept_valuer=self.intercept_valuer,
                                                     valuer=None, contexter_values=None,
                                                     return_value_wait_loaded=self.return_wait_loaded)
            self.loader.add_macther(matcher, self.foreign_keys, join_value)
            group_macther.add_matcher(matcher)
        return group_macther

    def fill_list_data(self, data):
        self.matcher = self.create_matcher(data)
        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            self.loader.try_load()
            self.wait_try_load_count = 0
        return self

    def fill(self, data):
        if data.__class__ is not dict and isinstance(data, list):
            if len(data) > 1:
                return self.fill_list_data(data)
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, has_list_args = [], False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                value = args_valuer.fill_get(data)
                if value.__class__ is list:
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if value.__class__ is list:
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            self.matcher = self.create_group_matcher(join_values)
        else:
            if self.foreign_key_filters is not None:
                for i in range(min(len(self.foreign_key_filters), len(join_values))):
                    foreign_key_filter = self.foreign_key_filters[i]
                    if foreign_key_filter:
                        join_values[i] = foreign_key_filter.filter(join_values[i])
            self.matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                      is_yield=self.require_yield_values,
                                                      intercept_valuer=self.intercept_valuer,
                                                      valuer=self.return_valuer,
                                                      contexter_values=None,
                                                      return_value_wait_loaded=self.return_wait_loaded)

        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            self.loader.try_load()
            self.wait_try_load_count = 0
        return self

    def fill_args1(self, data):
        if data.__class__ is not dict and isinstance(data, list):
            if len(data) > 1:
                return self.fill_list_data(data)
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        value = self.args_valuer0_fill_get(data)
        if value.__class__ is list:
            if len(value) == 1:
                if self.foreign_key_filters is not None and self.foreign_key_filters and self.foreign_key_filters[0]:
                    value = [self.foreign_key_filters[0].filter(value[0])]
                self.matcher = self.loader.create_matcher(self.foreign_keys, value,
                                                          is_yield=self.require_yield_values,
                                                          intercept_valuer=self.intercept_valuer,
                                                          valuer=self.return_valuer,
                                                          contexter_values=None,
                                                          return_value_wait_loaded=self.return_wait_loaded)
            else:
                self.matcher = self.create_group_matcher([value])
        else:
            if self.foreign_key_filters is not None and self.foreign_key_filters and self.foreign_key_filters[0]:
                value = self.foreign_key_filters[0].filter(value)
            self.matcher = self.loader.create_matcher(self.foreign_keys, [value],
                                                      is_yield=self.require_yield_values,
                                                      intercept_valuer=self.intercept_valuer,
                                                      valuer=self.return_valuer,
                                                      contexter_values=None,
                                                      return_value_wait_loaded=self.return_wait_loaded)

        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            self.loader.try_load()
            self.wait_try_load_count = 0
        return self

    def get(self):
        self.loader.load()
        return self.matcher.get(is_in_depth_citation=self.is_in_depth_citation)

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
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return None

    def is_const(self):
        return False

    def require_loaded(self):
        return True


class ContextDBJoinValuer(DBJoinValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
        self.matcher_context_id = id(self) * 10 + 1
        super(ContextDBJoinValuer, self).__init__(*args, **kwargs)

    def optimize(self):
        if self.args_valuers and len(self.args_valuers) == 1:
            self.args_valuer0_fill_get = self.args_valuers[0].fill_get
            self.fill = self.fill_args1

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
                    group_macther = self.loader.create_group_matcher(is_yield=True, return_value_wait_loaded=self.return_wait_loaded)
                    for value in data:
                        group_macther.add_matcher(self.create_matcher(value))
                    return group_macther

                contexter_values = self.contexter.values
                self.contexter.values = self.contexter.create_inherit_values(contexter_values)
                group_macther = self.loader.create_group_matcher(is_yield=False, return_valuer=self.return_valuer,
                                                                 contexter_values=self.contexter.values,
                                                                 return_value_wait_loaded=self.return_wait_loaded)
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
                if value.__class__ is list:
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if value.__class__ is list:
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            return self.create_group_matcher(join_values)
        if self.foreign_key_filters is not None:
            for i in range(min(len(self.foreign_key_filters), len(join_values))):
                foreign_key_filter = self.foreign_key_filters[i]
                if foreign_key_filter:
                    join_values[i] = foreign_key_filter.filter(join_values[i])
        if self.require_yield_values:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=True, intercept_valuer=self.intercept_valuer,
                                                 valuer=self.return_valuer, contexter_values=self.contexter.values,
                                                 return_value_wait_loaded=self.return_wait_loaded)
        else:
            matcher = self.loader.create_matcher(self.foreign_keys, join_values,
                                                 is_yield=False, intercept_valuer=self.intercept_valuer,
                                                 valuer=None, contexter_values=self.contexter.values,
                                                 return_value_wait_loaded=self.return_wait_loaded)
        self.contexter.values = contexter_values
        return matcher

    def create_group_matcher(self, join_values):
        def flat_join_values(join_values, list_indexs, i):
            if i >= len(list_indexs):
                return join_values
            cjoin_values = flat_join_values(join_values, list_indexs, i + 1)
            j, rjoin_values = list_indexs[i], []
            foreign_key_filter = self.foreign_key_filters[j] if self.foreign_key_filters is not None \
                                                                and j < len(self.foreign_key_filters[j]) else None
            for join_value in join_values[j]:
                for cjoin_value in cjoin_values:
                    cjoin_value = cjoin_value[:]
                    cjoin_value[j] = join_value if foreign_key_filter is None else foreign_key_filter.filter(join_value)
                    rjoin_values.append(cjoin_value)
            return rjoin_values

        list_indexs = [i for i in range(len(join_values)) if isinstance(join_values[i], list)]
        join_values = flat_join_values(join_values, list_indexs, 0)
        contexter_values = self.contexter.values
        if self.require_yield_values:
            group_macther = self.loader.create_group_matcher(is_yield=True, return_value_wait_loaded=self.return_wait_loaded)
        else:
            self.contexter.values = self.contexter.create_inherit_values(contexter_values)
            group_macther = self.loader.create_group_matcher(is_yield=False, return_valuer=self.return_valuer,
                                                             contexter_valuesself=self.contexter.values,
                                                             return_value_wait_loaded=self.return_wait_loaded)
        for join_value in join_values:
            if self.require_yield_values:
                self.contexter.values = self.contexter.create_inherit_values(contexter_values)
                matcher = self.loader.create_matcher(self.foreign_keys, join_value,
                                                     is_yield=True, intercept_valuer=self.intercept_valuer,
                                                     valuer=self.return_valuer, contexter_values=self.contexter.values,
                                                     return_value_wait_loaded=self.return_wait_loaded)
            else:
                matcher = self.loader.create_matcher(self.foreign_keys, join_value,
                                                     is_yield=False, intercept_valuer=self.intercept_valuer,
                                                     valuer=None, contexter_values=self.contexter.values,
                                                     return_value_wait_loaded=self.return_wait_loaded)
            group_macther.add_matcher(matcher)
        self.contexter.values = contexter_values
        return group_macther

    def fill_list_data(self, data):
        self.contexter.values[self.matcher_context_id] = self.create_matcher(data)
        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            contexter_values = self.contexter.values
            try:
                self.loader.try_load()
            finally:
                self.contexter.values = contexter_values
            self.wait_try_load_count = 0
        return self

    def fill(self, data):
        if data.__class__ is not dict and isinstance(data, list):
            if len(data) > 1:
                return self.fill_list_data(data)
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        join_values, has_list_args = [], False
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                value = args_valuer.fill_get(data)
                if value.__class__ is list:
                    if len(value) == 1:
                        value = value[0]
                    else:
                        has_list_args = True
                join_values.append(value)
        else:
            value = super(DBJoinValuer, self).fill_get(data)
            if value.__class__ is list:
                if len(value) == 1:
                    value = value[0]
                else:
                    has_list_args = True
            join_values.append(value)
        if has_list_args:
            self.contexter.values[self.matcher_context_id] = self.create_group_matcher(join_values)
        else:
            if self.foreign_key_filters is not None:
                for i in range(min(len(self.foreign_key_filters), len(join_values))):
                    foreign_key_filter = self.foreign_key_filters[i]
                    if foreign_key_filter:
                        join_values[i] = foreign_key_filter.filter(join_values[i])
            self.contexter.values[self.matcher_context_id] = self.loader.create_matcher(self.foreign_keys, join_values,
                                                                                        is_yield=self.require_yield_values,
                                                                                        intercept_valuer=self.intercept_valuer,
                                                                                        valuer=self.return_valuer,
                                                                                        contexter_values=self.contexter.values,
                                                                                        return_value_wait_loaded=self.return_wait_loaded)

        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            contexter_values = self.contexter.values
            try:
                self.loader.try_load()
            finally:
                self.contexter.values = contexter_values
            self.wait_try_load_count = 0
        return self

    def fill_args1(self, data):
        if data.__class__ is not dict and isinstance(data, list):
            if len(data) > 1:
                return self.fill_list_data(data)
            data = data[0] if data else None

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        value = self.args_valuer0_fill_get(data)
        if value.__class__ is list:
            if len(value) == 1:
                if self.foreign_key_filters is not None and self.foreign_key_filters and self.foreign_key_filters[0]:
                    value = [self.foreign_key_filters[0].filter(value[0])]
                self.contexter.values[self.matcher_context_id] = self.loader.create_matcher(self.foreign_keys, value,
                                                                                            is_yield=self.require_yield_values,
                                                                                            intercept_valuer=self.intercept_valuer,
                                                                                            valuer=self.return_valuer,
                                                                                            contexter_values=self.contexter.values,
                                                                                            return_value_wait_loaded=self.return_wait_loaded)
            else:
                self.contexter.values[self.matcher_context_id] = self.create_group_matcher([value])
        else:
            if self.foreign_key_filters is not None and self.foreign_key_filters and self.foreign_key_filters[0]:
                value = self.foreign_key_filters[0].filter(value)
            self.contexter.values[self.matcher_context_id] = self.loader.create_matcher(self.foreign_keys, [value],
                                                                                        is_yield=self.require_yield_values,
                                                                                        intercept_valuer=self.intercept_valuer,
                                                                                        valuer=self.return_valuer,
                                                                                        contexter_values=self.contexter.values,
                                                                                        return_value_wait_loaded=self.return_wait_loaded)

        self.wait_try_load_count += 1
        if self.wait_try_load_count >= self.join_batch:
            contexter_values = self.contexter.values
            try:
                self.loader.try_load()
            finally:
                self.contexter.values = contexter_values
            self.wait_try_load_count = 0
        return self

    def get(self):
        contexter_values = self.contexter.values
        try:
            self.loader.load()
            return contexter_values[self.matcher_context_id].get(is_in_depth_citation=self.is_in_depth_citation)
        finally:
            self.contexter.values = contexter_values

    def fill_get(self, data):
        return self.fill(data).get()
