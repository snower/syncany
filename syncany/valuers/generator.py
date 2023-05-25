# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import types
from .valuer import Valuer, Contexter, LoadAllFieldsException


class YieldValuer(Valuer):
    iter_valuers = None
    iter_datas = None

    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(YieldValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(YieldValuer, self).new_init()
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(YieldValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, yield_valuers=None, **kwargs):
        if yield_valuers is None:
            yield_valuers = []
        yield_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, yield_valuers=yield_valuers, **kwargs)
        if self.value_valuer:
            self.value_valuer.mount_loader(is_return_getter=False, yield_valuers=yield_valuers, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, yield_valuers=yield_valuers, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        value_valuer = self.value_valuer.clone(contexter, **kwargs) if self.value_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextYieldValuer(value_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextYieldValuer):
            return ContextYieldValuer(value_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.iter_valuers = []
        self.iter_datas = []
        return super(YieldValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.value_wait_loaded:
            if self.value_valuer:
                data = self.value_valuer.fill_get(data)

            if not self.return_valuer:
                if isinstance(data, list):
                    self.iter_datas = [self.do_filter(value) for value in data]
                else:
                    self.iter_datas = [self.do_filter(data)]
                return self

            if not self.wait_loaded:
                if isinstance(data, list):
                    if len(data) == 1:
                        self.iter_datas = [self.return_valuer.fill_get(self.do_filter(data[0]))]
                    else:
                        iter_datas, return_valuer = [], self.return_valuer
                        for value in data:
                            value = return_valuer.fill_get(self.do_filter(value))
                            iter_datas.append(value)
                            if not isinstance(value, (types.FunctionType, types.GeneratorType)):
                                continue
                            return_valuer = self.return_valuer.clone(Contexter() if isinstance(self, ContextYieldValuer)
                                                                     else None, inherited=True)
                        self.iter_datas = iter_datas
                else:
                    self.iter_datas = [self.return_valuer.fill_get(self.do_filter(data))]
                return self

            if isinstance(data, list):
                if len(data) == 1:
                    self.iter_valuers = [self.return_valuer.fill(self.do_filter(data[0]))]
                else:
                    if isinstance(self, ContextYieldValuer):
                        iter_valuers, contexter_values = [], self.contexter.values
                        for value in data:
                            self.return_valuer.contexter.values = self.contexter.create_inherit_values(contexter_values)
                            self.return_valuer.fill(value)
                            iter_valuers.append((self.return_valuer, self.return_valuer.contexter.values))
                        self.iter_valuers = iter_valuers
                        self.return_valuer.contexter.values = contexter_values
                    else:
                        self.iter_valuers = [(self.return_valuer.clone().fill(self.do_filter(value)), None)
                                             for value in data]
            else:
                self.iter_valuers = [self.return_valuer.fill(self.do_filter(data))]
            return self

        if self.value_valuer:
            self.value_valuer.fill(data)
        return self

    def get(self):
        if self.value_wait_loaded:
            data = self.value_valuer.get()
            if not self.return_valuer:
                if isinstance(data, list):
                    if len(data) == 1:
                        return self.do_filter(data[0])
                    iter_datas = [self.do_filter(value) for value in data]
                else:
                    return self.do_filter(data)
            else:
                if isinstance(data, list):
                    if len(data) == 1:
                        return self.return_valuer.fill_get(self.do_filter(data[0]))
                    else:
                        iter_datas, return_valuer = [], self.return_valuer
                        for value in data:
                            value = return_valuer.fill_get(self.do_filter(value))
                            iter_datas.append(value)
                            if not isinstance(value, (types.FunctionType, types.GeneratorType)):
                                continue
                            return_valuer = self.return_valuer.clone(Contexter() if isinstance(self, ContextYieldValuer)
                                                                     else None, inherited=True)
                else:
                    return self.return_valuer.fill_get(self.do_filter(data))
        elif self.wait_loaded:
            iter_datas = []
            for iter_valuer, contexter_values in self.iter_valuers:
                if contexter_values is not None:
                    iter_valuer.contexter.values = contexter_values
                iter_datas.append(iter_valuer.get())
            if len(iter_datas) == 1:
                return iter_datas[0]
        else:
            iter_datas = self.iter_datas
            if len(iter_datas) == 1:
                return iter_datas[0]

        if not iter_datas:
            def skip_empty(cdata):
                raise StopIteration
            return skip_empty

        def gen_iter(datas):
            yield None
            for value in datas:
                if value is None:
                    continue
                yield value
        g = gen_iter(iter_datas)
        g.send(None)
        return g

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            data = self.value_valuer.fill_get(data)
        if not self.return_valuer:
            if isinstance(data, list):
                if len(data) == 1:
                    return self.do_filter(data[0])
                iter_datas = [self.do_filter(value) for value in data]
            else:
                return self.do_filter(data)
        else:
            if isinstance(data, list):
                if len(data) == 1:
                    return self.return_valuer.fill_get(self.do_filter(data[0]))
                else:
                    iter_datas, return_valuer = [], self.return_valuer
                    for value in data:
                        value = return_valuer.fill_get(self.do_filter(value))
                        iter_datas.append(value)
                        if not isinstance(value, (types.FunctionType, types.GeneratorType)):
                            continue
                        return_valuer = self.return_valuer.clone(Contexter() if isinstance(self, ContextYieldValuer)
                                                                 else None, inherited=True)
            else:
                return self.return_valuer.fill_get(self.do_filter(data))

        if not iter_datas:
            def skip_empty(cdata):
                raise StopIteration
            return skip_empty

        def gen_iter(datas):
            yield None
            for value in datas:
                if value is None:
                    continue
                yield value
        g = gen_iter(iter_datas)
        g.send(None)
        return g

    def childs(self):
        childs = []
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        is_pass, fields = False, []
        try:
            if self.value_valuer:
                for field in self.value_valuer.get_fields():
                    fields.append(field)
        except LoadAllFieldsException:
            is_pass = True

        if (not self.value_valuer or is_pass) and self.return_valuer:
            for field in self.return_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if self.filter:
            return self.filter
        return None

    def is_yield(self):
        return True


class ContextYieldValuer(YieldValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.iter_valuers_context_id = (id(self), "iter_valuers")
        self.iter_datas_context_id = (id(self), "iter_datas")
        super(ContextYieldValuer, self).__init__(*args, **kwargs)

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
    def iter_valuers(self):
        try:
            return self.contexter.values[self.iter_valuers_context_id]
        except KeyError:
            return []

    @iter_valuers.setter
    def iter_valuers(self, v):
        if not v:
            if self.iter_valuers_context_id in self.contexter.values:
                self.contexter.values.pop(self.iter_valuers_context_id)
            return
        self.contexter.values[self.iter_valuers_context_id] = v

    @property
    def iter_datas(self):
        try:
            return self.contexter.values[self.iter_datas_context_id]
        except KeyError:
            return []

    @iter_datas.setter
    def iter_datas(self, v):
        if not v:
            if self.iter_valuers_context_id in self.contexter.values:
                self.contexter.values.pop(self.iter_valuers_context_id)
            return
        self.contexter.values[self.iter_datas_context_id] = v
