# -*- coding: utf-8 -*-
# 2020/6/29
# create by: snower

import types
from .valuer import Valuer


class MakeValuer(Valuer):
    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(MakeValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(MakeValuer, self).new_init()
        self.value_wait_loaded = self.check_wait_loaded()
        self.value_is_yield = True if self.value_wait_loaded else self.check_is_yield()
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False
        self.return_is_aggregate = True if self.return_valuer and self.return_valuer.is_aggregate() else False
        self.optimize_fast_make()

    def clone_init(self, from_valuer):
        super(MakeValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.value_is_yield = from_valuer.value_is_yield
        self.wait_loaded = from_valuer.wait_loaded
        self.return_is_aggregate = from_valuer.return_is_aggregate
        self.optimize_fast_make()

    def optimize_fast_make(self):
        if self.value_wait_loaded or self.value_is_yield or self.wait_loaded:
            return
        if not isinstance(self.value_valuer, dict):
            return
        value_valuer_count = len(self.value_valuer)
        if value_valuer_count == 0:
            self.fill_get = self.fill_get_dict0
        elif value_valuer_count == 1:
            value_valuer_values = list(self.value_valuer.values())
            self.value_valuer_key0 = value_valuer_values[0][0].fill_get
            self.value_valuer_value0 = value_valuer_values[0][1].fill_get
            self.fill_get = self.fill_get_dict1
        elif value_valuer_count == 2:
            value_valuer_values = list(self.value_valuer.values())
            self.value_valuer_key0 = value_valuer_values[0][0].fill_get
            self.value_valuer_value0 = value_valuer_values[0][1].fill_get
            self.value_valuer_key1 = value_valuer_values[1][0].fill_get
            self.value_valuer_value1 = value_valuer_values[1][1].fill_get
            self.fill_get = self.fill_get_dict2
        else:
            self.fill_get = self.fill_get_dict

    def check_wait_loaded(self):
        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                if key_valuer.require_loaded():
                    return True
                if value_valuer.require_loaded():
                    return True
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                if value_valuer.require_loaded():
                    return True
        elif isinstance(self.value_valuer, Valuer):
            if self.value_valuer.require_loaded():
                return True
        return False

    def check_is_yield(self):
        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                if value_valuer.is_yield():
                    return True
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                if value_valuer.is_yield():
                    return True
        elif isinstance(self.value_valuer, Valuer):
            if self.value_valuer.is_yield():
                return True
        return False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                key_valuer.mount_loader(is_return_getter=False, **kwargs)
                value_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)
        elif isinstance(self.value_valuer, list):
            for valuer in self.value_valuer:
                valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        if isinstance(self.value_valuer, dict):
            value_valuer = {key: (key_valuer.clone(contexter, **kwargs), value_valuer.clone(contexter, **kwargs))
                            for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, list):
            value_valuer = [valuer.clone(contexter, **kwargs) for valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value_valuer = self.value_valuer.clone(contexter, **kwargs)
        else:
            value_valuer = None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextMakeValuer(value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextMakeValuer):
            return ContextMakeValuer(value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.value_wait_loaded and not self.value_is_yield:
            if isinstance(self.value_valuer, dict):
                value = {key_valuer.fill_get(data): value_valuer.fill_get(data)
                         for key_valuer, value_valuer in self.value_valuer.values()}
            elif isinstance(self.value_valuer, list):
                value = [value_valuer.fill_get(data) for value_valuer in self.value_valuer]
            elif isinstance(self.value_valuer, Valuer):
                value = self.do_filter(self.value_valuer.fill_get(data))
            else:
                value = self.do_filter(None)
            if self.return_valuer:
                if not self.wait_loaded:
                    self.value = self.return_valuer.fill_get(value)
                else:
                    self.return_valuer.fill(value)
            else:
                self.value = value
            return self

        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                key_valuer.fill(data)
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.fill(data)
        return self

    def get(self):
        if self.value_wait_loaded or self.value_is_yield:
            GeneratorType = types.GeneratorType
            if isinstance(self.value_valuer, dict):
                value, yield_value = {}, {}
                for key, (key_valuer, value_valuer) in self.value_valuer.items():
                    key_value, value_value = key_valuer.get(), value_valuer.get()
                    if isinstance(value_value, GeneratorType):
                        yield_value[key] = (key_value, value_value)
                        value[key_value] = None
                    else:
                        value[key_value] = value_value
            elif isinstance(self.value_valuer, list):
                value, yield_value = [], []
                for i in range(len(self.value_valuer)):
                    value_value = self.value_valuer[i].get()
                    if isinstance(value_value, GeneratorType):
                        yield_value.append((i, value_value))
                        value.append(None)
                    else:
                        value.append(value_value)
            elif isinstance(self.value_valuer, Valuer):
                value_value, yield_value = self.value_valuer.get(), None
                if isinstance(value_value, GeneratorType):
                    value, yield_value = None, value_value
                else:
                    value = self.do_filter(value_value)
            else:
                value, yield_value = self.do_filter(None), None
            if yield_value:
                return self.get_yield(value, yield_value, False)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value

        if self.return_valuer:
            if not self.wait_loaded:
                return self.value
            return self.return_valuer.get()
        return self.value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_is_yield:
            GeneratorType = types.GeneratorType
            if isinstance(self.value_valuer, dict):
                value, yield_value = {}, {}
                for key, (key_valuer, value_valuer) in self.value_valuer.items():
                    key_value, value_value = key_valuer.fill_get(data), value_valuer.fill_get(data)
                    if isinstance(value_value, GeneratorType):
                        yield_value[key] = (key_value, value_value)
                        value[key_value] = None
                    else:
                        value[key_value] = value_value
            elif isinstance(self.value_valuer, list):
                value, yield_value = [], []
                for i in range(len(self.value_valuer)):
                    value_value = self.value_valuer[i].fill_get(data)
                    if isinstance(value_value, GeneratorType):
                        yield_value.append((i, value_value))
                        value.append(None)
                    else:
                        value.append(value_value)
            elif isinstance(self.value_valuer, Valuer):
                value_value, yield_value = self.value_valuer.fill_get(data), None
                if isinstance(value_value, GeneratorType):
                    value, yield_value = None, value_value
                else:
                    value = self.do_filter(value_value)
            else:
                value, yield_value = self.do_filter(None), None
            if yield_value:
                return self.get_yield(value, yield_value, False)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value

        if isinstance(self.value_valuer, dict):
            value = {key_valuer.fill_get(data): value_valuer.fill_get(data)
                     for key_valuer, value_valuer in self.value_valuer.values()}
        elif isinstance(self.value_valuer, list):
            value = [value_valuer.fill_get(data) for value_valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value = self.do_filter(self.value_valuer.fill_get(data))
        else:
            value = self.do_filter(None)
        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def fill_get_dict0(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get({})
        return {}

    def fill_get_dict1(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get({self.value_valuer_key0(data): self.value_valuer_value0(data)})
        return {self.value_valuer_key0(data): self.value_valuer_value0(data)}

    def fill_get_dict2(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get({self.value_valuer_key0(data): self.value_valuer_value0(data),
                                                self.value_valuer_key1(data): self.value_valuer_value1(data)})
        return {self.value_valuer_key0(data): self.value_valuer_value0(data),
                self.value_valuer_key1(data): self.value_valuer_value1(data)}

    def fill_get_dict(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get({key_valuer.fill_get(data): value_valuer.fill_get(data)
                                                for key_valuer, value_valuer in self.value_valuer.values()})
        return {key_valuer.fill_get(data): value_valuer.fill_get(data)
                for key_valuer, value_valuer in self.value_valuer.values()}

    def get_yield(self, value, yield_value, has_yield_data=True):
        GeneratorType = types.GeneratorType
        if isinstance(value, dict):
            while True:
                has_value, ovalue, oyield_value = False, value.copy(), {}
                for key, (key_value, value_value) in yield_value.items():
                    try:
                        value_value = value_value.send(None)
                        if isinstance(value_value, GeneratorType):
                            oyield_value[key] = (key_value, value_value)
                        else:
                            ovalue[key_value] = value_value
                        has_value = True
                    except StopIteration:
                        ovalue[key_value] = None
                if not has_value:
                    if not has_yield_data:
                        if self.return_valuer:
                            yield self.return_valuer.fill_get(value)
                        else:
                            yield value
                    break
                has_yield_data = True
                if oyield_value:
                    yield self.get_yield(ovalue, oyield_value)
                elif self.return_valuer:
                    if self.return_is_aggregate:
                        yield self.get_aggregate(ovalue)
                    else:
                        yield self.return_valuer.fill_get(ovalue)
                else:
                    yield ovalue
        elif isinstance(yield_value, list):
            while True:
                has_value, ovalue, oyield_value = False, value[:], []
                for i, value_value in yield_value:
                    try:
                        value_value = value_value.send(None)
                        if isinstance(value_value, GeneratorType):
                            oyield_value.append((i, value_value))
                        else:
                            ovalue[i] = value_value
                        has_value = True
                    except StopIteration:
                        ovalue[i] = None
                if not has_value:
                    if not has_yield_data:
                        if self.return_valuer:
                            yield self.return_valuer.fill_get(value)
                        else:
                            yield value
                    break
                has_yield_data = True
                if oyield_value:
                    yield self.get_yield(ovalue, oyield_value)
                elif self.return_valuer:
                    if self.return_is_aggregate:
                        yield self.get_aggregate(ovalue)
                    else:
                        yield self.return_valuer.fill_get(ovalue)
                else:
                    yield ovalue
        else:
            while True:
                try:
                    ovalue = yield_value.send(None)
                    has_yield_data = True
                    if isinstance(ovalue, GeneratorType):
                        yield self.get_yield(None, ovalue)
                    elif self.return_valuer:
                        if self.return_is_aggregate:
                            yield self.get_aggregate(self.do_filter(ovalue))
                        else:
                            yield self.return_valuer.fill_get(self.do_filter(ovalue))
                    else:
                        yield self.do_filter(ovalue)
                except StopIteration:
                    if not has_yield_data:
                        if self.return_valuer:
                            yield self.return_valuer.fill_get(self.do_filter(value))
                        else:
                            yield self.do_filter(value)
                    break

    def get_aggregate(self, value):
        return self.return_valuer.clone(inherited=True).fill_get(value)

    def childs(self):
        childs = []
        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                childs.append(key_valuer)
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, Valuer):
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            childs.extend(self.inherit_valuers)
        return childs

    def get_fields(self):
        fields = []
        if isinstance(self.value_valuer, dict):
            for key_valuer, value_valuer in self.value_valuer.values():
                for field in key_valuer.get_fields():
                    fields.append(field)
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, Valuer):
            for field in self.value_valuer.get_fields():
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

        if isinstance(self.value_valuer, Valuer):
            return self.value_valuer.get_final_filter()

        return None


class ContextMakeValuer(MakeValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextMakeValuer, self).__init__(*args, **kwargs)

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

    def get_aggregate(self, value):
        contexter_values = self.contexter.values
        return_contexter_values = self.contexter.create_inherit_values(contexter_values)
        self.return_valuer.contexter.values = return_contexter_values
        try:
            aggregate_value = self.return_valuer.fill_get(value)
            if isinstance(aggregate_value, types.FunctionType):
                def calculate_value(cdata):
                    self.return_valuer.contexter.values = return_contexter_values
                    try:
                        return aggregate_value(cdata)
                    finally:
                        self.contexter.values = contexter_values
                return calculate_value
            return aggregate_value
        finally:
            self.contexter.values = contexter_values
