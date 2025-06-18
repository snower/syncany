# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import datetime
import weakref
from ..utils import ensure_timezone
from ..filters import ArrayFilter
from .valuer import Valuer, LoadAllFieldsException


class DataValuer(Valuer):
    def __init__(self, return_valuer, inherit_valuers, *args, **kwargs):
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(DataValuer, self).__init__(*args, **kwargs)

        self.data_scoper = None
        self.option = None

    def update_key(self, key):
        super(DataValuer, self).update_key(key)
        if self.optimized:
            self.optimize()

    def new_init(self):
        super(DataValuer, self).new_init()
        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
        else:
            self.parse_key()

    def clone_init(self, from_valuer):
        super(DataValuer, self).clone_init(from_valuer)
        self.data_scoper = from_valuer.data_scoper

    def optimize(self):
        if not self.key:
            self.fill = self.fill_none
            self.fill_get = self.fill_get_none
        elif self.key == "*":
            self.fill = self.fill_star
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = self.do_filter
            else:
                self.fill_get = self.fill_get_star
        elif not self.inherit_valuers and not self.return_valuer and "." not in self.key:
            if self.filter or self.data_scoper is None:
                self.fill = self.fill_dict_key
                self.fill_get = self.fill_get_dict_key
            else:
                from ..loaders import DBLoader, DBJoinLoader
                from .make import MakeValuer
                if isinstance(self.data_scoper, DBLoader):
                    self.fill = self.fast_fill_dict_key
                    self.fill_get = self.fast_fill_get_dict_key
                elif isinstance(self.data_scoper, DBJoinLoader):
                    self.fill = self.fast_fill_none_or_dict_key
                    self.fill_get = self.fast_fill_get_none_or_dict_key
                elif isinstance(self.data_scoper, MakeValuer) and isinstance(self.data_scoper.value_valuer, dict):
                    self.fill = self.fast_fill_dict_key
                    self.fill_get = self.fast_fill_get_dict_key
                else:
                    self.fill = self.fill_dict_key
                    self.fill_get = self.fill_get_dict_key

        if not self.return_valuer:
            self.get = self.fast_get
        self.optimized = True

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_scoper(self, scoper=None, is_return_getter=True, **kwargs):
        if scoper is not None:
            self.data_scoper = weakref.proxy(scoper)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        if self.return_valuer:
            self.return_valuer.mount_scoper(scoper=self, is_return_getter=is_return_getter and True, **kwargs)
        self.optimize()

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            valuer = ContextDataValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=contexter)
        elif isinstance(self, ContextDataValuer):
            valuer = ContextDataValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=self.contexter)
        else:
            valuer = self.__class__(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self)
        valuer.option = self.option
        return valuer

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if isinstance(data, dict) and self.key in data:
            value = self.do_filter(data[self.key])
        elif data is None or not self.key:
            value = self.do_filter(None)
        elif self.key == "*":
            value = self.do_filter(data)
        else:
            if not self.key_getters:
                if self.key in self.KEY_GETTER_CACHES:
                    self.key_getters = self.KEY_GETTER_CACHES[self.key]
                else:
                    self.parse_key()
            try:
                key_getter_index, key_getter_len = 0, len(self.key_getters)
                while key_getter_index < key_getter_len:
                    data, index = self.key_getters[key_getter_index](data)
                    if data is None:
                        break
                    key_getter_index += index
                value = self.do_filter(data)
            except:
                value = self.do_filter(None)

        if self.return_valuer:
            self.return_valuer.fill(value)
        else:
            self.value = value
        return self

    def fill_none(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            self.return_valuer.fill(self.do_filter(None) if self.filter else None)
        else:
            self.value = self.do_filter(None) if self.filter else None
        return self

    def fill_star(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            self.return_valuer.fill(self.do_filter(data))
        else:
            self.value = self.do_filter(data)
        return self

    def fill_dict_key(self, data):
        try:
            if self.filter:
                self.value = self.do_filter(data[self.key])
            else:
                value = data[self.key]
                if isinstance(value, datetime.datetime):
                    self.value = ensure_timezone(value)
                else:
                    self.value = value
        except (TypeError, KeyError):
            if data is None or not self.key:
                value = self.do_filter(None)
            elif self.key == "*":
                value = self.do_filter(data)
            else:
                if not self.key_getters:
                    if self.key in self.KEY_GETTER_CACHES:
                        self.key_getters = self.KEY_GETTER_CACHES[self.key]
                    else:
                        self.parse_key()
                try:
                    key_getter_index, key_getter_len = 0, len(self.key_getters)
                    while key_getter_index < key_getter_len:
                        data, index = self.key_getters[key_getter_index](data)
                        if data is None:
                            break
                        key_getter_index += index
                    value = self.do_filter(data)
                except:
                    value = self.do_filter(None)
            self.value = value
        return self

    def fast_fill_dict_key(self, data):
        value = data.get(self.key)
        if isinstance(value, datetime.datetime):
            self.value = ensure_timezone(value)
        else:
            self.value = value
        return self

    def fast_fill_none_or_dict_key(self, data):
        if data is None:
            return None
        value = data.get(self.key)
        if isinstance(value, datetime.datetime):
            self.value = ensure_timezone(value)
        else:
            self.value = value
        return self

    def get(self):
        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

    def fast_get(self):
        return self.value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if isinstance(data, dict) and self.key in data:
            value = self.do_filter(data[self.key])
        elif data is None or not self.key:
            value = self.do_filter(None)
        elif self.key == "*":
            value = self.do_filter(data)
        else:
            if not self.key_getters:
                if self.key in self.KEY_GETTER_CACHES:
                    self.key_getters = self.KEY_GETTER_CACHES[self.key]
                else:
                    self.parse_key()
            try:
                key_getter_index, key_getter_len = 0, len(self.key_getters)
                while key_getter_index < key_getter_len:
                    data, index = self.key_getters[key_getter_index](data)
                    if data is None:
                        break
                    key_getter_index += index
                value = self.do_filter(data)
            except:
                value = self.do_filter(None)

        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def fill_get_none(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.do_filter(None) if self.filter else None)
        return self.do_filter(None) if self.filter else None

    def fill_get_star(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.do_filter(data))
        return self.do_filter(data)

    def fill_get_dict_key(self, data):
        try:
            if self.filter:
                value = self.do_filter(data[self.key])
            else:
                value = data[self.key]
                if isinstance(value, datetime.datetime):
                    value = ensure_timezone(value)
        except (TypeError, KeyError):
            if data is None or not self.key:
                value = self.do_filter(None)
            elif self.key == "*":
                value = self.do_filter(data)
            else:
                if not self.key_getters:
                    if self.key in self.KEY_GETTER_CACHES:
                        self.key_getters = self.KEY_GETTER_CACHES[self.key]
                    else:
                        self.parse_key()
                try:
                    key_getter_index, key_getter_len = 0, len(self.key_getters)
                    while key_getter_index < key_getter_len:
                        data, index = self.key_getters[key_getter_index](data)
                        if data is None:
                            break
                        key_getter_index += index
                    value = self.do_filter(data)
                except:
                    value = self.do_filter(None)
        return value

    def fast_fill_get_dict_key(self, data):
        value = data.get(self.key)
        if isinstance(value, datetime.datetime):
            return ensure_timezone(value)
        return value

    def fast_fill_get_none_or_dict_key(self, data):
        if data is None:
            return None
        value = data.get(self.key)
        if isinstance(value, datetime.datetime):
            return ensure_timezone(value)
        return value

    def do_filter(self, value):
        if not self.filter:
            if isinstance(value, datetime.datetime):
                value = ensure_timezone(value)
            return value

        if isinstance(value, list):
            if isinstance(self.filter, ArrayFilter):
                return value
            return [self.filter.filter(v) for v in value]
        return self.filter.filter(value)

    def childs(self):
        if self.return_valuer:
            return [self.return_valuer]
        return []

    def get_fields(self):
        if not self.key or self.key == "*":
            if self.return_valuer:
                return self.return_valuer.get_fields()
            raise LoadAllFieldsException()

        keys = [key for key in self.key.split(".") if key and key[0] != ":"]
        if not keys:
            return []
        return keys

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return self.filter

    def require_loaded(self):
        if self.return_valuer:
            return self.return_valuer.require_loaded()
        return False

    def is_const(self):
        return False

    def is_aggregate(self):
        if self.return_valuer:
            return self.return_valuer.is_aggregate()
        return False

    def is_yield(self):
        if self.return_valuer:
            return self.return_valuer.is_aggregate()
        return False


class ContextDataValuer(DataValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
        super(ContextDataValuer, self).__init__(*args, **kwargs)

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
