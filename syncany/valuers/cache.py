# -*- coding: utf-8 -*-
# 2021/11/7
# create by: snower

from .valuer import Valuer


class CacheValuer(Valuer):
    cache_key = ""

    def __init__(self, cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.cache_loader = cache_loader
        self.key_valuer = key_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CacheValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(CacheValuer, self).new_init()
        self.key_wait_loaded = self.key_valuer and self.key_valuer.require_loaded()
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()

    def clone_init(self, from_valuer):
        super(CacheValuer, self).clone_init(from_valuer)
        self.key_wait_loaded = from_valuer.key_wait_loaded
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        key_valuer = self.key_valuer.clone(contexter) if self.key_valuer else None
        calculate_valuer = self.calculate_valuer.clone(contexter) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextCacheValuer(self.cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextCacheValuer):
            return ContextCacheValuer(self.cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.cache_key = ""
        return super(CacheValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.key_valuer.fill(data)
        if self.key_wait_loaded:
            self.calculate_valuer.fill(data)
            return self
        self.cache_key = str(self.key_valuer.get())
        value = self.cache_loader.get(self.cache_key)
        if value is not None:
            self.value = value
            return self

        self.calculate_valuer.fill(data)
        if self.calculate_wait_loaded:
            return self
        value = self.calculate_valuer.get()
        if value is not None:
            self.cache_loader.set(self.cache_key, value)

        if self.return_valuer:
            value = self.do_filter(value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            self.return_valuer.fill(value)
        else:
            self.value = self.do_filter(value)
        return self

    def get(self):
        if not self.key_wait_loaded:
            if not self.calculate_wait_loaded:
                if self.return_valuer:
                    return self.return_valuer.get()
                return self.value
            value = self.value
        else:
            self.cache_key = str(self.key_valuer.get())
            value = self.cache_loader.get(self.cache_key)

        if value is None and self.calculate_wait_loaded:
            value = self.calculate_valuer.get()
            if value is not None:
                self.cache_loader.set(self.cache_key, value)

        if self.return_valuer:
            value = self.do_filter(value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            self.return_valuer.fill(value)
            value = self.return_valuer.get()
        else:
            value = self.do_filter(value)
        return value

    def childs(self):
        childs = []
        if self.key_valuer:
            childs.append(self.key_valuer)
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.key_valuer:
            for field in self.key_valuer.get_fields():
                fields.append(field)
        if self.calculate_valuer:
            for field in self.calculate_valuer.get_fields():
                fields.append(field)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.calculate_valuer.get_final_filter()

        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        return None


class ContextCacheValuer(CacheValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.cache_key_context_id = (id(self), "cache_key")
        super(ContextCacheValuer, self).__init__(*args, **kwargs)

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
    def cache_key(self):
        try:
            return self.contexter.values[self.cache_key_context_id]
        except KeyError:
            return ""

    @cache_key.setter
    def cache_key(self, v):
        if v == "":
            if self.cache_key_context_id in self.contexter.values:
                self.contexter.values.pop(self.cache_key_context_id)
            return
        self.contexter.values[self.cache_key_context_id] = v
