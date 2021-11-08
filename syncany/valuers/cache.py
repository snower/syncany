# -*- coding: utf-8 -*-
# 2021/11/7
# create by: snower

from .valuer import Valuer

class CacheValuer(Valuer):
    def __init__(self, cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.cache_loader = cache_loader
        self.key_valuer = key_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CacheValuer, self).__init__(*args, **kwargs)

        self.cache_key = ""

    def init_valuer(self):
        self.key_wait_loaded = self.key_valuer and self.key_valuer.require_loaded()
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        key_valuer = self.key_valuer.clone() if self.key_valuer else None
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.cache_loader, key_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, key_wait_loaded=self.key_wait_loaded,
                              calculate_wait_loaded=self.calculate_wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.key_valuer.fill(data)
        if self.key_wait_loaded:
            self.calculate_valuer.fill(data)
            return self
        self.cache_key = str(self.key_valuer.get())
        self.value = self.cache_loader.get(self.cache_key)
        if self.value is not None:
            return self

        self.calculate_valuer.fill(data)
        if self.calculate_wait_loaded:
            return self
        self.value = self.calculate_valuer.get()
        if self.value is not None:
            self.cache_loader.set(self.cache_key, self.value)

        if self.return_valuer:
            self.do_filter(self.value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                self.value = final_filter.filter(self.value)
            self.return_valuer.fill(self.value)
        else:
            self.do_filter(self.value)
        return self

    def get(self):
        if not self.key_wait_loaded:
            if not self.calculate_wait_loaded:
                if self.return_valuer:
                    self.value = self.return_valuer.get()
                return self.value
        else:
            self.cache_key = str(self.key_valuer.get())
            self.value = self.cache_loader.get(self.cache_key)

        if self.value is None and self.calculate_wait_loaded:
            self.value = self.calculate_valuer.get()
            if self.value is not None:
                self.cache_loader.set(self.cache_key, self.value)

        if self.return_valuer:
            self.do_filter(self.value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                self.value = final_filter.filter(self.value)
            self.return_valuer.fill(self.value)
            self.value = self.return_valuer.get()
        else:
            self.do_filter(self.value)
        return self.value

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