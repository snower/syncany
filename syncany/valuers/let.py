# -*- coding: utf-8 -*-
# 2020/7/1
# create by: snower

from .valuer import Valuer

class LetValuer(Valuer):
    def __init__(self, key_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        super(LetValuer, self).__init__(*args, **kwargs)

        self.key_valuer = key_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.wait_loaded = True if not self.return_valuer else False
        self.filled_data = None

        if self.return_valuer:
            self.check_wait_loaded()

    def check_wait_loaded(self):
        if self.key_valuer.require_loaded():
            self.wait_loaded = True

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        key_valuer = self.key_valuer.clone() if self.key_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(key_valuer, return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        self.key_valuer.fill(data)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.wait_loaded:
            self.key = self.key_valuer.get()
            super(LetValuer, self).fill(data)
            if self.return_valuer:
                self.return_valuer.fill(super(LetValuer, self).get())
        else:
            self.filled_data = data
        return self

    def get(self):
        if self.wait_loaded:
            self.key = self.key_valuer.get()
            super(LetValuer, self).fill(self.filled_data)
            self.filled_data = None
            if self.return_valuer:
                self.return_valuer.fill(super(LetValuer, self).get())
                return self.return_valuer.get()
        return super(LetValuer, self).get()

    def childs(self):
        if not self.return_valuer:
            return [self.key_valuer]
        return [self.key_valuer, self.return_valuer] + (self.inherit_valuers or [])

    def get_fields(self):
        fields = []
        for field in self.key_valuer.get_fields():
            fields.append(field)

        if not self.wait_loaded and self.return_valuer:
            for field in self.return_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return None