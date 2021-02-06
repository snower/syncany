# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer, LoadAllFieldsException

class DBValuer(Valuer):
    def __init__(self, return_valuer, inherit_valuers, *args, **kwargs):
        super(DBValuer, self).__init__(*args, **kwargs)

        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        super(DBValuer, self).fill(data)

        if self.return_valuer:
            self.return_valuer.fill(self.value)
        return self

    def get(self):
        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

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
        return False