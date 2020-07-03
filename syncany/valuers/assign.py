# -*- coding: utf-8 -*-
# 2020/7/3
# create by: snower

from .valuer import Valuer

class AssignValuer(Valuer):
    def __init__(self, global_value, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        super(AssignValuer, self).__init__(*args, **kwargs)

        self.global_value = global_value
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.global_value, calculate_valuer, return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            self.calculate_valuer.fill(self.global_value)
            if not self.calculate_wait_loaded:
                self.value = self.calculate_valuer.get()
                self.global_value[self.key] = self.value
                if self.return_valuer:
                    self.return_valuer.fill(self.value)
        elif self.return_valuer:
            self.value = self.global_value.get(self.key, None)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                self.value = final_filter.filter(self.value)
            self.return_valuer.fill(self.value)
        else:
            self.value = self.global_value.get(self.key, None)
        return self

    def get(self):
        if self.calculate_valuer:
            if self.calculate_wait_loaded:
                self.value = self.calculate_valuer.get()
                self.global_value[self.key] = self.value
                if self.return_valuer:
                    self.return_valuer.fill(self.value)

        if self.return_valuer:
            self.value = self.return_valuer.get()

        if self.filter:
            if isinstance(self.value, (list, tuple, set)):
                values = []
                for v in self.value:
                    values.append(self.filter.filter(v))
                return values
            return self.filter.filter(self.value)
        return self.value

    def childs(self):
        childs = []
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
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.return_valuer:
            return self.calculate_valuer.get_final_filter()

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        return None