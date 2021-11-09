# -*- coding: utf-8 -*-
# 2021/11/7
# create by: snower

from .valuer import Valuer

class StateValuer(Valuer):
    def __init__(self, state_value, calculate_valuer, default_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.state_value = state_value
        self.calculate_valuer = calculate_valuer
        self.default_valuer = default_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(StateValuer, self).__init__(*args, **kwargs)

    def init_valuer(self):
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        default_valuer = self.default_valuer.clone() if self.default_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.state_value, calculate_valuer, default_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, calculate_wait_loaded=self.calculate_wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            self.calculate_valuer.fill(self.state_value)
            if not self.calculate_wait_loaded:
                self.value = self.calculate_valuer.get()
                if not self.value and self.default_valuer:
                    self.default_valuer.fill(self.state_value)
                    self.value = self.default_valuer.get()
                self.do_filter(self.value)
                if self.return_valuer:
                    self.return_valuer.fill(self.value)
        elif self.return_valuer:
            self.do_filter(self.state_value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                self.value = final_filter.filter(self.value)
            self.return_valuer.fill(self.value)
        else:
            self.do_filter(self.state_value)
        return self

    def get(self):
        if self.calculate_valuer:
            if self.calculate_wait_loaded:
                self.value = self.calculate_valuer.get()
                if not self.value and self.default_valuer:
                    self.default_valuer.fill(self.state_value)
                    self.value = self.default_valuer.get()
                self.do_filter(self.value)
                if self.return_valuer:
                    self.return_valuer.fill(self.value)

        if self.return_valuer:
            self.value = self.return_valuer.get()
        return self.value

    def childs(self):
        childs = []
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.default_valuer:
            childs.append(self.default_valuer)
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
        if self.return_valuer:
            return self.calculate_valuer.get_final_filter()

        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        if self.default_valuer:
            return self.default_valuer.get_final_filter()
        return None