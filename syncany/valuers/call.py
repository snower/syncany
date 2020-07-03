# -*- coding: utf-8 -*-
# 2020/7/3
# create by: snower

from .valuer import Valuer

class CallReturnManager(object):
    def __init__(self):
        self.datas = {}

    def loaded(self, key):
        if not isinstance(key, (int, float, str, bytes)):
            key = "@id_%s" % id(key)

        if key not in self.datas:
            return key, False
        return key, True

    def get(self, key):
        if key not in self.datas:
            return None
        return self.datas[key]

    def set(self, key, value):
        self.datas[key] = value

class CallValuer(Valuer):
    def __init__(self, calculate_valuer, return_valuer, inherit_valuers, return_manager, *args, **kwargs):
        super(CallValuer, self).__init__(*args, **kwargs)

        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.return_manager = return_manager or CallReturnManager()
        self.calculate_wait_loaded = True if not self.return_valuer or \
                                             (self.calculate_valuer and
                                              self.calculate_valuer.require_loaded()) else False
        self.calculated = False
        self.calculated_key = None

    def get_manager(self):
        return self.return_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(calculate_valuer, return_valuer, inherit_valuers,
                              self.return_manager, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.calculated_key, self.calculated = self.return_manager.loaded(data)
        if not self.calculated:
            self.calculate_valuer.fill(data)
            if self.return_valuer and not self.calculate_wait_loaded:
                self.value = self.calculate_valuer.get()
                self.return_manager.set(self.calculated_key, self.value)
                self.return_valuer.fill(self.value)
        else:
            self.value = self.return_manager.get(self.calculated_key)
            if self.return_valuer:
                self.return_valuer.fill(self.value)

        return self

    def get(self):
        if self.calculated:
            if self.return_valuer:
                return self.return_valuer.get()
            return self.value

        if self.calculate_wait_loaded:
            self.value = self.calculate_valuer.get()
            self.return_manager.set(self.calculated_key, self.value)
            if self.return_valuer:
                self.return_valuer.fill(self.value)
                return self.return_valuer.get()

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
        if self.calculate_valuer:
            for field in self.calculate_valuer.get_fields():
                fields.append(field)

        if self.return_valuer and not self.calculate_wait_loaded:
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

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()

        return None