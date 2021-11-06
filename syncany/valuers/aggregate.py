# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

from .valuer import Valuer

class AggregateManager(object):
    def __init__(self):
        self.datas = {}

    def loaded(self, key, name):
        if key not in self.datas:
            return False

        if name not in self.datas[key][2]:
            return False
        return True

    def get(self, key):
        if key not in self.datas:
            return None
        return self.datas[key][0]

    def set(self, key, name, value):
        if key not in self.datas:
            return None

        self.datas[key][0][name] = value
        if name in self.datas[key][1]:
            self.datas[key][1][name] = value

    def add(self, key, name, data):
        if key in self.datas:
            self.datas[key][2][name] = True
            return self.datas[key][0]
        scope_data = {k: v for k, v in data.items()}
        self.datas[key] = (scope_data, data, {name: True})
        return scope_data

class AggregateValuer(Valuer):
    def __init__(self, key_valuer, calculate_valuer, inherit_valuers, aggregate_manager, *args, **kwargs):
        self.key_valuer = key_valuer
        self.calculate_valuer = calculate_valuer
        self.inherit_valuers = inherit_valuers
        self.aggregate_manager = aggregate_manager or AggregateManager()
        super(AggregateValuer, self).__init__(*args, **kwargs)

        self.key_value = None
        self.loader_loaded = False

    def get_manager(self):
        return self.aggregate_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        key_valuer = self.key_valuer.clone() if self.key_valuer else None
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(key_valuer, calculate_valuer, inherit_valuers, self.aggregate_manager, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.key_valuer:
            self.key_valuer.fill(data)
        return self

    def get(self):
        self.key_value = self.key_valuer.get() if self.key_valuer else ""
        self.loader_loaded = self.aggregate_manager.loaded(self.key_value, self.key)
        if self.loader_loaded:
            loader_data = self.aggregate_manager.get(self.key_value)

            self.calculate_valuer.fill(loader_data)
            self.do_filter(self.calculate_valuer.get())
            self.aggregate_manager.set(self.key_value, self.key, self.value)

        def gen_iter():
            loader_data = yield None
            if not self.loader_loaded:
                final_filter = self.calculate_valuer.get_final_filter() if not self.filter else self.filter
                if final_filter:
                    loader_data[self.key] = final_filter(None)
                loader_data = self.aggregate_manager.add(self.key_value, self.key, loader_data)

                self.calculate_valuer.fill(loader_data)
                self.do_filter(self.calculate_valuer.get())
                self.aggregate_manager.set(self.key_value, self.key, self.value)
                yield self.value

        g = gen_iter()
        g.send(None)
        return g

    def childs(self):
        childs = []
        if self.key_valuer:
            childs.append(self.key_valuer)
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.key_valuer:
            for field in self.key_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        return None