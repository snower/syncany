# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

from .valuer import Valuer

def raise_stop_iteration(data):
    raise StopIteration

class AggregateValue(object):
    def __init__(self, data, state):
        self.scope_data = None
        self.data = data
        self.state = state

class AggregateManager(object):
    def __init__(self):
        self.datas = {}

    def loaded(self, key, name):
        if key not in self.datas:
            return False

        if name not in self.datas[key].state:
            return False
        return True

    def get(self, key):
        if key not in self.datas:
            return None
        aggregate_value = self.datas[key]
        return aggregate_value.scope_data or aggregate_value.data

    def set(self, key, name, value):
        if key not in self.datas:
            return None

        aggregate_value = self.datas[key]
        if name in aggregate_value.data:
            aggregate_value.data[name] = value
            if aggregate_value.scope_data:
                aggregate_value.scope_data[name] = value
        else:
            if not aggregate_value.scope_data:
                aggregate_value.scope_data = {key: value for key, value in aggregate_value.data.items()}
            aggregate_value.scope_data[name] = value

    def add(self, key, name, data, value):
        if key in self.datas:
            aggregate_value = self.datas[key]
            aggregate_value.state[name] = True
        else:
            aggregate_value = AggregateValue(data, {name: True})
            self.datas[key] = aggregate_value

        if name in aggregate_value.data:
            aggregate_value.data[name] = value
        else:
            aggregate_value.scope_data = {key: value for key, value in aggregate_value.data.items()}
            aggregate_value.scope_data[name] = value

    def reset(self):
        self.datas = {}

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
            cdata = self.aggregate_manager.get(self.key_value)
            self.calculate_valuer.fill(cdata)
            self.do_filter(self.calculate_valuer.get())
            self.aggregate_manager.set(self.key_value, self.key, self.value)
            return raise_stop_iteration

        def calculate_value(data):
            self.calculate_valuer.fill(data)
            self.do_filter(self.calculate_valuer.get())
            self.aggregate_manager.add(self.key_value, self.key, data, self.value)
            return self.value
        return calculate_value

    def reset(self):
        self.aggregate_manager.reset()
        super(AggregateValuer, self).reset()

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