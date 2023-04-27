# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

from .valuer import Valuer


class AggregateData(object):
    def __init__(self, data, state):
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
        return self.datas[key].data

    def set(self, key, name, value):
        if key not in self.datas:
            return None
        self.datas[key].data[name] = value

    def add(self, key, name, data, value):
        if key in self.datas:
            aggregate_value = self.datas[key]
            aggregate_value.state[name] = True
        else:
            aggregate_value = AggregateData(data, {name: True})
            self.datas[key] = aggregate_value
        aggregate_value.data[name] = value

    def reset(self):
        self.datas = {}


class AggregateValuer(Valuer):
    def __init__(self, key_valuer, calculate_valuer, inherit_valuers, aggregate_manager, *args, **kwargs):
        self.key_valuer = key_valuer
        self.calculate_valuer = calculate_valuer
        self.inherit_valuers = inherit_valuers
        self.aggregate_manager = aggregate_manager or AggregateManager()
        super(AggregateValuer, self).__init__(*args, **kwargs)

    def get_manager(self):
        return self.aggregate_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        key_valuer = self.key_valuer.clone(contexter) if self.key_valuer else None
        calculate_valuer = self.calculate_valuer.clone(contexter) if self.calculate_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextAggregateValuer(key_valuer, calculate_valuer, inherit_valuers, self.aggregate_manager, self.key,
                                          self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextAggregateValuer):
            return ContextAggregateValuer(key_valuer, calculate_valuer, inherit_valuers, self.aggregate_manager, self.key,
                                          self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(key_valuer, calculate_valuer, inherit_valuers, self.aggregate_manager, self.key,
                              self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.key_valuer:
            self.key_valuer.fill(data)
        return self

    def get(self):
        key_value = self.key_valuer.get() if self.key_valuer else ""

        def calculate_value(data):
            loader_loaded = self.aggregate_manager.loaded(key_value, self.key)
            if loader_loaded:
                cdata = self.aggregate_manager.get(key_value)
                value = self.do_filter(self.calculate_valuer.fill(cdata).get())
                self.aggregate_manager.set(key_value, self.key, value)
                raise StopIteration

            value = self.do_filter(self.calculate_valuer.fill(data).get())
            self.aggregate_manager.add(key_value, self.key, data, value)
            return value
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


class ContextAggregateValuer(AggregateValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextAggregateValuer, self).__init__(*args, **kwargs)

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

