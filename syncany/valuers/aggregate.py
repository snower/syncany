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

    def get_aggregate_data(self, key):
        self.get_aggregate_data = self.datas.get
        return self.datas.get(key)

    def add_aggregate_data(self, key, name, data, value):
        aggregate_value = AggregateData(data, {name: True})
        self.datas[key] = aggregate_value
        aggregate_value.data[name] = value

    def reset(self):
        self.datas.clear()


class AggregateValuer(Valuer):
    def __init__(self, key_valuer, calculate_valuer, inherit_valuers, aggregate_manager, *args, **kwargs):
        self.key_valuer = key_valuer
        self.calculate_valuer = calculate_valuer
        self.inherit_valuers = inherit_valuers
        self.aggregate_manager = aggregate_manager or AggregateManager()
        super(AggregateValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(AggregateValuer, self).new_init()
        self.key_wait_loaded = True if self.key_valuer and self.key_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(AggregateValuer, self).clone_init(from_valuer)
        self.key_wait_loaded = from_valuer.key_wait_loaded

    def get_manager(self):
        return self.aggregate_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=False, aggreagte_valuers=None, **kwargs):
        if aggreagte_valuers is None:
            aggreagte_valuers = []
        aggreagte_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, aggreagte_valuers=aggreagte_valuers, **kwargs)
        if self.key_valuer:
            self.key_valuer.mount_loader(is_return_getter=False, aggreagte_valuers=aggreagte_valuers, **kwargs)
        if self.calculate_valuer:
            self.calculate_valuer.mount_loader(is_return_getter=False, aggreagte_valuers=aggreagte_valuers, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        key_valuer = self.key_valuer.clone(contexter, **kwargs) if self.key_valuer else None
        calculate_valuer = self.calculate_valuer.clone(contexter, **kwargs) if self.calculate_valuer else None
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

        if not self.key_wait_loaded:
            if self.key_valuer:
                self.value = self.key_valuer.fill_get(data)
            return self
        if self.key_valuer:
            self.key_valuer.fill(data)
        return self

    def get(self):
        if not self.key_wait_loaded:
            key_value = self.value if self.key_valuer else ""
        else:
            key_value = self.key_valuer.get() if self.key_valuer else ""

        def calculate_value(cdata):
            aggregate_data = self.aggregate_manager.get_aggregate_data(key_value)
            if aggregate_data is None:
                if self.filter:
                    value = self.do_filter(self.calculate_valuer.fill_get(cdata))
                else:
                    value = self.calculate_valuer.fill_get(cdata)
                self.aggregate_manager.add_aggregate_data(key_value, self.key, cdata, value)
                return value
            if self.key not in aggregate_data.state:
                if self.filter:
                    value = self.do_filter(self.calculate_valuer.fill_get(cdata))
                else:
                    value = self.calculate_valuer.fill_get(cdata)
                aggregate_data.state[self.key] = True
                aggregate_data.data[self.key] = value
                return value
            if self.filter:
                aggregate_data.data[self.key] = self.do_filter(self.calculate_valuer.fill_get(aggregate_data.data))
            else:
                aggregate_data.data[self.key] = self.calculate_valuer.fill_get(aggregate_data.data)
            raise StopIteration
        return calculate_value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)
        key_value = self.key_valuer.fill_get(data) if self.key_valuer else ""

        def calculate_value(cdata):
            aggregate_data = self.aggregate_manager.get_aggregate_data(key_value)
            if aggregate_data is None:
                if self.filter:
                    value = self.do_filter(self.calculate_valuer.fill_get(cdata))
                else:
                    value = self.calculate_valuer.fill_get(cdata)
                self.aggregate_manager.add_aggregate_data(key_value, self.key, cdata, value)
                return value
            if self.key not in aggregate_data.state:
                if self.filter:
                    value = self.do_filter(self.calculate_valuer.fill_get(cdata))
                else:
                    value = self.calculate_valuer.fill_get(cdata)
                aggregate_data.state[self.key] = True
                aggregate_data.data[self.key] = value
                return value
            if self.filter:
                aggregate_data.data[self.key] = self.do_filter(self.calculate_valuer.fill_get(aggregate_data.data))
            else:
                aggregate_data.data[self.key] = self.calculate_valuer.fill_get(aggregate_data.data)
            raise StopIteration
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

    def is_aggregate(self):
        return True

    def is_yield(self):
        return False


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

