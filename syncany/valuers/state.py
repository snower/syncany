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

    def new_init(self):
        super(StateValuer, self).new_init()
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()
        self.default_wait_loaded = True if self.default_valuer and self.default_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(StateValuer, self).clone_init(from_valuer)
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded
        self.default_wait_loaded = from_valuer.default_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_scoper(self, scoper=None, is_return_getter=False,**kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        if self.calculate_valuer:
            self.calculate_valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        if self.default_valuer:
            self.default_valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        if self.return_valuer:
            self.return_valuer.mount_scoper(scoper=self, is_return_getter=is_return_getter and True, **kwargs)
        self.optimize()

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        calculate_valuer = self.calculate_valuer.clone(contexter, **kwargs) if self.calculate_valuer else None
        default_valuer = self.default_valuer.clone(contexter, **kwargs) if self.default_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextStateValuer(self.state_value, calculate_valuer, default_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextStateValuer):
            return ContextStateValuer(self.state_value, calculate_valuer, default_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.state_value, calculate_valuer, default_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            self.calculate_valuer.fill(self.state_value)
            if not self.calculate_wait_loaded:
                value = self.calculate_valuer.get()
                if not value and self.default_valuer:
                    value = self.default_valuer.fill_get(self.state_value)
                value = self.do_filter(value)
                if self.return_valuer:
                    self.return_valuer.fill(value)
                else:
                    self.value = value
        elif self.return_valuer:
            value = self.do_filter(self.state_value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            self.return_valuer.fill(value)
        else:
            self.value = self.do_filter(self.state_value)
        return self

    def get(self):
        if self.calculate_valuer:
            if self.calculate_wait_loaded:
                value = self.calculate_valuer.get()
                if not value and self.default_valuer:
                    value = self.default_valuer.fill_get(self.state_value)
                value = self.do_filter(value)
                if self.return_valuer:
                    return self.return_valuer.fill_get(value)
                return value
        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            value = self.calculate_valuer.fill_get(self.state_value)
            if not value and self.default_valuer:
                value = self.default_valuer.fill_get(self.state_value)
            value = self.do_filter(value)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value
        if self.return_valuer:
            value = self.do_filter(self.state_value)
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            return self.return_valuer.fill_get(value)
        return self.do_filter(self.state_value)

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

    def is_const(self):
        return False


class ContextStateValuer(StateValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
        super(ContextStateValuer, self).__init__(*args, **kwargs)

    def optimize(self):
        if not self.calculate_wait_loaded and not self.default_wait_loaded and not self.wait_loaded:
            self.fill = self.defer_fill
            self.get = self.defer_get
            self.optimized = True

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

    def defer_fill(self, data):
        if data is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return self
        self.contexter.values[self.value_context_id] = data
        return self

    def defer_get(self):
        try:
            data = self.contexter.values[self.value_context_id]
        except KeyError:
            data = None
        return self.fill_get(data)
