# -*- coding: utf-8 -*-
# 2020/7/3
# create by: snower

from .valuer import Valuer


class AssignValuer(Valuer):
    def __init__(self, global_value, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.global_value = global_value
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(AssignValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(AssignValuer, self).new_init()
        self.calculate_wait_loaded = self.calculate_valuer and self.calculate_valuer.require_loaded()

    def clone_init(self, from_valuer):
        super(AssignValuer, self).clone_init(from_valuer)
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        calculate_valuer = self.calculate_valuer.clone(contexter) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextAssignValuer(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextAssignValuer):
            return ContextAssignValuer(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            self.calculate_valuer.fill(self.global_value)
            if not self.calculate_wait_loaded:
                value = self.do_filter(self.calculate_valuer.get())
                self.global_value[self.key] = value
                if self.return_valuer:
                    self.return_valuer.fill(value)
                else:
                    self.value = value
        elif self.return_valuer:
            value = self.do_filter(self.global_value.get(self.key, None))
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            self.return_valuer.fill(value)
        else:
            self.value = self.do_filter(self.global_value.get(self.key, None))
        return self

    def get(self):
        if self.calculate_valuer:
            if self.calculate_wait_loaded:
                value = self.do_filter(self.calculate_valuer.get())
                self.global_value[self.key] = value
                if self.return_valuer:
                    return self.return_valuer.fill(value).get()
                return value
        if self.return_valuer:
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
        return None


class ContextAssignValuer(AssignValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextAssignValuer, self).__init__(*args, **kwargs)

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
